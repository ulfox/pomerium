package storage

import (
	"context"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/pomerium/pomerium/pkg/cryptutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/protoutil"
)

type encryptedRecordStream struct {
	underlying RecordStream
	backend    *encryptedBackend
	err        error
}

func (e *encryptedRecordStream) Close() error {
	return e.underlying.Close()
}

func (e *encryptedRecordStream) Next(wait bool) bool {
	return e.underlying.Next(wait)
}

func (e *encryptedRecordStream) Record() *databroker.Record {
	r := e.underlying.Record()
	if r != nil {
		var err error
		r, err = e.backend.decryptRecord(r)
		if err != nil {
			e.err = err
		}
	}
	return r
}

func (e *encryptedRecordStream) Err() error {
	if e.err == nil {
		e.err = e.underlying.Err()
	}
	return e.err
}

type encryptedBackend struct {
	underlying Backend
	cipher     cipher.AEAD
}

// NewEncryptedBackend creates a new encrypted backend.
func NewEncryptedBackend(secret []byte, underlying Backend) (Backend, error) {
	c, err := cryptutil.NewAEADCipher(secret)
	if err != nil {
		return nil, err
	}

	return &encryptedBackend{
		underlying: underlying,
		cipher:     c,
	}, nil
}

func (e *encryptedBackend) Close() error {
	return e.underlying.Close()
}

func (e *encryptedBackend) Get(ctx context.Context, recordType, id string) (*databroker.Record, error) {
	record, err := e.underlying.Get(ctx, recordType, id)
	if err != nil {
		return nil, err
	}
	record, err = e.decryptRecord(record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (e *encryptedBackend) GetOptions(ctx context.Context, recordType string) (*databroker.Options, error) {
	return e.underlying.GetOptions(ctx, recordType)
}

func (e *encryptedBackend) Lease(ctx context.Context, leaseName, leaseID string, ttl time.Duration) (bool, error) {
	return e.underlying.Lease(ctx, leaseName, leaseID, ttl)
}

func (e *encryptedBackend) Put(ctx context.Context, records []*databroker.Record) (uint64, error) {
	encryptedRecords := make([]*databroker.Record, len(records))
	for i, record := range records {
		encrypted, err := e.encrypt(record.GetData())
		if err != nil {
			return 0, err
		}

		newRecord := proto.Clone(record).(*databroker.Record)
		newRecord.Data = encrypted
		encryptedRecords[i] = newRecord
	}

	serverVersion, err := e.underlying.Put(ctx, encryptedRecords)
	if err != nil {
		return 0, err
	}

	for i, record := range records {
		record.ModifiedAt = encryptedRecords[i].ModifiedAt
		record.Version = encryptedRecords[i].Version
	}

	return serverVersion, nil
}

func (e *encryptedBackend) SetOptions(ctx context.Context, recordType string, options *databroker.Options) error {
	return e.underlying.SetOptions(ctx, recordType, options)
}

func (e *encryptedBackend) Sync(ctx context.Context, serverVersion, recordVersion uint64) (RecordStream, error) {
	stream, err := e.underlying.Sync(ctx, serverVersion, recordVersion)
	if err != nil {
		return nil, err
	}
	return &encryptedRecordStream{
		underlying: stream,
		backend:    e,
	}, nil
}

func (e *encryptedBackend) SyncLatest(ctx context.Context, recordType string, filter FilterExpression) (serverVersion, recordVersion uint64, stream RecordStream, err error) {
	serverVersion, recordVersion, stream, err = e.underlying.SyncLatest(ctx, recordType, filter)
	if err != nil {
		return serverVersion, recordVersion, nil, err
	}
	return serverVersion, recordVersion, &encryptedRecordStream{
		underlying: stream,
		backend:    e,
	}, nil
}

func (e *encryptedBackend) decryptRecord(in *databroker.Record) (out *databroker.Record, err error) {
	data, err := e.decrypt(in.Data)
	if err != nil {
		return nil, err
	}
	// Create a new record so that we don't re-use any internal state
	return &databroker.Record{
		Version:    in.Version,
		Type:       in.Type,
		Id:         in.Id,
		Data:       data,
		ModifiedAt: in.ModifiedAt,
		DeletedAt:  in.DeletedAt,
	}, nil
}

func (e *encryptedBackend) decrypt(in *anypb.Any) (out *anypb.Any, err error) {
	if in == nil {
		return nil, nil
	}

	msg, err := in.UnmarshalNew()
	if err != nil {
		return nil, err
	}

	var encrypted []byte
	switch msg := msg.(type) {
	case *wrapperspb.BytesValue:
		encrypted = msg.Value
	case *structpb.Struct:
		v, ok := msg.GetFields()["encrypted"]
		if !ok {
			return nil, fmt.Errorf("missing encrypted field in struct")
		}
		encrypted, err = base64.StdEncoding.DecodeString(v.GetStringValue())
		if err != nil {
			return nil, fmt.Errorf("unexpected encrypted payload in struct: %w", err)
		}
	default:
		return nil, fmt.Errorf("unexpected protobuf type in encrypted payload: %T", encrypted)
	}

	plaintext, err := cryptutil.Decrypt(e.cipher, encrypted, nil)
	if err != nil {
		return nil, err
	}

	out = new(anypb.Any)
	err = proto.Unmarshal(plaintext, out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (e *encryptedBackend) encrypt(in *anypb.Any) (out *anypb.Any, err error) {
	plaintext, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	encrypted := cryptutil.Encrypt(e.cipher, plaintext, nil)

	// anything in the "$index" field will be preserved
	if index := GetRecordIndex(in); index != nil {
		return protoutil.NewAny(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"encrypted": structpb.NewStringValue(base64.StdEncoding.EncodeToString(encrypted)),
				"index":     structpb.NewStructValue(index),
			},
		}), nil
	}

	return protoutil.NewAny(&wrapperspb.BytesValue{
		Value: encrypted,
	}), nil
}
