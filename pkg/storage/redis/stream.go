package redis

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/pomerium/internal/log"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/storage"
)

func newSyncRecordStream(
	ctx context.Context,
	backend *Backend,
	serverVersion uint64,
	recordVersion uint64,
) storage.RecordStream {
	return newChangedRecordStream(ctx, backend, recordVersion)
}

func newSyncLatestRecordStream(
	ctx context.Context,
	backend *Backend,
	recordType string,
	expr storage.FilterExpression,
) (storage.RecordStream, error) {
	recordVersion, err := backend.client.Get(ctx, lastVersionKey).Uint64()
	if errors.Is(err, redis.Nil) {
		// this happens if there are no records
	} else if err != nil {
		return nil, err
	}
	filter, err := storage.RecordStreamFilterFromFilterExpression(expr)
	if err != nil {
		return nil, err
	}
	if recordType != "" {
		filter = filter.And(func(record *databroker.Record) (keep bool) {
			return record.GetType() == recordType
		})
	}

	// stream1 are all the records for the given type
	stream1, err := newFilteredRecordStream(ctx, backend, recordType, expr, filter)
	if err != nil {
		return nil, err
	}

	// stream2 are any records which changed since we started streaming
	stream2 := newChangedRecordStream(ctx, backend, recordVersion)
	stream2 = storage.NewFilteredRecordStream(stream2, filter)

	// stream is the concatenation of the two streams
	stream := storage.NewConcatenatedRecordStream(stream1, stream2)
	return stream, nil
}

func newFilteredRecordStream(
	ctx context.Context,
	backend *Backend,
	recordType string,
	expr storage.FilterExpression,
	filter storage.RecordStreamFilter,
) (storage.RecordStream, error) {
	if and, ok := expr.(storage.AndFilterExpression); ok && len(and) > 0 {
		return newFilteredRecordStream(ctx, backend, recordType, and[0], filter)
	}

	if or, ok := expr.(storage.OrFilterExpression); ok && len(or) > 0 {
		var streams []storage.RecordStream
		for _, subExpr := range or {
			stream, err := newFilteredRecordStream(ctx, backend, recordType, subExpr, filter)
			if err != nil {
				return nil, err
			}
			streams = append(streams, stream)
		}
		return storage.NewDedupedRecordStream(storage.NewConcatenatedRecordStream(streams...)), nil
	}

	if equals, ok := expr.(storage.EqualsFilterExpression); ok {
		switch strings.Join(equals.Fields, ".") {
		case "id":
			if recordType != "" {
				recordID := equals.Value
				return storage.NewSingleRecordStream(ctx, func(ctx context.Context) (*databroker.Record, error) {
					record, err := getRecord(ctx, backend.client, recordType, recordID)
					return record, err
				}), nil
			}
		case "$index":
			if recordType != "" {
				indexValue := equals.Value
				return storage.NewSingleRecordStream(ctx, func(ctx context.Context) (*databroker.Record, error) {
					if addr, err := netip.ParseAddr(indexValue); err == nil {
						record, err := findRecordByIndexCIDR(ctx, backend.client, recordType, addr)
						return record, err
					}
					return nil, storage.ErrNotFound
				}), nil
			}
		default:
			return nil, fmt.Errorf("only id or $index is supported for filters")
		}
	}

	// finally return all records
	match := ""
	if recordType != "" {
		match = recordType + "/*"
	}
	stream := newRecordStream(ctx, backend, match)
	stream = storage.NewFilteredRecordStream(stream, filter)
	return stream, nil
}

type recordStream struct {
	backend *Backend
	match   string

	ctx         context.Context
	cancel      context.CancelFunc
	scannedOnce bool
	cursor      uint64
	pending     []*databroker.Record
	err         error
}

func newRecordStream(ctx context.Context, backend *Backend, match string) storage.RecordStream {
	stream := &recordStream{
		backend: backend,
		match:   match,
	}
	stream.ctx, stream.cancel = context.WithCancel(ctx)
	return stream
}

func (stream *recordStream) Close() error {
	stream.cancel()
	return nil
}

func (stream *recordStream) Next(block bool) bool {
	for {
		if stream.err != nil {
			return false
		}

		if len(stream.pending) > 1 {
			stream.pending = stream.pending[1:]
			return true
		}

		if stream.scannedOnce && stream.cursor == 0 {
			return false
		}

		var values []string
		values, stream.cursor, stream.err = stream.backend.client.HScan(
			stream.ctx,
			recordHashKey,
			stream.cursor,
			stream.match,
			0,
		).Result()
		stream.scannedOnce = true
		if errors.Is(stream.err, redis.Nil) {
			stream.err = nil
		} else if stream.err != nil {
			return false
		}

		for i := 1; i < len(values); i += 2 {
			var record databroker.Record
			err := proto.Unmarshal([]byte(values[i]), &record)
			if err != nil {
				log.Warn(stream.ctx).Err(err).Msg("redis: invalid record detected")
				continue
			}
			stream.pending = append(stream.pending, &record)
		}

		if len(stream.pending) > 0 {
			return true
		}
	}
}

func (stream *recordStream) Record() *databroker.Record {
	if len(stream.pending) == 0 {
		return nil
	}
	return stream.pending[0]
}

func (stream *recordStream) Err() error {
	return stream.err
}

type changedRecordStream struct {
	backend       *Backend
	recordVersion uint64

	ctx     context.Context
	cancel  context.CancelFunc
	record  *databroker.Record
	err     error
	ticker  *time.Ticker
	changed chan context.Context
}

func newChangedRecordStream(ctx context.Context, backend *Backend, recordVersion uint64) storage.RecordStream {
	stream := &changedRecordStream{
		backend:       backend,
		recordVersion: recordVersion,
		ticker:        time.NewTicker(watchPollInterval),
		changed:       backend.onChange.Bind(),
	}
	stream.ctx, stream.cancel = context.WithCancel(ctx)
	return stream
}

func (stream *changedRecordStream) Close() error {
	stream.cancel()
	stream.ticker.Stop()
	stream.backend.onChange.Unbind(stream.changed)
	return nil
}

func (stream *changedRecordStream) Next(block bool) bool {
	for {
		if stream.err != nil {
			return false
		}

		stream.record, stream.err = getNextChangedRecord(
			stream.ctx,
			stream.backend.client,
			stream.recordVersion,
		)
		if errors.Is(stream.err, storage.ErrNotFound) {
			stream.err = nil
		} else if stream.err != nil {
			return false
		}

		if stream.record != nil {
			stream.recordVersion = stream.record.GetVersion()
			return true
		}

		if !block {
			return false
		}

		select {
		case <-stream.ctx.Done():
			stream.err = stream.ctx.Err()
			return false
		case <-stream.ticker.C:
		case <-stream.changed:
		}
	}
}

func (stream *changedRecordStream) Record() *databroker.Record {
	return stream.record
}

func (stream *changedRecordStream) Err() error {
	return stream.err
}
