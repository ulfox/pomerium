package databroker

import (
	"context"
	"sync"

	"github.com/google/uuid"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/pomerium/pkg/cryptutil"
	"github.com/pomerium/pomerium/pkg/protoutil"
)

// A Querier is a read-only subset of the client methods
type Querier interface {
	Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error)
}

// nilQuerier always returns NotFound.
type nilQuerier struct{}

func (nilQuerier) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error) {
	return nil, status.Error(codes.NotFound, "not implemented")
}

var querierKey struct{}

// GetQuerier gets the databroker Querier from the context.
func GetQuerier(ctx context.Context) Querier {
	q, ok := ctx.Value(querierKey).(Querier)
	if !ok {
		q = nilQuerier{}
	}
	return q
}

// WithQuerier sets the databroker Querier on a context.
func WithQuerier(ctx context.Context, querier Querier) context.Context {
	return context.WithValue(ctx, querierKey, querier)
}

// A StaticQuerier implements the Querier interface by returning statically defined protobuf messages.
type StaticQuerier struct {
	records []*Record
}

// NewStaticQuerier creates a new StaticQuerier.
func NewStaticQuerier(msgs ...proto.Message) *StaticQuerier {
	getter := &StaticQuerier{}
	for _, msg := range msgs {
		any := protoutil.NewAny(msg)
		record := new(Record)
		record.ModifiedAt = timestamppb.Now()
		record.Version = cryptutil.NewRandomUInt64()
		record.Id = uuid.New().String()
		record.Data = any
		record.Type = any.TypeUrl
		if hasID, ok := msg.(interface{ GetId() string }); ok {
			record.Id = hasID.GetId()
		}
		getter.records = append(getter.records, record)
	}
	return getter
}

// Query queries for records.
func (q *StaticQuerier) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error) {
	return nil, status.Error(codes.NotFound, "not implemented")
}

// A ClientQuerier implements the Querier interface by making calls to the databroker over gRPC.
type ClientQuerier struct {
	client DataBrokerServiceClient
}

// NewQuerier creates a new Querier from a client.
func NewQuerier(client DataBrokerServiceClient) Querier {
	return &ClientQuerier{client: client}
}

// Query queries for records.
func (q *ClientQuerier) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error) {
	return q.client.Query(ctx, in, opts...)
}

// A TracingQuerier records calls to Query.
type TracingQuerier struct {
	underlying Querier

	mu     sync.Mutex
	traces []QueryTrace
}

// A QueryTrace traces a call to Query.
type QueryTrace struct {
	ServerVersion, RecordVersion uint64

	RecordType    string
	Query, Filter string
}

// NewTracingQuerier creates a new TracingQuerier.
func NewTracingQuerier(q Querier) *TracingQuerier {
	return &TracingQuerier{
		underlying: q,
	}
}

// Query queries for records.
func (q *TracingQuerier) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error) {
	res, err := q.underlying.Query(ctx, in, opts...)
	if err == nil {
		q.mu.Lock()
		q.traces = append(q.traces, QueryTrace{
			RecordType: in.GetType(),
			Query:      in.GetQuery(),
			Filter:     in.GetFilter(),
		})
		q.mu.Unlock()
	}
	return res, err
}

// Records returns all the traces.
func (q *TracingQuerier) Traces() []QueryTrace {
	q.mu.Lock()
	traces := make([]QueryTrace, len(q.traces))
	copy(traces, q.traces)
	q.mu.Unlock()
	return traces
}
