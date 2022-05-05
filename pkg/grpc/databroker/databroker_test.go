package databroker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/pomerium/pomerium/pkg/protoutil"
)

func TestApplyOffsetAndLimit(t *testing.T) {
	cases := []struct {
		name          string
		records       []*Record
		offset, limit int
		expect        []*Record
	}{
		{
			name:    "empty",
			records: nil,
			offset:  10,
			limit:   5,
			expect:  nil,
		},
		{
			name:    "less than limit",
			records: []*Record{{Id: "A"}, {Id: "B"}, {Id: "C"}, {Id: "D"}},
			offset:  1,
			limit:   10,
			expect:  []*Record{{Id: "B"}, {Id: "C"}, {Id: "D"}},
		},
		{
			name:    "more than limit",
			records: []*Record{{Id: "A"}, {Id: "B"}, {Id: "C"}, {Id: "D"}, {Id: "E"}, {Id: "F"}, {Id: "G"}, {Id: "H"}},
			offset:  3,
			limit:   2,
			expect:  []*Record{{Id: "D"}, {Id: "E"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, cnt := ApplyOffsetAndLimit(tc.records, tc.offset, tc.limit)
			assert.Equal(t, len(tc.records), cnt)
			assert.Equal(t, tc.expect, actual)
		})
	}
}

func TestOptimumPutRequestsFromRecords(t *testing.T) {
	var records []*Record
	for i := 0; i < 10_000; i++ {
		s := structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"long_string": structpb.NewStringValue(strings.Repeat("x", 987)),
			},
		})
		records = append(records, &Record{
			Id:   fmt.Sprintf("%d", i),
			Data: protoutil.NewAny(s),
		})
	}
	requests := OptimumPutRequestsFromRecords(records)
	for _, request := range requests {
		assert.LessOrEqual(t, proto.Size(request), maxMessageSize)
		assert.GreaterOrEqual(t, proto.Size(request), maxMessageSize/2)
	}
}

type mockServer struct {
	DataBrokerServiceServer

	syncLatest func(empty *SyncLatestRequest, server DataBrokerService_SyncLatestServer) error
}

func (m *mockServer) SyncLatest(req *SyncLatestRequest, stream DataBrokerService_SyncLatestServer) error {
	return m.syncLatest(req, stream)
}
