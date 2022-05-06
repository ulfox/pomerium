package redis

import (
	"context"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/pomerium/pomerium/internal/redisutil"
	"github.com/pomerium/pomerium/internal/testutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/protoutil"
	"github.com/pomerium/pomerium/pkg/storage"
)

func TestIndexCIDR(t *testing.T) {
	if os.Getenv("GITHUB_ACTION") != "" && runtime.GOOS == "darwin" {
		t.Skip("Github action can not run docker on MacOS")
	}

	type M = map[string]interface{}

	ctx, clearTimeout := context.WithTimeout(context.Background(), time.Second*30)
	defer clearTimeout()

	require.NoError(t, testutil.WithTestRedis(false, func(rawURL string) error {
		client, err := redisutil.NewClientFromURL(rawURL, nil)
		if err != nil {
			return err
		}
		defer client.Close()

		s1, _ := structpb.NewStruct(M{
			"$index": M{
				"cidr": "10.0.0.0/8",
			},
		})
		r1 := &databroker.Record{
			Type: "example",
			Id:   "r1",
			Data: protoutil.NewAny(s1),
		}
		s2, _ := structpb.NewStruct(M{
			"$index": M{
				"cidr": "172.16.0.0/12",
			},
		})
		r2 := &databroker.Record{
			Type: "example",
			Id:   "r2",
			Data: protoutil.NewAny(s2),
		}
		s3, _ := structpb.NewStruct(M{
			"$index": M{
				"cidr": "192.168.0.0/16",
			},
		})
		r3 := &databroker.Record{
			Type: "example",
			Id:   "r3",
			Data: protoutil.NewAny(s3),
		}

		for _, r := range []*databroker.Record{r1, r2, r3} {
			_, err = setRecord(ctx, client, r).Result()
			assert.NoError(t, err)
			_, err = setRecordIndexCIDR(ctx, client, "example", r.GetId(), *storage.GetRecordIndexCIDR(r.GetData())).Result()
			assert.NoError(t, err)
		}

		var record *databroker.Record

		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("9.255.255.255"))
		_ = assert.Error(t, err) && assert.Nil(t, record)
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("10.0.0.0"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r1", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("10.255.255.255"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r1", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("11.0.0.0"))
		_ = assert.Error(t, err) && assert.Nil(t, record)

		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("172.15.255.255"))
		_ = assert.Error(t, err) && assert.Nil(t, record)
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("172.16.0.0"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r2", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("172.31.255.255"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r2", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("172.32.0.0"))
		_ = assert.Error(t, err) && assert.Nil(t, record)

		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("192.167.255.255"))
		_ = assert.Error(t, err) && assert.Nil(t, record)
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("192.168.0.0"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r3", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("192.168.255.255"))
		_ = assert.NoError(t, err) && assert.Equal(t, "r3", record.GetId())
		record, err = findRecordByIndexCIDR(ctx, client, "example", netip.MustParseAddr("192.169.0.0"))
		_ = assert.Error(t, err) && assert.Nil(t, record)

		return nil
	}))
}
