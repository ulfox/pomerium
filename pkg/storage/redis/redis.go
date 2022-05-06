package redis

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/pomerium/internal/netutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/storage"
)

func addRecordChange(
	ctx context.Context,
	cmdable redis.Cmdable,
	record *databroker.Record,
) {
	bs, err := proto.Marshal(record)
	if err != nil {
		return
	}

	cmdable.ZAdd(ctx, changesSetKey, &redis.Z{
		Score:  float64(record.GetVersion()),
		Member: bs,
	})
	cmdable.ZAdd(ctx, getRecordTypeChangesKey(record.GetType()), &redis.Z{
		Score:  float64(record.GetModifiedAt().GetSeconds()) + (1 - 1/float64(record.GetVersion()+1)),
		Member: record.GetId(),
	})
}

func deleteRecord(
	ctx context.Context,
	cmdable redis.Cmdable,
	recordType, recordID string,
) {
	cmdable.HDel(ctx, recordHashKey, fmt.Sprintf("%s/%s", recordType, recordID))
}

func deleteRecordIndexCIDR(
	ctx context.Context,
	cmdable redis.Cmdable,
	recordType, recordID string,
) {
	cmdable.ZRem(ctx, fmt.Sprintf("%s/%s", indexCIDRKey, recordType), recordID)
}

func findRecordByIndexCIDR(
	ctx context.Context,
	cmdable redis.Cmdable,
	recordType string,
	recordAddr netip.Addr,
) (*databroker.Record, error) {
	score := netutil.AddrToFloat(recordAddr)

	keys, err := cmdable.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     fmt.Sprintf("%s/%s", indexCIDRKey, recordType),
		Start:   "-inf",
		Stop:    score,
		ByScore: true,
		Rev:     true,
		Count:   1,
	}).Result()
	if errors.Is(err, redis.Nil) || len(keys) == 0 {
		return nil, storage.ErrNotFound
	}

	record, err := getRecord(ctx, cmdable, recordType, keys[0])
	if err != nil {
		return nil, err
	}

	cidr := storage.GetRecordIndexCIDR(record.GetData())
	if cidr == nil {
		return nil, err
	}
	if !cidr.Contains(recordAddr) {
		return nil, storage.ErrNotFound
	}

	return record, nil
}

func getNextChangedRecord(
	ctx context.Context,
	cmdable redis.Cmdable,
	afterRecordVersion uint64,
) (*databroker.Record, error) {
	results, err := cmdable.ZRangeByScore(ctx, changesSetKey, &redis.ZRangeBy{
		Min:    fmt.Sprintf("(%d", afterRecordVersion),
		Max:    "+inf",
		Offset: 0,
		Count:  1,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, storage.ErrNotFound
	} else if err != nil {
		return nil, err
	} else if len(results) == 0 {
		return nil, storage.ErrNotFound
	}

	var record databroker.Record
	err = proto.Unmarshal([]byte(results[0]), &record)
	if err != nil {
		_, _ = cmdable.ZRem(ctx, changesSetKey, results[0]).Result()  // delete the bad record
		return getNextChangedRecord(ctx, cmdable, afterRecordVersion) // try again
	}

	return &record, nil
}

func getRecord(
	ctx context.Context,
	cmdable redis.Cmdable,
	recordType, recordID string,
) (*databroker.Record, error) {
	cmd := cmdable.HGet(ctx, recordHashKey, fmt.Sprintf("%s/%s", recordType, recordID))
	raw, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		return nil, storage.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	var record databroker.Record
	err = proto.Unmarshal([]byte(raw), &record)
	if err != nil {
		return nil, storage.ErrNotFound
	}

	return &record, nil
}

func setRecord(
	ctx context.Context,
	cmdable redis.Cmdable,
	record *databroker.Record,
) *redis.IntCmd {
	bs, _ := proto.Marshal(record)
	return cmdable.HSet(ctx, recordHashKey, fmt.Sprintf("%s/%s", record.GetType(), record.GetId()), bs)
}

func setRecordIndexCIDR(
	ctx context.Context,
	cmdable redis.Cmdable,
	recordType, recordID string,
	cidr netip.Prefix,
) *redis.IntCmd {
	start, _ := netutil.PrefixToAddrRange(cidr)
	score := netutil.AddrToFloat(start)
	return cmdable.ZAdd(ctx, fmt.Sprintf("%s/%s", indexCIDRKey, recordType), &redis.Z{
		Member: recordID,
		Score:  score,
	})
}
