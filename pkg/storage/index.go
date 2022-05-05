package storage

import (
	"net/netip"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	indexField = "$index"
	cidrField  = "cidr"
)

// GetIndexCIDR returns the $index.cidr for a record's data. If none is available nil is returned.
func GetIndexCIDR(msg proto.Message) *netip.Prefix {
	var s *structpb.Struct
	if sv, ok := msg.(*structpb.Value); ok {
		s = sv.GetStructValue()
	} else {
		s, _ = msg.(*structpb.Struct)
	}
	if s == nil {
		return nil
	}

	f, ok := s.Fields[indexField]
	if !ok {
		return nil
	}

	obj := f.GetStructValue()
	if obj == nil {
		return nil
	}

	cf, ok := obj.Fields[cidrField]
	if !ok {
		return nil
	}

	c := cf.GetStringValue()
	if c == "" {
		return nil
	}

	prefix, err := netip.ParsePrefix(c)
	if err != nil {
		return nil
	}
	return &prefix
}
