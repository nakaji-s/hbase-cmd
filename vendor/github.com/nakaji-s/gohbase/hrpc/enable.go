// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/nakaji-s/gohbase/pb"
)

// EnableTable represents a EnableTable HBase call
type EnableTable struct {
	tableOp
}

// NewEnableTable creates a new EnableTable request that will enable the
// given table in HBase. For use by the admin client.
func NewEnableTable(ctx context.Context, table []byte) *EnableTable {
	return &EnableTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
}

// Name returns the name of this RPC call.
func (et *EnableTable) Name() string {
	return "EnableTable"
}

// ToProto converts the RPC into a protobuf message
func (et *EnableTable) ToProto() (proto.Message, error) {
	table := strings.Split("default:"+string(et.table), ":")
	return &pb.EnableTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte(table[len(table)-2]),
			Qualifier: []byte(table[len(table)-1]),
		},
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (et *EnableTable) NewResponse() proto.Message {
	return &pb.EnableTableResponse{}
}
