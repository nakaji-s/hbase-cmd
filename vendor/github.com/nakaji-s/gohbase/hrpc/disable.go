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

// DisableTable represents a DisableTable HBase call
type DisableTable struct {
	tableOp
}

// NewDisableTable creates a new DisableTable request that will disable the
// given table in HBase. For use by the admin client.
func NewDisableTable(ctx context.Context, table []byte) *DisableTable {
	return &DisableTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
}

// Name returns the name of this RPC call.
func (dt *DisableTable) Name() string {
	return "DisableTable"
}

// ToProto converts the RPC into a protobuf message
func (dt *DisableTable) ToProto() (proto.Message, error) {
	table := strings.Split("default:"+string(dt.table), ":")
	return &pb.DisableTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte(table[len(table)-2]),
			Qualifier: []byte(table[len(table)-1]),
		},
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DisableTable) NewResponse() proto.Message {
	return &pb.DisableTableResponse{}
}
