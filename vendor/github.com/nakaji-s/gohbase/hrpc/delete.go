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

// DeleteTable represents a DeleteTable HBase call
type DeleteTable struct {
	tableOp
}

// NewDeleteTable creates a new DeleteTable request that will delete the
// given table in HBase. For use by the admin client.
func NewDeleteTable(ctx context.Context, table []byte) *DeleteTable {
	return &DeleteTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
}

// Name returns the name of this RPC call.
func (dt *DeleteTable) Name() string {
	return "DeleteTable"
}

// ToProto converts the RPC into a protobuf message
func (dt *DeleteTable) ToProto() (proto.Message, error) {
	table := strings.Split("default:"+string(dt.table), ":")
	return &pb.DeleteTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte(table[len(table)-2]),
			Qualifier: []byte(table[len(table)-1]),
		},
	}, nil
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DeleteTable) NewResponse() proto.Message {
	return &pb.DeleteTableResponse{}
}
