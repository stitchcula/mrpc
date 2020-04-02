package mrpc

import (
	"bytes"
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"time"
)

type (
	MethodDesc  = grpc.MethodDesc
	StreamDesc  = grpc.StreamDesc
	ServiceDesc = grpc.ServiceDesc
	ServiceInfo = grpc.ServiceInfo
	MethodInfo  = grpc.MethodInfo
)

const (
	DefaultRequestPrefix  = "/mrpc/request"
	DefaultResponsePrefix = "/mrpc/response"
)

func WaitContext(ctx context.Context, token mqtt.Token) bool {
	dl, ok := ctx.Deadline()
	if !ok {
		return token.Wait()
	}
	return token.WaitTimeout(dl.Sub(time.Now()))
}

var EOF = []byte{0}

type OriginalProto struct {
	data []byte
}

func (orig *OriginalProto) Reset()         { orig.data = orig.data[:0] }
func (orig *OriginalProto) String() string { return fmt.Sprintf("%x", orig.data) }
func (orig *OriginalProto) ProtoMessage()  {}

func (orig *OriginalProto) Unmarshal(byt []byte) error {
	if byt == nil {
		byt = make([]byte, 0, 0)
	}
	orig.data = byt
	return nil
}

func (orig OriginalProto) Marshal() ([]byte, error) {
	return orig.data, nil
}

// Concat NOT change itself
func (orig *OriginalProto) Concat(subs ...*OriginalProto) *OriginalProto {
	bf := bytes.NewBufferString(string(orig.data))
	for _, sub := range subs {
		bf.Write(sub.data)
	}
	return &OriginalProto{data: bf.Bytes()}
}

var strToCode = map[string]codes.Code{
	`OK`: codes.OK,
	`CANCELLED`:/* [sic] */ codes.Canceled,
	`UNKNOWN`:             codes.Unknown,
	`INVALID_ARGUMENT`:    codes.InvalidArgument,
	`DEADLINE_EXCEEDED`:   codes.DeadlineExceeded,
	`NOT_FOUND`:           codes.NotFound,
	`ALREADY_EXISTS`:      codes.AlreadyExists,
	`PERMISSION_DENIED`:   codes.PermissionDenied,
	`RESOURCE_EXHAUSTED`:  codes.ResourceExhausted,
	`FAILED_PRECONDITION`: codes.FailedPrecondition,
	`ABORTED`:             codes.Aborted,
	`OUT_OF_RANGE`:        codes.OutOfRange,
	`UNIMPLEMENTED`:       codes.Unimplemented,
	`INTERNAL`:            codes.Internal,
	`UNAVAILABLE`:         codes.Unavailable,
	`DATA_LOSS`:           codes.DataLoss,
	`UNAUTHENTICATED`:     codes.Unauthenticated,
}
