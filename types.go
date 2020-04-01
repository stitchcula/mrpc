package mrpc

import (
	"bytes"
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/grpc"
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
