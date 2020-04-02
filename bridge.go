package mrpc

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/protobuf/proto"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
)

var MessageIDs = sync.Pool{New: func() interface{} {
	return "/" + strconv.FormatUint(rand.Uint64(), 10)
}}

func NewBridge(clt mqtt.Client, target string) grpc.StreamServerInterceptor {
	if !strings.HasPrefix(target, "/") {
		target = "/" + target
	}
	var (
		ResponsePrefix = DefaultRequestPrefix + target
		RequestPrefix  = DefaultResponsePrefix + target
	)

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		// srv will be nil when hit UnknownServiceHandler
		if srv != nil {
			return handler(srv, ss)
		}

		if !clt.IsConnected() {
			if t := clt.Connect(); WaitContext(ss.Context(), t) && t.Error() != nil {
				return t.Error()
			}
		}

		sid := MessageIDs.Get().(string)
		defer MessageIDs.Put(sid)
		ch := make(chan error)
		defer close(ch)

		resp := ResponsePrefix + info.FullMethod + sid
		if t := clt.Subscribe(resp+"/#", 0, func(_ mqtt.Client, msg mqtt.Message) {
			defer msg.Ack()

			code, ok := strToCode[strings.TrimPrefix(msg.Topic(), resp+"/")]
			if !ok {
				ch <- status.Error(codes.Internal, fmt.Sprintf("%x", msg.Payload()))
				return
			} else if code != codes.OK {
				s := new(spb.Status)
				if err := proto.Unmarshal(msg.Payload(), s); err != nil {
					ch <- status.Error(codes.Internal, err.Error())
					return
				}
				s.Code = int32(code)
				ch <- status.FromProto(s).Err()
				return
			}

			if err := ss.SendMsg(OriginalProto{data: msg.Payload()}); err != nil {
				ch <- err
			}

		}); WaitContext(ss.Context(), t) && t.Error() != nil {
			return t.Error()
		}
		defer clt.Unsubscribe(resp)

		go func() {
			reqs := RequestPrefix + info.FullMethod + sid
			rx := &OriginalProto{}
			for {
				if err := ss.RecvMsg(rx); err == io.EOF {
					return
				} else if err != nil {
					return
				}

				if t := clt.Publish(reqs, 0, false, rx); WaitContext(ss.Context(), t) && t.Error() != nil {
					return
				}
			}
		}()

		select {
		case <-ss.Context().Done():
			err = ss.Context().Err()
		case err = <-ch:
		}
		return
	}
}
