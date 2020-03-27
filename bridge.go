package mrpc

import (
	"bytes"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"strconv"
	"sync"
)

var MessageIDs = sync.Pool{New: func() interface{} {
	return "/" + strconv.FormatUint(rand.Uint64(), 10)
}}

func NewBridge(clt mqtt.Client) grpc.StreamServerInterceptor {
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

		resp := "/mrpc/response" + info.FullMethod + sid
		if t := clt.Subscribe(resp, 0, func(_ mqtt.Client, msg mqtt.Message) {
			if bytes.Equal(EOF, msg.Payload()) {
				ch <- nil
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
			reqs := "/mrpc/request" + info.FullMethod + sid
			rx := &OriginalProto{}
			defer clt.Publish(reqs, 0, false, EOF)
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
