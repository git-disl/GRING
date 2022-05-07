package GRING

import (
	"encoding/binary"
	//"errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"io"
	"fmt"
	"context"
	"math"
)

type message struct {
	length uint32
	nonce uint64
	recv_nonce uint64
	data  []byte
}

func (m message) marshal(dst []byte) []byte {
	dst = append(dst, make([]byte, 20)...)
	binary.BigEndian.PutUint32(dst[:4], m.length)
	binary.BigEndian.PutUint64(dst[4:12], m.nonce)
	binary.BigEndian.PutUint64(dst[12:20], m.recv_nonce)
	dst = append(dst, m.data...)

	return dst
}

func unmarshalMessage(data []byte) (message, error) {
	if len(data) < 20 {
		return message{}, io.ErrUnexpectedEOF
	}

	length := binary.BigEndian.Uint32(data[:4])
	nonce := binary.BigEndian.Uint64(data[4:12])
	recv_nonce := binary.BigEndian.Uint64(data[12:20])
	data = data[20:]

	return message{length: length, nonce: nonce, recv_nonce: recv_nonce, data: data}, nil
}

// HandlerContext provides contextual information upon the recipient of data from an inbound/outbound connection. It
// provides the option of responding to a request should the data received be of a request.
type HandlerContext struct {
	client *Client
	msg    message
	sent   atomic.Bool
}

// ID returns the ID of the inbound/outbound peer that sent you the data that is currently being handled.
func (ctx *HandlerContext) ID() ID {
	return ctx.client.ID()
}

// Logger returns the logger instance associated to the inbound/outbound peer being handled.
func (ctx *HandlerContext) Logger() *zap.Logger {
	return ctx.client.Logger()
}

// Data returns the raw bytes that some peer has sent to you.
//
// Data may be called concurrently.
func (ctx *HandlerContext) Data() []byte {
	return ctx.msg.data
}

// IsRequest marks whether or not the data received was intended to be of a request.
//
// IsRequest may be called concurrently.
func (ctx *HandlerContext) IsRequest() bool {
	return ctx.msg.nonce > 0
}

// Send sends data back to the peer that has sent you data. Should the data the peer send you be of a request, Send
// will send data back as a response. It returns an error if multiple responses attempt to be sent to a single request,
// or if an error occurred while attempting to send the peer a message.
//
// Send may be called concurrently.
func (ctx *HandlerContext) Send(data []byte) error {
        var length uint32 
        var start uint32
	var end uint32
	start = uint32(0)
	end = uint32(0)

	if(uint32(len(data)) >= (ctx.client.node.maxRecvMessageSize-uint32(160))) {
            var msize uint32
            msize=0
            length=0
            for msize < uint32(len(data)) {
                length++
                msize += ctx.client.node.maxRecvMessageSize-uint32(160) //consider control field in a message: length(uint32) and nonce(uint64) recv_nonce(uint64)
            }
            //fmt.Printf("ctx send: Bulk sending. data size: %d , node.maxRecvMessageSize : %d length : %d\n",len(data), ctx.client.node.maxRecvMessageSize, length)

            recv_nonce := uint64(math.MaxUint64)
            for i := length; i > uint32(0); i-- {
                if uint32(len(data)) - end < ctx.client.node.maxRecvMessageSize-uint32(160) {
                    end = uint32(len(data))
                }else {
                    end += (ctx.client.node.maxRecvMessageSize-uint32(160))
                }

                ch, nonce, err := ctx.client.requests.nextNonce()
                if err != nil {
                    fmt.Printf("request nonce error\n")
                    return err
                }

		// TODO : change msgctx with HandlerContext
		msgctx, cancel := context.WithCancel(context.Background())
                //fmt.Printf("ctx send: send request nonce:%d, recv_nonce:%d length:%d start:%d , end:%d\n",nonce, recv_nonce, i,start,end)
                if err := ctx.client.send(nonce, recv_nonce, data[start:end], i); err != nil {
                    fmt.Printf("request send error\n")
                    ctx.client.requests.markRequestFailed(nonce)
                    return err
                }
                //fmt.Printf("ctx send: wait for ACK\n")
                var msg message
                select {
                    case msg = <-ch:
                        // TODO check ACK
			if recv_nonce == uint64(math.MaxUint64) {
                            recv_nonce = msg.recv_nonce
                        }
                        //fmt.Printf("ACK received. recv_nonce:%d\n",recv_nonce)
		        cancel()
                        start = end
			continue
                    case <-msgctx.Done():
                        fmt.Printf("msgctx done\n")
                        return msgctx.Err()
                }
            }//for
	    //fmt.Printf("ctx send: send request done\n")

            return nil
        }else { // single unit size
            if err := ctx.client.send(ctx.msg.nonce, math.MaxUint64, data, 0); err != nil {
                 return err
            }
        }
	return nil
}

// DecodeMessage decodes the raw bytes that some peer has sent you into a Go type. The Go type must have previously
// been registered to the node to which the handler this context is under was registered on. An error is thrown
// otherwise.
//
// It is highly recommended that should you choose to have your application utilize GRING's serialization/
// deserialization framework for data over-the-wire, that all handlers use them by default.
//
// DecodeMessage may be called concurrently.
func (ctx *HandlerContext) DecodeMessage() (Serializable, error) {
	return ctx.client.node.DecodeMessage(ctx.Data())
}

// SendMessage encodes and serializes a Go type into a byte slice, and sends data back to the peer that has sent you
// data as either a response or message. Refer to (*HandlerContext).Send for more details. An error is thrown if
// the Go type passed in has not been registered to the node to which the handler this context is under was registered
// on.
//
// It is highly recommended that should you choose to have your application utilize GRING's
// serialization/deserialization framework for data over-the-wire, that all handlers use them by default.
//
// SendMessage may be called concurrently.
func (ctx *HandlerContext) SendMessage(msg Serializable) error {
	data, err := ctx.client.node.EncodeMessage(msg)
	if err != nil {
		return err
	}

	return ctx.Send(data)
}
