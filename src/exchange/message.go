package exchange

import (
	"encoding/binary"
	"reflect"
)

const (
	HeaderLength      = 19
	MagicHigh    byte = 0x12
	MagicLow     byte = 0x34
)

// SerializeType defines serialization type of payload.
type SerializeType byte

const (
	// ProtoBuffer for payload.
	ProtoBuffer SerializeType = iota + 1
	// SerializeNone uses raw []byte and don't serialize/deserialize
	SerializeNone
	// JSON for payload.
	JSON
	// MsgPack for payload
	MsgPack
	// Thrift
	// Thrift for payload
	Thrift
)

type Request struct {
	*Header
	ServicePath    string
	ServiceName    string
	header         map[string]string
	ParameterType  []reflect.Type
	ParameterValue []interface{}
	ServiceVersion string
	//data string
}
type Response struct {
	*Header
	ErrorString string
	Result      interface{}
}
type Header [HeaderLength]byte

//todo 待解决
func (h Header) BodyLength() int32 {
	return int32(binary.BigEndian.Uint32(h[15:]))
}

// SerializeType returns serialization type of payload.
func (h Header) SerializeType() SerializeType {
	return SerializeType(h[4])
}

// IsHeartbeat returns whether the message is heartbeat message.
func (h Header) IsHeartbeat() bool {
	return h[3] == 0x3
}

// IsOneway returns whether the message is one-way message.
// If true, server won't send responses.
func (h Header) IsOneway() bool {
	return h[3] == 0x2
}

// NewRequest creates an empty request.
func NewRequest() *Request {
	header := Header([19]byte{})
	header[0] = MagicHigh
	header[1] = MagicLow

	return &Request{
		Header: &header,
	}
}
func NewResponse() *Response {
	header := Header([19]byte{})
	header[0] = MagicHigh
	header[1] = MagicLow

	return &Response{
		Header: &header,
	}
}
