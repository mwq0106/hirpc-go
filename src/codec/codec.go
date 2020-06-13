package codec

import (
	"fmt"

	proto "github.com/gogo/protobuf/proto"
	pb "github.com/golang/protobuf/proto"
)

// Codec defines the interface that decode/encode payload.
type Codec interface {
	Encode(i interface{}) ([]byte, error)
	Decode(data []byte, i interface{}) error
}

// PBCodec uses protobuf marshaler and unmarshaler.
type PBCodec struct{}

// Encode encodes an object into slice of bytes.
func (c PBCodec) Encode(i interface{}) ([]byte, error) {
	if m, ok := i.(proto.Marshaler); ok {
		return m.Marshal()
	}

	if m, ok := i.(pb.Message); ok {
		return pb.Marshal(m)
	}

	return nil, fmt.Errorf("%T is not a proto.Marshaler", i)
}

// Decode decodes an object from slice of bytes.
func (c PBCodec) Decode(data []byte, i interface{}) error {
	if m, ok := i.(proto.Unmarshaler); ok {
		return m.Unmarshal(data)
	}

	if m, ok := i.(pb.Message); ok {
		return pb.Unmarshal(data, m)
	}

	return fmt.Errorf("%T is not a proto.Unmarshaler", i)
}
