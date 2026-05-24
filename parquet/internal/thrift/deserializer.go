package thrift

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

func DeserializeFromBytes(ctx context.Context, b []byte, msg thrift.TStruct) error {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocolFactory := thrift.NewTCompactProtocolFactoryConf(nil)
	protocol := protocolFactory.GetProtocol(transport)

	deserializer := thrift.TDeserializer{
		Transport: transport,
		Protocol:  protocol,
	}
	if err := deserializer.Read(ctx, msg, b); err != nil {
		return fmt.Errorf("thrift deserialize for struct %T: %w", msg, err)
	}
	return nil
}

func GetStreamTransport(r io.Reader) *thrift.StreamTransport {
	return thrift.NewStreamTransportR(r)
}

func DeserializeFromStreamTransport(ctx context.Context, transport *thrift.StreamTransport, msg thrift.TStruct) error {
	protocolFactory := thrift.NewTCompactProtocolFactoryConf(nil)
	protocol := protocolFactory.GetProtocol(transport)

	if err := msg.Read(ctx, protocol); err != nil {
		return fmt.Errorf("thrift deserialize for struct %T: %w", msg, err)
	}
	return nil
}
