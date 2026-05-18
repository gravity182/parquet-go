package thrift

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

func Deserialize(ctx context.Context, b []byte, msg thrift.TStruct) error {
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
