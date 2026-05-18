package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/gravity182/parq/parquet/internal/thrift"
	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

type Reader struct {
	r      io.ReaderAt
	size   int64
	closer io.Closer
}

func NewReader(r io.ReaderAt, size int64, closer io.Closer) *Reader {
	return &Reader{
		r:      r,
		size:   size,
		closer: closer,
	}
}

func OpenFile(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open a Parquet file '%q': %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat a Parquet file '%q': %w", path, err)
	}
	return NewReader(f, info.Size(), f), nil
}

func (r *Reader) Close() error {
	if r.closer == nil {
		return nil
	}
	if err := r.closer.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}

func (r *Reader) Metadata() (*Metadata, error) {
	// TODO: might want to utilize OS page cache better; think read order

	// header (4 bytes) + footer (8 bytes) at min required
	if r.size < 12 {
		return nil, fmt.Errorf("size is too small")
	}

	headerBuf := make([]byte, 4)
	if _, err := r.r.ReadAt(headerBuf, 0); err != nil {
		return nil, fmt.Errorf("read a header: %w", err)
	}
	if string(headerBuf) != MAGIC {
		return nil, fmt.Errorf("header magic mismatch: got %q, want %q", headerBuf, MAGIC)
	}

	footerBuf := make([]byte, 8)
	if _, err := r.r.ReadAt(footerBuf, r.size-8); err != nil {
		return nil, fmt.Errorf("read a footer: %w", err)
	}
	if string(footerBuf[4:8]) != MAGIC {
		return nil, fmt.Errorf("footer magic mismatch: got %q, want %q", footerBuf, MAGIC)
	}

	metadataLen := int64(binary.LittleEndian.Uint32(footerBuf[:4]))
	if metadataLen < 0 || metadataLen > r.size-12 {
		return nil, fmt.Errorf("invalid metadata length: %d", metadataLen)
	}

	metadataOffset := r.size - 8 - metadataLen
	metadataBuf := make([]byte, metadataLen)
	if _, err := r.r.ReadAt(metadataBuf, metadataOffset); err != nil {
		return nil, fmt.Errorf("read a metadata: %w", err)
	}

	meta := thriftgen.NewFileMetaData()
	if err := thrift.Deserialize(context.Background(), metadataBuf, meta); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	schema, err := parseSchema(meta.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}

	return &Metadata{
		Version:      meta.Version,
		NumRows:      meta.NumRows,
		NumRowGroups: len(meta.RowGroups),
		Schema:       schema,
	}, nil
}

func (r *Reader) Schema() (*Schema, error) {
	meta, err := r.Metadata()
	if err != nil {
		return nil, err
	}
	return meta.Schema, nil
}

func dfs(idx int, schema []*thriftgen.SchemaElement) (*SchemaNode, int, error) {
	if idx >= len(schema) {
		return nil, 0, fmt.Errorf("index %d out of range", idx)
	}

	elem := schema[idx]
	node, err := newSchemaNode(elem)
	if err != nil {
		return nil, 0, fmt.Errorf("map schema node: %w", err)
	}

	next := idx + 1
	for i := range elem.GetNumChildren() {
		child, childNext, err := dfs(next, schema)
		if err != nil {
			return nil, 0, fmt.Errorf("parse child node %d of %q: %w", i, elem.Name, err)
		}

		node.Children = append(node.Children, child)
		next = childNext
	}

	return node, next, nil
}

func parseSchema(schema []*thriftgen.SchemaElement) (*Schema, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("schema is empty")
	}

	root, _, err := dfs(0, schema)
	if err != nil {
		return nil, err
	}
	return &Schema{Root: root}, nil
}

type Metadata struct {
	Version      int32
	NumRows      int64
	NumRowGroups int
	Schema       *Schema
}

type Schema struct {
	Root *SchemaNode
}

type SchemaNode struct {
	Name           string
	Type           *PhysicalType
	RepetitionType *RepetitionType
	Children       []*SchemaNode
}

func newSchemaNode(elem *thriftgen.SchemaElement) (*SchemaNode, error) {
	node := &SchemaNode{Name: elem.GetName()}

	if elem.Type != nil {
		physicalType, err := toInternalPhysicalType(*elem.Type)
		if err != nil {
			return nil, fmt.Errorf("map physical type for schema element %q: %w", elem.GetName(), err)
		}
		node.Type = &physicalType

		if elem.GetNumChildren() > 0 {
			return nil, fmt.Errorf("Leaf nodes shouldn't have any children, but got %d for node %q", elem.GetNumChildren(), elem.GetName())
		}
	}

	if elem.RepetitionType != nil {
		repetitionType, err := toInternalRepetitionType(*elem.RepetitionType)
		if err != nil {
			return nil, fmt.Errorf("map repetition type for schema element %q: %w", elem.GetName(), err)
		}
		node.RepetitionType = &repetitionType
	}

	return node, nil
}

type PhysicalType int64

const (
	TypeBoolean PhysicalType = iota
	TypeInt32
	TypeInt64
	TypeInt96
	TypeFloat
	TypeDouble
	TypeByteArray
	TypeFixedLenByteArray
)

func toInternalPhysicalType(t thriftgen.Type) (PhysicalType, error) {
	switch t {
	case thriftgen.Type_BOOLEAN:
		return TypeBoolean, nil
	case thriftgen.Type_INT32:
		return TypeInt32, nil
	case thriftgen.Type_INT64:
		return TypeInt64, nil
	case thriftgen.Type_INT96:
		return TypeInt96, nil
	case thriftgen.Type_FLOAT:
		return TypeFloat, nil
	case thriftgen.Type_DOUBLE:
		return TypeDouble, nil
	case thriftgen.Type_BYTE_ARRAY:
		return TypeByteArray, nil
	case thriftgen.Type_FIXED_LEN_BYTE_ARRAY:
		return TypeFixedLenByteArray, nil
	default:
		return 0, fmt.Errorf("unknown parquet physical type: %s", t)
	}
}

func (t PhysicalType) String() string {
	switch t {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeInt96:
		return "INT96"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeByteArray:
		return "BYTE_ARRAY"
	case TypeFixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return fmt.Sprintf("PhysicalType(%d)", t)
	}
}

type RepetitionType int64

const (
	RepetitionTypeRequired RepetitionType = iota
	RepetitionTypeOptional
	RepetitionTypeRepeated
)

func toInternalRepetitionType(t thriftgen.FieldRepetitionType) (RepetitionType, error) {
	switch t {
	case thriftgen.FieldRepetitionType_REQUIRED:
		return RepetitionTypeRequired, nil
	case thriftgen.FieldRepetitionType_OPTIONAL:
		return RepetitionTypeOptional, nil
	case thriftgen.FieldRepetitionType_REPEATED:
		return RepetitionTypeRepeated, nil
	default:
		return 0, fmt.Errorf("unknown parquet repetition type: %s", t)
	}
}

func (t RepetitionType) String() string {
	switch t {
	case RepetitionTypeRequired:
		return "REQUIRED"
	case RepetitionTypeOptional:
		return "OPTIONAL"
	case RepetitionTypeRepeated:
		return "REPEATED"
	default:
		return fmt.Sprintf("RepetitionType(%d)", t)
	}
}
