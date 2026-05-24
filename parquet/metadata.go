package parquet

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gravity182/parq/parquet/internal/thrift"
	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

func (r *Reader) Metadata() (*Metadata, error) {
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
	if metadataLen > r.size-12 {
		return nil, fmt.Errorf("invalid metadata length: %d", metadataLen)
	}
	metadataOffset := r.size - 8 - metadataLen
	metadataBuf := make([]byte, metadataLen)
	if _, err := r.r.ReadAt(metadataBuf, metadataOffset); err != nil {
		return nil, fmt.Errorf("read a metadata: %w", err)
	}

	meta := thriftgen.NewFileMetaData()
	if err := thrift.DeserializeFromBytes(context.Background(), metadataBuf, meta); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	schema, err := parseSchema(meta.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}

	rowGroups, err := parseRowGroups(meta.RowGroups)
	if err != nil {
		return nil, fmt.Errorf("parse row groups: %w", err)
	}

	return &Metadata{
		Version:   meta.Version,
		NumRows:   meta.NumRows,
		Schema:    schema,
		RowGroups: rowGroups,
	}, nil
}

func (r *Reader) Schema() (*Schema, error) {
	meta, err := r.Metadata()
	if err != nil {
		return nil, err
	}
	return meta.Schema, nil
}

func dfs(
	idx int,
	level int,
	defLevel int,
	repLevel int,
	path []string,
	leaves *[]ColumnDescriptor,
	schema []*thriftgen.SchemaElement,
) (*SchemaNode, int, error) {
	if idx < 0 || idx >= len(schema) {
		return nil, 0, fmt.Errorf("index %d out of range", idx)
	}

	elem := schema[idx]
	node, err := newSchemaNode(elem)
	if err != nil {
		return nil, 0, fmt.Errorf("map schema node: %w", err)
	}
	if level > 0 {
		path[level-1] = elem.Name
	}
	if level > 0 && node.RepetitionType != nil {
		switch *node.RepetitionType {
		case RepetitionTypeRepeated:
			repLevel++
			defLevel++
		case RepetitionTypeOptional:
			defLevel++
		case RepetitionTypeRequired:
			// nothing
		}
	}

	next := idx + 1
	if node.Type != nil {
		*leaves = append(*leaves, newColumnDescriptor(node, defLevel, repLevel, path, level))
	} else {
		for i := range elem.GetNumChildren() {
			child, childNext, err := dfs(next, level+1, defLevel, repLevel, path, leaves, schema)
			if err != nil {
				return nil, 0, fmt.Errorf("parse child node %d of %q: %w", i, elem.Name, err)
			}

			node.Children = append(node.Children, child)
			next = childNext
		}
	}
	return node, next, nil
}

func newColumnDescriptor(
	node *SchemaNode,
	defLevel int,
	repLevel int,
	path []string,
	level int,
) ColumnDescriptor {
	pathInSchema := make([]string, level)
	copy(pathInSchema, path[:level])

	return ColumnDescriptor{
		Name:               node.Name,
		Type:               *node.Type,
		MaxDefinitionLevel: defLevel,
		MaxRepetitionLevel: repLevel,
		PathInSchema:       pathInSchema,
	}
}

func parseSchema(schema []*thriftgen.SchemaElement) (*Schema, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("schema is empty")
	}

	// root node is not accounted, therefore -1
	path := make([]string, len(schema)-1)
	leaves := make([]ColumnDescriptor, 0, len(schema)-1)
	root, next, err := dfs(0, 0, 0, 0, path, &leaves, schema)
	if err != nil {
		return nil, err
	}
	if next != len(schema) {
		return nil, fmt.Errorf("illegal next idx: %d", next)
	}
	return &Schema{Root: root, Columns: leaves}, nil
}

func parseRowGroups(rowGroups []*thriftgen.RowGroup) ([]*RowGroup, error) {
	res := make([]*RowGroup, 0, len(rowGroups))
	for i, rowGroup := range rowGroups {
		parsed, err := parseRowGroup(rowGroup)
		if err != nil {
			return nil, fmt.Errorf("parse row group %d: %w", i, err)
		}
		res = append(res, parsed)
	}
	return res, nil
}

func parseRowGroup(rowGroup *thriftgen.RowGroup) (*RowGroup, error) {
	if rowGroup == nil {
		return nil, fmt.Errorf("row group is nil")
	}

	columnChunks := make([]*ColumnChunk, 0, len(rowGroup.Columns))
	for i, column := range rowGroup.Columns {
		parsed, err := parseColumnChunk(column)
		if err != nil {
			return nil, fmt.Errorf("parse column chunk %d: %w", i, err)
		}
		columnChunks = append(columnChunks, parsed)
	}

	return &RowGroup{
		ColumnChunks:        columnChunks,
		TotalByteSize:       rowGroup.TotalByteSize,
		NumRows:             rowGroup.NumRows,
		FileOffset:          rowGroup.FileOffset,
		TotalCompressedSize: rowGroup.TotalCompressedSize,
		Ordinal:             rowGroup.Ordinal,
	}, nil
}

func parseColumnChunk(column *thriftgen.ColumnChunk) (*ColumnChunk, error) {
	if column == nil {
		return nil, fmt.Errorf("column chunk is nil")
	}
	if column.MetaData == nil {
		return nil, fmt.Errorf("column metadata is nil")
	}

	metadata, err := parseColumnMetaData(column.MetaData)
	if err != nil {
		return nil, fmt.Errorf("parse column metadata %v: %w", column.MetaData.PathInSchema, err)
	}

	return &ColumnChunk{
		MetaData:          metadata,
		OffsetIndexOffset: column.OffsetIndexOffset,
		OffsetIndexLength: column.OffsetIndexLength,
		ColumnIndexOffset: column.ColumnIndexOffset,
		ColumnIndexLength: column.ColumnIndexLength,
	}, nil
}

func parseColumnMetaData(metadata *thriftgen.ColumnMetaData) (*ColumnMetaData, error) {
	if metadata == nil {
		return nil, fmt.Errorf("column metadata is nil")
	}

	physicalType, err := toInternalPhysicalType(metadata.Type)
	if err != nil {
		return nil, fmt.Errorf("physical type: %w", err)
	}

	encodings, err := toInternalEncodings(metadata.Encodings)
	if err != nil {
		return nil, fmt.Errorf("encodings: %w", err)
	}

	codec, err := toInternalCompressionCodec(metadata.Codec)
	if err != nil {
		return nil, fmt.Errorf("codec: %w", err)
	}

	return &ColumnMetaData{
		Type:                  physicalType,
		Encodings:             encodings,
		PathInSchema:          metadata.PathInSchema,
		Codec:                 codec,
		NumValues:             metadata.NumValues,
		TotalUncompressedSize: metadata.TotalUncompressedSize,
		TotalCompressedSize:   metadata.TotalCompressedSize,
		DataPageOffset:        metadata.DataPageOffset,
		IndexPageOffset:       metadata.IndexPageOffset,
		DictionaryPageOffset:  metadata.DictionaryPageOffset,
		BloomFilterOffset:     metadata.BloomFilterOffset,
		BloomFilterLength:     metadata.BloomFilterLength,
	}, nil
}

type Metadata struct {
	Version   int32
	NumRows   int64
	Schema    *Schema
	RowGroups []*RowGroup
}

type Schema struct {
	Root    *SchemaNode
	Columns []ColumnDescriptor
}

type SchemaNode struct {
	Name           string
	Type           *PhysicalType
	RepetitionType *RepetitionType
	Children       []*SchemaNode
}

type ColumnDescriptor struct {
	Name               string
	Type               PhysicalType
	MaxDefinitionLevel int
	MaxRepetitionLevel int
	PathInSchema       []string
}

type RowGroup struct {
	ColumnChunks        []*ColumnChunk
	TotalByteSize       int64
	NumRows             int64
	FileOffset          *int64
	TotalCompressedSize *int64
	Ordinal             *int16
}

type ColumnChunk struct {
	MetaData          *ColumnMetaData
	OffsetIndexOffset *int64
	OffsetIndexLength *int32
	ColumnIndexOffset *int64
	ColumnIndexLength *int32
}

type ColumnMetaData struct {
	Type                  PhysicalType
	Encodings             []Encoding
	PathInSchema          []string
	Codec                 CompressionCodec
	NumValues             int64
	TotalUncompressedSize int64
	TotalCompressedSize   int64
	DataPageOffset        int64
	IndexPageOffset       *int64
	DictionaryPageOffset  *int64
	BloomFilterOffset     *int64
	BloomFilterLength     *int32
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

type Encoding int64

const (
	EncodingPlain Encoding = iota
	EncodingPlainDictionary
	EncodingRLE
	EncodingBitPacked
	EncodingDeltaBinaryPacked
	EncodingDeltaLengthByteArray
	EncodingDeltaByteArray
	EncodingRLEDictionary
	EncodingByteStreamSplit
)

func toInternalEncoding(t thriftgen.Encoding) (Encoding, error) {
	switch t {
	case thriftgen.Encoding_PLAIN:
		return EncodingPlain, nil
	case thriftgen.Encoding_PLAIN_DICTIONARY:
		return EncodingPlainDictionary, nil
	case thriftgen.Encoding_RLE:
		return EncodingRLE, nil
	case thriftgen.Encoding_BIT_PACKED:
		return EncodingBitPacked, nil
	case thriftgen.Encoding_DELTA_BINARY_PACKED:
		return EncodingDeltaBinaryPacked, nil
	case thriftgen.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return EncodingDeltaLengthByteArray, nil
	case thriftgen.Encoding_DELTA_BYTE_ARRAY:
		return EncodingDeltaByteArray, nil
	case thriftgen.Encoding_RLE_DICTIONARY:
		return EncodingRLEDictionary, nil
	case thriftgen.Encoding_BYTE_STREAM_SPLIT:
		return EncodingByteStreamSplit, nil
	default:
		return 0, fmt.Errorf("unknown parquet encoding: %s", t)
	}
}

func toInternalEncodings(encodings []thriftgen.Encoding) ([]Encoding, error) {
	res := make([]Encoding, 0, len(encodings))
	for _, encoding := range encodings {
		internalEncoding, err := toInternalEncoding(encoding)
		if err != nil {
			return nil, err
		}
		res = append(res, internalEncoding)
	}
	return res, nil
}

func (e Encoding) String() string {
	switch e {
	case EncodingPlain:
		return "PLAIN"
	case EncodingPlainDictionary:
		return "PLAIN_DICTIONARY"
	case EncodingRLE:
		return "RLE"
	case EncodingBitPacked:
		return "BIT_PACKED"
	case EncodingDeltaBinaryPacked:
		return "DELTA_BINARY_PACKED"
	case EncodingDeltaLengthByteArray:
		return "DELTA_LENGTH_BYTE_ARRAY"
	case EncodingDeltaByteArray:
		return "DELTA_BYTE_ARRAY"
	case EncodingRLEDictionary:
		return "RLE_DICTIONARY"
	case EncodingByteStreamSplit:
		return "BYTE_STREAM_SPLIT"
	default:
		return fmt.Sprintf("Encoding(%d)", e)
	}
}

type CompressionCodec int64

const (
	CompressionCodecUncompressed CompressionCodec = iota
	CompressionCodecSnappy
	CompressionCodecGzip
	CompressionCodecLzo
	CompressionCodecBrotli
	CompressionCodecLz4
	CompressionCodecZstd
	CompressionCodecLz4Raw
)

func toInternalCompressionCodec(t thriftgen.CompressionCodec) (CompressionCodec, error) {
	switch t {
	case thriftgen.CompressionCodec_UNCOMPRESSED:
		return CompressionCodecUncompressed, nil
	case thriftgen.CompressionCodec_SNAPPY:
		return CompressionCodecSnappy, nil
	case thriftgen.CompressionCodec_GZIP:
		return CompressionCodecGzip, nil
	case thriftgen.CompressionCodec_LZO:
		return CompressionCodecLzo, nil
	case thriftgen.CompressionCodec_BROTLI:
		return CompressionCodecBrotli, nil
	case thriftgen.CompressionCodec_LZ4:
		return CompressionCodecLz4, nil
	case thriftgen.CompressionCodec_ZSTD:
		return CompressionCodecZstd, nil
	case thriftgen.CompressionCodec_LZ4_RAW:
		return CompressionCodecLz4Raw, nil
	default:
		return 0, fmt.Errorf("unknown parquet compression codec: %s", t)
	}
}

func (c CompressionCodec) String() string {
	switch c {
	case CompressionCodecUncompressed:
		return "UNCOMPRESSED"
	case CompressionCodecSnappy:
		return "SNAPPY"
	case CompressionCodecGzip:
		return "GZIP"
	case CompressionCodecLzo:
		return "LZO"
	case CompressionCodecBrotli:
		return "BROTLI"
	case CompressionCodecLz4:
		return "LZ4"
	case CompressionCodecZstd:
		return "ZSTD"
	case CompressionCodecLz4Raw:
		return "LZ4_RAW"
	default:
		return fmt.Sprintf("CompressionCodec(%d)", c)
	}
}
