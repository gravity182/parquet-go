package parquet

import (
	"slices"
	"strings"
	"testing"

	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

func schemaGroup(name string, repetition thriftgen.FieldRepetitionType, numChildren int32) *thriftgen.SchemaElement {
	return &thriftgen.SchemaElement{
		Name:           name,
		RepetitionType: &repetition,
		NumChildren:    &numChildren,
	}
}

func schemaLeaf(name string, repetition thriftgen.FieldRepetitionType, physicalType thriftgen.Type) *thriftgen.SchemaElement {
	return &thriftgen.SchemaElement{
		Name:           name,
		RepetitionType: &repetition,
		Type:           &physicalType,
	}
}

func TestParseSchemaFlatOptionalColumnDescriptor(t *testing.T) {
	schema, err := parseSchema([]*thriftgen.SchemaElement{
		schemaGroup("root", thriftgen.FieldRepetitionType_REQUIRED, 1),
		schemaLeaf("id", thriftgen.FieldRepetitionType_OPTIONAL, thriftgen.Type_INT32),
	})
	if err != nil {
		t.Fatalf("parseSchema returned error: %v", err)
	}
	if len(schema.Columns) != 1 {
		t.Fatalf("len(Columns) = %d, want 1", len(schema.Columns))
	}

	column := schema.Columns[0]
	if column.Name != "id" {
		t.Fatalf("Name = %q, want %q", column.Name, "id")
	}
	if column.Type != TypeInt32 {
		t.Fatalf("Type = %v, want %v", column.Type, TypeInt32)
	}
	if column.MaxDefinitionLevel != 1 {
		t.Fatalf("MaxDefinitionLevel = %d, want 1", column.MaxDefinitionLevel)
	}
	if column.MaxRepetitionLevel != 0 {
		t.Fatalf("MaxRepetitionLevel = %d, want 0", column.MaxRepetitionLevel)
	}
	if !slices.Equal(column.PathInSchema, []string{"id"}) {
		t.Fatalf("PathInSchema = %v, want [id]", column.PathInSchema)
	}
}

func TestParseSchemaNestedOptionalGroupColumnDescriptor(t *testing.T) {
	schema, err := parseSchema([]*thriftgen.SchemaElement{
		schemaGroup("root", thriftgen.FieldRepetitionType_REQUIRED, 1),
		schemaGroup("user", thriftgen.FieldRepetitionType_OPTIONAL, 1),
		schemaLeaf("name", thriftgen.FieldRepetitionType_REQUIRED, thriftgen.Type_BYTE_ARRAY),
	})
	if err != nil {
		t.Fatalf("parseSchema returned error: %v", err)
	}
	if len(schema.Columns) != 1 {
		t.Fatalf("len(Columns) = %d, want 1", len(schema.Columns))
	}

	column := schema.Columns[0]
	if column.Name != "name" {
		t.Fatalf("Name = %q, want %q", column.Name, "name")
	}
	if column.Type != TypeByteArray {
		t.Fatalf("Type = %v, want %v", column.Type, TypeByteArray)
	}
	if column.MaxDefinitionLevel != 1 {
		t.Fatalf("MaxDefinitionLevel = %d, want 1", column.MaxDefinitionLevel)
	}
	if column.MaxRepetitionLevel != 0 {
		t.Fatalf("MaxRepetitionLevel = %d, want 0", column.MaxRepetitionLevel)
	}
	if !slices.Equal(column.PathInSchema, []string{"user", "name"}) {
		t.Fatalf("PathInSchema = %v, want [user name]", column.PathInSchema)
	}
}

func TestParseSchemaRepeatedColumnDescriptor(t *testing.T) {
	schema, err := parseSchema([]*thriftgen.SchemaElement{
		schemaGroup("root", thriftgen.FieldRepetitionType_REQUIRED, 1),
		schemaLeaf("tags", thriftgen.FieldRepetitionType_REPEATED, thriftgen.Type_INT32),
	})
	if err != nil {
		t.Fatalf("parseSchema returned error: %v", err)
	}
	if len(schema.Columns) != 1 {
		t.Fatalf("len(Columns) = %d, want 1", len(schema.Columns))
	}

	column := schema.Columns[0]
	if column.Name != "tags" {
		t.Fatalf("Name = %q, want %q", column.Name, "tags")
	}
	if column.Type != TypeInt32 {
		t.Fatalf("Type = %v, want %v", column.Type, TypeInt32)
	}
	if column.MaxDefinitionLevel != 1 {
		t.Fatalf("MaxDefinitionLevel = %d, want 1", column.MaxDefinitionLevel)
	}
	if column.MaxRepetitionLevel != 1 {
		t.Fatalf("MaxRepetitionLevel = %d, want 1", column.MaxRepetitionLevel)
	}
	if !slices.Equal(column.PathInSchema, []string{"tags"}) {
		t.Fatalf("PathInSchema = %v, want [tags]", column.PathInSchema)
	}
}

func TestParseSchemaColumnDescriptorPathsAreStableCopies(t *testing.T) {
	schema, err := parseSchema([]*thriftgen.SchemaElement{
		schemaGroup("root", thriftgen.FieldRepetitionType_REQUIRED, 2),
		schemaGroup("user", thriftgen.FieldRepetitionType_OPTIONAL, 1),
		schemaLeaf("name", thriftgen.FieldRepetitionType_REQUIRED, thriftgen.Type_BYTE_ARRAY),
		schemaGroup("event", thriftgen.FieldRepetitionType_OPTIONAL, 1),
		schemaLeaf("id", thriftgen.FieldRepetitionType_REQUIRED, thriftgen.Type_INT64),
	})
	if err != nil {
		t.Fatalf("parseSchema returned error: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("len(Columns) = %d, want 2", len(schema.Columns))
	}
	if !slices.Equal(schema.Columns[0].PathInSchema, []string{"user", "name"}) {
		t.Fatalf("Columns[0].PathInSchema = %v, want [user name]", schema.Columns[0].PathInSchema)
	}
	if !slices.Equal(schema.Columns[1].PathInSchema, []string{"event", "id"}) {
		t.Fatalf("Columns[1].PathInSchema = %v, want [event id]", schema.Columns[1].PathInSchema)
	}
}

func TestParseRowGroups(t *testing.T) {
	filePath := "columns.parquet"
	rowGroupFileOffset := int64(11)
	totalCompressedSize := int64(22)
	ordinal := int16(3)
	offsetIndexOffset := int64(44)
	offsetIndexLength := int32(55)
	columnIndexOffset := int64(66)
	columnIndexLength := int32(77)
	indexPageOffset := int64(88)
	dictionaryPageOffset := int64(99)
	bloomFilterOffset := int64(111)
	bloomFilterLength := int32(222)

	rowGroups, err := parseRowGroups([]*thriftgen.RowGroup{
		{
			Columns: []*thriftgen.ColumnChunk{
				{
					FilePath:          &filePath,
					FileOffset:        123,
					OffsetIndexOffset: &offsetIndexOffset,
					OffsetIndexLength: &offsetIndexLength,
					ColumnIndexOffset: &columnIndexOffset,
					ColumnIndexLength: &columnIndexLength,
					MetaData: &thriftgen.ColumnMetaData{
						Type:                  thriftgen.Type_INT32,
						Encodings:             []thriftgen.Encoding{thriftgen.Encoding_PLAIN, thriftgen.Encoding_RLE},
						PathInSchema:          []string{"user", "id"},
						Codec:                 thriftgen.CompressionCodec_SNAPPY,
						NumValues:             10,
						TotalUncompressedSize: 100,
						TotalCompressedSize:   80,
						DataPageOffset:        200,
						IndexPageOffset:       &indexPageOffset,
						DictionaryPageOffset:  &dictionaryPageOffset,
						BloomFilterOffset:     &bloomFilterOffset,
						BloomFilterLength:     &bloomFilterLength,
					},
				},
			},
			TotalByteSize:       1000,
			NumRows:             10,
			FileOffset:          &rowGroupFileOffset,
			TotalCompressedSize: &totalCompressedSize,
			Ordinal:             &ordinal,
		},
	})
	if err != nil {
		t.Fatalf("parseRowGroups returned error: %v", err)
	}
	if len(rowGroups) != 1 {
		t.Fatalf("len(rowGroups) = %d, want 1", len(rowGroups))
	}

	rowGroup := rowGroups[0]
	if rowGroup.TotalByteSize != 1000 {
		t.Fatalf("TotalByteSize = %d, want 1000", rowGroup.TotalByteSize)
	}
	if rowGroup.NumRows != 10 {
		t.Fatalf("NumRows = %d, want 10", rowGroup.NumRows)
	}
	if rowGroup.FileOffset == nil || *rowGroup.FileOffset != rowGroupFileOffset {
		t.Fatalf("FileOffset = %v, want %d", rowGroup.FileOffset, rowGroupFileOffset)
	}
	if rowGroup.TotalCompressedSize == nil || *rowGroup.TotalCompressedSize != totalCompressedSize {
		t.Fatalf("TotalCompressedSize = %v, want %d", rowGroup.TotalCompressedSize, totalCompressedSize)
	}
	if rowGroup.Ordinal == nil || *rowGroup.Ordinal != ordinal {
		t.Fatalf("Ordinal = %v, want %d", rowGroup.Ordinal, ordinal)
	}
	if len(rowGroup.ColumnChunks) != 1 {
		t.Fatalf("len(Columns) = %d, want 1", len(rowGroup.ColumnChunks))
	}

	column := rowGroup.ColumnChunks[0]
	if column.OffsetIndexOffset == nil || *column.OffsetIndexOffset != offsetIndexOffset {
		t.Fatalf("OffsetIndexOffset = %v, want %d", column.OffsetIndexOffset, offsetIndexOffset)
	}
	if column.OffsetIndexLength == nil || *column.OffsetIndexLength != offsetIndexLength {
		t.Fatalf("OffsetIndexLength = %v, want %d", column.OffsetIndexLength, offsetIndexLength)
	}
	if column.ColumnIndexOffset == nil || *column.ColumnIndexOffset != columnIndexOffset {
		t.Fatalf("ColumnIndexOffset = %v, want %d", column.ColumnIndexOffset, columnIndexOffset)
	}
	if column.ColumnIndexLength == nil || *column.ColumnIndexLength != columnIndexLength {
		t.Fatalf("ColumnIndexLength = %v, want %d", column.ColumnIndexLength, columnIndexLength)
	}

	meta := column.MetaData
	if meta == nil {
		t.Fatal("MetaData is nil")
	}
	if meta.Type != TypeInt32 {
		t.Fatalf("Type = %v, want %v", meta.Type, TypeInt32)
	}
	if len(meta.Encodings) != 2 || meta.Encodings[0] != EncodingPlain || meta.Encodings[1] != EncodingRLE {
		t.Fatalf("Encodings = %v, want [%v %v]", meta.Encodings, EncodingPlain, EncodingRLE)
	}
	if len(meta.PathInSchema) != 2 || meta.PathInSchema[0] != "user" || meta.PathInSchema[1] != "id" {
		t.Fatalf("PathInSchema = %v, want [user id]", meta.PathInSchema)
	}
	if meta.Codec != CompressionCodecSnappy {
		t.Fatalf("Codec = %v, want %v", meta.Codec, CompressionCodecSnappy)
	}
	if meta.NumValues != 10 {
		t.Fatalf("NumValues = %d, want 10", meta.NumValues)
	}
	if meta.TotalUncompressedSize != 100 {
		t.Fatalf("TotalUncompressedSize = %d, want 100", meta.TotalUncompressedSize)
	}
	if meta.TotalCompressedSize != 80 {
		t.Fatalf("TotalCompressedSize = %d, want 80", meta.TotalCompressedSize)
	}
	if meta.DataPageOffset != 200 {
		t.Fatalf("DataPageOffset = %d, want 200", meta.DataPageOffset)
	}
	if meta.IndexPageOffset == nil || *meta.IndexPageOffset != indexPageOffset {
		t.Fatalf("IndexPageOffset = %v, want %d", meta.IndexPageOffset, indexPageOffset)
	}
	if meta.DictionaryPageOffset == nil || *meta.DictionaryPageOffset != dictionaryPageOffset {
		t.Fatalf("DictionaryPageOffset = %v, want %d", meta.DictionaryPageOffset, dictionaryPageOffset)
	}
	if meta.BloomFilterOffset == nil || *meta.BloomFilterOffset != bloomFilterOffset {
		t.Fatalf("BloomFilterOffset = %v, want %d", meta.BloomFilterOffset, bloomFilterOffset)
	}
	if meta.BloomFilterLength == nil || *meta.BloomFilterLength != bloomFilterLength {
		t.Fatalf("BloomFilterLength = %v, want %d", meta.BloomFilterLength, bloomFilterLength)
	}
}

func TestParseRowGroupsNilRowGroup(t *testing.T) {
	_, err := parseRowGroups([]*thriftgen.RowGroup{nil})
	if err == nil {
		t.Fatal("parseRowGroups returned nil error")
	}
	if !strings.Contains(err.Error(), "parse row group 0: row group is nil") {
		t.Fatalf("error = %q, want nil row group context", err)
	}
}

func TestParseRowGroupsNilColumnChunk(t *testing.T) {
	_, err := parseRowGroups([]*thriftgen.RowGroup{
		{Columns: []*thriftgen.ColumnChunk{nil}},
	})
	if err == nil {
		t.Fatal("parseRowGroups returned nil error")
	}
	if !strings.Contains(err.Error(), "parse row group 0: parse column chunk 0: column chunk is nil") {
		t.Fatalf("error = %q, want nil column chunk context", err)
	}
}

func TestParseRowGroupsNilColumnMetadata(t *testing.T) {
	_, err := parseRowGroups([]*thriftgen.RowGroup{
		{Columns: []*thriftgen.ColumnChunk{{}}},
	})
	if err == nil {
		t.Fatal("parseRowGroups returned nil error")
	}
	if !strings.Contains(err.Error(), "parse row group 0: parse column chunk 0: column metadata is nil") {
		t.Fatalf("error = %q, want nil column metadata context", err)
	}
}
