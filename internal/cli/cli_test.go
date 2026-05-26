package cli

import (
	"bytes"
	"testing"

	"github.com/gravity182/parq/parquet"
)

func TestPrintMetadata(t *testing.T) {
	repetitionType := parquet.RepetitionTypeRequired
	physicalType := parquet.TypeInt32
	rowGroupFileOffset := int64(11)
	totalCompressedSize := int64(22)
	ordinal := int16(1)
	offsetIndexOffset := int64(33)
	offsetIndexLength := int32(44)
	columnIndexOffset := int64(55)
	columnIndexLength := int32(66)
	indexPageOffset := int64(77)
	dictionaryPageOffset := int64(88)
	bloomFilterOffset := int64(99)
	bloomFilterLength := int32(111)
	meta := &parquet.Metadata{
		Version: 1,
		NumRows: 42,
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             42,
				TotalByteSize:       1000,
				FileOffset:          &rowGroupFileOffset,
				TotalCompressedSize: &totalCompressedSize,
				Ordinal:             &ordinal,
				ColumnChunks: []*parquet.ColumnChunk{
					{
						OffsetIndexOffset: &offsetIndexOffset,
						OffsetIndexLength: &offsetIndexLength,
						ColumnIndexOffset: &columnIndexOffset,
						ColumnIndexLength: &columnIndexLength,
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.TypeInt32,
							Encodings:             []parquet.Encoding{parquet.EncodingPlain, parquet.EncodingRLE},
							PathInSchema:          []string{"schema", "id"},
							Codec:                 parquet.CompressionCodecSnappy,
							NumValues:             42,
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
			},
		},
		Schema: &parquet.Schema{
			Columns: []parquet.ColumnDescriptor{
				{
					Name:               "id",
					Type:               parquet.TypeInt32,
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					PathInSchema:       []string{"schema", "id"},
				},
			},
			Root: &parquet.SchemaNode{
				Name:           "schema",
				RepetitionType: &repetitionType,
				Children: []*parquet.SchemaNode{
					{
						Name:           "id",
						RepetitionType: &repetitionType,
						Type:           &physicalType,
					},
				},
			},
		},
	}

	var out bytes.Buffer
	printMetadata(&out, meta)

	want := "metadata:\n" +
		"  version: 1\n" +
		"  rows: 42\n" +
		"  row groups: 1\n" +
		"  columns: 1\n" +
		"schema:\n" +
		"- schema REQUIRED group\n" +
		"  - id REQUIRED INT32\n" +
		"columns:\n" +
		"- column 0\n" +
		"  path: schema.id\n" +
		"  type: INT32\n" +
		"  max definition level: 1\n" +
		"  max repetition level: 0\n" +
		"row groups:\n" +
		"- row group 0\n" +
		"  rows: 42\n" +
		"  total byte size: 1000\n" +
		"  file offset: 11\n" +
		"  total compressed size: 22\n" +
		"  ordinal: 1\n" +
		"  column chunks:\n" +
		"  - column chunk 0\n" +
		"    offset index offset: 33\n" +
		"    offset index length: 44\n" +
		"    column index offset: 55\n" +
		"    column index length: 66\n" +
		"    metadata:\n" +
		"      path: schema.id\n" +
		"      type: INT32\n" +
		"      codec: SNAPPY\n" +
		"      encodings: PLAIN, RLE\n" +
		"      values: 42\n" +
		"      total uncompressed size: 100\n" +
		"      total compressed size: 80\n" +
		"      data page offset: 200\n" +
		"      index page offset: 77\n" +
		"      dictionary page offset: 88\n" +
		"      bloom filter offset: 99\n" +
		"      bloom filter length: 111\n"

	if got := out.String(); got != want {
		t.Fatalf("printMetadata() = %q, want %q", got, want)
	}
}
