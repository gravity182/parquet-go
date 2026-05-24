package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/gravity182/parq/parquet/internal/thrift"
	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

type Scanner struct {
	r              io.ReaderAt
	meta           *Metadata
	rowGroupCursor int
}

func NewScanner(r io.ReaderAt, meta *Metadata) *Scanner {
	return &Scanner{
		r:              r,
		meta:           meta,
		rowGroupCursor: 0,
	}
}

func (r *Reader) Scanner() (*Scanner, error) {
	meta, err := r.Metadata()
	if err != nil {
		return nil, fmt.Errorf("get metadata: %w", err)
	}
	return NewScanner(r.r, meta), nil
}

func (s *Scanner) Rows() error {
	for _, rowGroup := range s.meta.RowGroups {
		err := parseRowGroupData(rowGroup, s.meta.Schema, s.r)
		if err != nil {
			return err
		}
	}
	return nil
}

func parseRowGroupData(rowGroup *RowGroup, schema *Schema, r io.ReaderAt) error {
	for columnChunkIdx, columnChunk := range rowGroup.ColumnChunks {
		column := schema.Columns[columnChunkIdx]
		fmt.Printf("Column: %q\n", column.Name)

		err := parseColumnChunkData(columnChunk, column, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func parseColumnChunkData(columnChunk *ColumnChunk, column ColumnDescriptor, r io.ReaderAt) error {
	chunkMeta := columnChunk.MetaData
	if chunkMeta.Codec != CompressionCodecUncompressed {
		return fmt.Errorf("Unsupported compression codec: %q", chunkMeta.Codec)
	}
	// buffered io
	transport := thrift.GetStreamTransport(io.NewSectionReader(r, chunkMeta.DataPageOffset, chunkMeta.TotalCompressedSize))

	totalNumValues := chunkMeta.NumValues
	fmt.Printf("Total num values: %d\n", totalNumValues)

	collectedNumValues := int64(0)
	for collectedNumValues < totalNumValues {
		header := thriftgen.NewPageHeader()
		if err := thrift.DeserializeFromStreamTransport(context.Background(), transport, header); err != nil {
			return fmt.Errorf("read header")
		}
		if header.Type != thriftgen.PageType_DATA_PAGE {
			return fmt.Errorf("unsupported page type: %q", header.Type)
		}

		pageNumValues := int(header.DataPageHeader.NumValues)
		fmt.Printf("Current page num values: %v\n", pageNumValues)

		body := make([]byte, header.CompressedPageSize)
		if _, err := io.ReadFull(transport, body); err != nil {
			return fmt.Errorf("unexpected eof when reading page body")
		}
		bodyReader := bytes.NewReader(body)

		repetitionLevels, err := decodeLevels(bodyReader, column.MaxRepetitionLevel, pageNumValues)
		if err != nil {
			return fmt.Errorf("decode repetition levels: %w", err)
		}
		fmt.Printf("Repetition levels: %v\n", repetitionLevels)

		definitionLevels, err := decodeLevels(bodyReader, column.MaxDefinitionLevel, pageNumValues)
		if err != nil {
			return fmt.Errorf("decode repetition levels: %w", err)
		}
		fmt.Printf("Definition levels: %v\n", definitionLevels)

		fmt.Printf("Collected values: %d\n", pageNumValues)
		fmt.Printf("Remaining bytes: %d\n", bodyReader.Len())

		collectedNumValues += int64(pageNumValues)

		// todo: read correctly according to the definition & repetition levels
		// simple guards
		for _, defLevel := range definitionLevels {
			if defLevel != uint32(column.MaxDefinitionLevel) {
				return fmt.Errorf("I can only read max definition levels now :(")
			}
		}
		if len(repetitionLevels) != 0 {
			return fmt.Errorf("Repetition levels not supported yet :(")
		}
		fmt.Printf("values: ")
		for range pageNumValues {
			value, err := parseNextValue(bodyReader, column.Type)
			if err != nil {
				return fmt.Errorf("read value: %w", err)
			}
			fmt.Printf("%v ", value)
		}
		fmt.Println()
	}
	return nil
}

func parseNextValue(r *bytes.Reader, physicalType PhysicalType) (any, error) {
	switch physicalType {
	case TypeInt32:
		var v int32
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case TypeInt64:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case TypeByteArray:
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	default:
		return nil, fmt.Errorf("Unknown physical type: %q", physicalType)
	}
}

func decodeLevels(r *bytes.Reader, maxLevel int, numValues int) ([]uint32, error) {
	if maxLevel == 0 {
		return make([]uint32, 0), nil
	}

	var levelsLength uint32
	if err := binary.Read(r, binary.LittleEndian, &levelsLength); err != nil {
		return nil, fmt.Errorf("read levels length: %w", err)
	}
	levelsEncoded := make([]byte, levelsLength)
	if _, err := io.ReadFull(r, levelsEncoded); err != nil {
		return nil, fmt.Errorf("read levels: %w", err)
	}
	bitWidth := int(math.Ceil(math.Log2(float64(maxLevel + 1))))
	return decodeRLEBitPacked(levelsEncoded, bitWidth, numValues)
}

func decodeRLEBitPacked(encodedData []byte, bitWidth int, numValues int) ([]uint32, error) {
	if bitWidth > 32 {
		return nil, fmt.Errorf("Bit width exceeds uint32: %d", bitWidth)
	}
	r := bytes.NewReader(encodedData)
	values := make([]uint32, 0, numValues)
	rleByteWidth := int(math.Ceil(float64(bitWidth) / 8))

	for len(values) < numValues {
		header, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read run header: %w", err)
		}
		if header&1 == 1 {
			// bit-packed-header := varint-encode(<bit-pack-scaled-run-len> << 1 | 1)
			scaledRunLength := int(header >> 1)
			// the number of values is (scaledRunLength * 8).
			runLength := scaledRunLength * 8
			// the body takes (scaledRunLength * 8 * bitWidth / 8) bytes
			runBody := make([]byte, scaledRunLength*bitWidth)
			if _, err := io.ReadFull(r, runBody); err != nil {
				return nil, fmt.Errorf("read bit-packed run body: %w", err)
			}
			decodedRun := unpack(runBody, bitWidth, runLength)
			values = append(values, decodedRun...)
		} else {
			// rle-header := varint-encode( (rle-run-len) << 1)
			runLength := int(header >> 1)
			repeatedValueBytes := make([]byte, rleByteWidth, 4)
			if _, err := io.ReadFull(r, repeatedValueBytes); err != nil {
				return nil, fmt.Errorf("read rle run repeated value: %w", err)
			}
			// pad the value to 4 bytes
			for range 4 - rleByteWidth {
				repeatedValueBytes = append(repeatedValueBytes, 0b0)
			}

			repeatedValue := binary.LittleEndian.Uint32(repeatedValueBytes)
			for range runLength {
				values = append(values, repeatedValue)
			}
		}
	}
	return values[:numValues], nil
}

// Unpacks bit-packed values.
//
// The values are packed from the least significant bit of each byte to the most significant bit.
func unpack(data []byte, bitWidth int, count int) []uint32 {
	out := make([]uint32, 0, count)

	for i := range count {
		value := uint32(0)

		for j := range bitWidth {
			bitOffset := i*bitWidth + j
			byteIndex := bitOffset / 8
			bitIndex := bitOffset % 8

			bit := (data[byteIndex] >> bitIndex) & 1
			value |= uint32(bit) << j
		}
		out = append(out, value)
	}
	return out
}
