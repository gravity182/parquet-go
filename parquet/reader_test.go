package parquet

import (
	"testing"

	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

func TestToInternalPhysicalType(t *testing.T) {
	tests := []struct {
		name string
		in   thriftgen.Type
		want PhysicalType
	}{
		{"boolean", thriftgen.Type_BOOLEAN, TypeBoolean},
		{"int32", thriftgen.Type_INT32, TypeInt32},
		{"int64", thriftgen.Type_INT64, TypeInt64},
		{"int96", thriftgen.Type_INT96, TypeInt96},
		{"float", thriftgen.Type_FLOAT, TypeFloat},
		{"double", thriftgen.Type_DOUBLE, TypeDouble},
		{"byte array", thriftgen.Type_BYTE_ARRAY, TypeByteArray},
		{"fixed len byte array", thriftgen.Type_FIXED_LEN_BYTE_ARRAY, TypeFixedLenByteArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInternalPhysicalType(tt.in)
			if err != nil {
				t.Fatalf("toInternalPhysicalType(%v) returned error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("toInternalPhysicalType(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInternalPhysicalTypeUnknown(t *testing.T) {
	_, err := toInternalPhysicalType(thriftgen.Type(999))
	if err == nil {
		t.Fatal("toInternalPhysicalType returned nil error for unknown type")
	}
}

func TestToInternalRepetitionType(t *testing.T) {
	tests := []struct {
		name string
		in   thriftgen.FieldRepetitionType
		want RepetitionType
	}{
		{"required", thriftgen.FieldRepetitionType_REQUIRED, RepetitionTypeRequired},
		{"optional", thriftgen.FieldRepetitionType_OPTIONAL, RepetitionTypeOptional},
		{"repeated", thriftgen.FieldRepetitionType_REPEATED, RepetitionTypeRepeated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInternalRepetitionType(tt.in)
			if err != nil {
				t.Fatalf("toInternalRepetitionType(%v) returned error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("toInternalRepetitionType(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInternalRepetitionTypeUnknown(t *testing.T) {
	_, err := toInternalRepetitionType(thriftgen.FieldRepetitionType(999))
	if err == nil {
		t.Fatal("toInternalRepetitionType returned nil error for unknown repetition type")
	}
}

func TestPhysicalTypeString(t *testing.T) {
	tests := []struct {
		name string
		in   PhysicalType
		want string
	}{
		{"boolean", TypeBoolean, "BOOLEAN"},
		{"int32", TypeInt32, "INT32"},
		{"int64", TypeInt64, "INT64"},
		{"int96", TypeInt96, "INT96"},
		{"float", TypeFloat, "FLOAT"},
		{"double", TypeDouble, "DOUBLE"},
		{"byte array", TypeByteArray, "BYTE_ARRAY"},
		{"fixed len byte array", TypeFixedLenByteArray, "FIXED_LEN_BYTE_ARRAY"},
		{"unknown", PhysicalType(999), "PhysicalType(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRepetitionTypeString(t *testing.T) {
	tests := []struct {
		name string
		in   RepetitionType
		want string
	}{
		{"required", RepetitionTypeRequired, "REQUIRED"},
		{"optional", RepetitionTypeOptional, "OPTIONAL"},
		{"repeated", RepetitionTypeRepeated, "REPEATED"},
		{"unknown", RepetitionType(999), "RepetitionType(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestToInternalEncoding(t *testing.T) {
	tests := []struct {
		name string
		in   thriftgen.Encoding
		want Encoding
	}{
		{"plain", thriftgen.Encoding_PLAIN, EncodingPlain},
		{"plain dictionary", thriftgen.Encoding_PLAIN_DICTIONARY, EncodingPlainDictionary},
		{"rle", thriftgen.Encoding_RLE, EncodingRLE},
		{"bit packed", thriftgen.Encoding_BIT_PACKED, EncodingBitPacked},
		{"delta binary packed", thriftgen.Encoding_DELTA_BINARY_PACKED, EncodingDeltaBinaryPacked},
		{"delta length byte array", thriftgen.Encoding_DELTA_LENGTH_BYTE_ARRAY, EncodingDeltaLengthByteArray},
		{"delta byte array", thriftgen.Encoding_DELTA_BYTE_ARRAY, EncodingDeltaByteArray},
		{"rle dictionary", thriftgen.Encoding_RLE_DICTIONARY, EncodingRLEDictionary},
		{"byte stream split", thriftgen.Encoding_BYTE_STREAM_SPLIT, EncodingByteStreamSplit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInternalEncoding(tt.in)
			if err != nil {
				t.Fatalf("toInternalEncoding(%v) returned error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("toInternalEncoding(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInternalEncodingDoesNotRelyOnThriftOrdinal(t *testing.T) {
	got, err := toInternalEncoding(thriftgen.Encoding_PLAIN_DICTIONARY)
	if err != nil {
		t.Fatalf("toInternalEncoding(%v) returned error: %v", thriftgen.Encoding_PLAIN_DICTIONARY, err)
	}
	if got != EncodingPlainDictionary {
		t.Fatalf("toInternalEncoding(%v) = %v, want %v", thriftgen.Encoding_PLAIN_DICTIONARY, got, EncodingPlainDictionary)
	}
	if thriftgen.Encoding_PLAIN_DICTIONARY == thriftgen.Encoding(EncodingPlainDictionary) {
		t.Fatal("test setup expected internal EncodingPlainDictionary to differ from thrift ordinal")
	}
}

func TestToInternalEncodingUnknown(t *testing.T) {
	_, err := toInternalEncoding(thriftgen.Encoding(999))
	if err == nil {
		t.Fatal("toInternalEncoding returned nil error for unknown encoding")
	}
}

func TestToInternalEncodings(t *testing.T) {
	got, err := toInternalEncodings([]thriftgen.Encoding{
		thriftgen.Encoding_PLAIN,
		thriftgen.Encoding_RLE,
		thriftgen.Encoding_BYTE_STREAM_SPLIT,
	})
	if err != nil {
		t.Fatalf("toInternalEncodings returned error: %v", err)
	}

	want := []Encoding{EncodingPlain, EncodingRLE, EncodingByteStreamSplit}
	if len(got) != len(want) {
		t.Fatalf("len(toInternalEncodings) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("toInternalEncodings[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestToInternalCompressionCodec(t *testing.T) {
	tests := []struct {
		name string
		in   thriftgen.CompressionCodec
		want CompressionCodec
	}{
		{"uncompressed", thriftgen.CompressionCodec_UNCOMPRESSED, CompressionCodecUncompressed},
		{"snappy", thriftgen.CompressionCodec_SNAPPY, CompressionCodecSnappy},
		{"gzip", thriftgen.CompressionCodec_GZIP, CompressionCodecGzip},
		{"lzo", thriftgen.CompressionCodec_LZO, CompressionCodecLzo},
		{"brotli", thriftgen.CompressionCodec_BROTLI, CompressionCodecBrotli},
		{"lz4", thriftgen.CompressionCodec_LZ4, CompressionCodecLz4},
		{"zstd", thriftgen.CompressionCodec_ZSTD, CompressionCodecZstd},
		{"lz4 raw", thriftgen.CompressionCodec_LZ4_RAW, CompressionCodecLz4Raw},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInternalCompressionCodec(tt.in)
			if err != nil {
				t.Fatalf("toInternalCompressionCodec(%v) returned error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("toInternalCompressionCodec(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInternalCompressionCodecUnknown(t *testing.T) {
	_, err := toInternalCompressionCodec(thriftgen.CompressionCodec(999))
	if err == nil {
		t.Fatal("toInternalCompressionCodec returned nil error for unknown compression codec")
	}
}

func TestEncodingString(t *testing.T) {
	tests := []struct {
		name string
		in   Encoding
		want string
	}{
		{"plain", EncodingPlain, "PLAIN"},
		{"plain dictionary", EncodingPlainDictionary, "PLAIN_DICTIONARY"},
		{"rle", EncodingRLE, "RLE"},
		{"bit packed", EncodingBitPacked, "BIT_PACKED"},
		{"delta binary packed", EncodingDeltaBinaryPacked, "DELTA_BINARY_PACKED"},
		{"delta length byte array", EncodingDeltaLengthByteArray, "DELTA_LENGTH_BYTE_ARRAY"},
		{"delta byte array", EncodingDeltaByteArray, "DELTA_BYTE_ARRAY"},
		{"rle dictionary", EncodingRLEDictionary, "RLE_DICTIONARY"},
		{"byte stream split", EncodingByteStreamSplit, "BYTE_STREAM_SPLIT"},
		{"unknown", Encoding(999), "Encoding(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompressionCodecString(t *testing.T) {
	tests := []struct {
		name string
		in   CompressionCodec
		want string
	}{
		{"uncompressed", CompressionCodecUncompressed, "UNCOMPRESSED"},
		{"snappy", CompressionCodecSnappy, "SNAPPY"},
		{"gzip", CompressionCodecGzip, "GZIP"},
		{"lzo", CompressionCodecLzo, "LZO"},
		{"brotli", CompressionCodecBrotli, "BROTLI"},
		{"lz4", CompressionCodecLz4, "LZ4"},
		{"zstd", CompressionCodecZstd, "ZSTD"},
		{"lz4 raw", CompressionCodecLz4Raw, "LZ4_RAW"},
		{"unknown", CompressionCodec(999), "CompressionCodec(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
