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
