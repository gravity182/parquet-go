package parquet

import (
	"slices"
	"testing"

	"github.com/gravity182/parq/parquet/internal/thrift/thriftgen"
)

func TestValidity(t *testing.T) {
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
