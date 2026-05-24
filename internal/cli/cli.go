package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gravity182/parq/parquet"
)

func Run(args []string) error {
	return RunWithIO(args, os.Stdout, os.Stderr)
}

func RunWithIO(args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		runUsage(stdout)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		runUsage(stdout)
		return nil
	case "version":
		return runVersion(args[1:], stdout)
	case "metadata":
		return runMetadata(args[1:], stdout)
	case "schema":
		return runSchema(args[1:], stdout)
	case "rows":
		return runRows(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runMetadata(args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: parq metadata <file.parquet>")
	}
	path := args[0]
	reader, err := parquet.OpenFile(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	meta, err := reader.Metadata()
	if err != nil {
		return err
	}
	printMetadata(stdout, meta)

	return nil
}

func printMetadata(w io.Writer, meta *parquet.Metadata) {
	if meta == nil {
		fmt.Fprintln(w, "metadata: <empty>")
		return
	}

	fmt.Fprintln(w, "metadata:")
	fmt.Fprintf(w, "  version: %d\n", meta.Version)
	fmt.Fprintf(w, "  rows: %d\n", meta.NumRows)
	fmt.Fprintf(w, "  row groups: %d\n", len(meta.RowGroups))
	fmt.Fprintf(w, "  columns: %d\n", schemaColumnCount(meta.Schema))
	printSchema(w, meta.Schema)
	printColumns(w, meta.Schema)
	printRowGroups(w, meta.RowGroups)
}

func schemaColumnCount(schema *parquet.Schema) int {
	if schema == nil {
		return 0
	}
	return len(schema.Columns)
}

func printColumns(w io.Writer, schema *parquet.Schema) {
	if schema == nil || len(schema.Columns) == 0 {
		fmt.Fprintln(w, "columns: <empty>")
		return
	}

	fmt.Fprintln(w, "columns:")
	for i, column := range schema.Columns {
		printColumnDescriptor(w, i, column)
	}
}

func printColumnDescriptor(w io.Writer, idx int, column parquet.ColumnDescriptor) {
	fmt.Fprintf(w, "- column %d\n", idx)
	fmt.Fprintf(w, "  path: %s\n", formatStringSlice(column.PathInSchema))
	fmt.Fprintf(w, "  type: %s\n", column.Type)
	fmt.Fprintf(w, "  max definition level: %d\n", column.MaxDefinitionLevel)
	fmt.Fprintf(w, "  max repetition level: %d\n", column.MaxRepetitionLevel)
}

func printRowGroups(w io.Writer, rowGroups []*parquet.RowGroup) {
	if len(rowGroups) == 0 {
		fmt.Fprintln(w, "row groups: <empty>")
		return
	}

	fmt.Fprintln(w, "row groups:")
	for i, rowGroup := range rowGroups {
		printRowGroup(w, i, rowGroup)
	}
}

func printRowGroup(w io.Writer, idx int, rowGroup *parquet.RowGroup) {
	fmt.Fprintf(w, "- row group %d\n", idx)
	if rowGroup == nil {
		fmt.Fprintln(w, "  <nil>")
		return
	}

	fmt.Fprintf(w, "  rows: %d\n", rowGroup.NumRows)
	fmt.Fprintf(w, "  total byte size: %d\n", rowGroup.TotalByteSize)
	fmt.Fprint(w, "  file offset: ")
	if rowGroup.FileOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *rowGroup.FileOffset)
	}
	fmt.Fprint(w, "  total compressed size: ")
	if rowGroup.TotalCompressedSize == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *rowGroup.TotalCompressedSize)
	}
	fmt.Fprint(w, "  ordinal: ")
	if rowGroup.Ordinal == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *rowGroup.Ordinal)
	}

	if len(rowGroup.ColumnChunks) == 0 {
		fmt.Fprintln(w, "  columns: <empty>")
		return
	}

	fmt.Fprintln(w, "  column chunks:")
	for i, column := range rowGroup.ColumnChunks {
		printColumnChunk(w, i, column)
	}
}

func printColumnChunk(w io.Writer, idx int, column *parquet.ColumnChunk) {
	fmt.Fprintf(w, "  - column chunk %d\n", idx)
	if column == nil {
		fmt.Fprintln(w, "    <nil>")
		return
	}

	fmt.Fprint(w, "    offset index offset: ")
	if column.OffsetIndexOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *column.OffsetIndexOffset)
	}
	fmt.Fprint(w, "    offset index length: ")
	if column.OffsetIndexLength == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *column.OffsetIndexLength)
	}
	fmt.Fprint(w, "    column index offset: ")
	if column.ColumnIndexOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *column.ColumnIndexOffset)
	}
	fmt.Fprint(w, "    column index length: ")
	if column.ColumnIndexLength == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *column.ColumnIndexLength)
	}
	printColumnMetaData(w, column.MetaData)
}

func printColumnMetaData(w io.Writer, meta *parquet.ColumnMetaData) {
	if meta == nil {
		fmt.Fprintln(w, "    metadata: <empty>")
		return
	}

	fmt.Fprintln(w, "    metadata:")
	fmt.Fprintf(w, "      path: %s\n", formatStringSlice(meta.PathInSchema))
	fmt.Fprintf(w, "      type: %s\n", meta.Type)
	fmt.Fprintf(w, "      codec: %s\n", meta.Codec)
	fmt.Fprintf(w, "      encodings: %s\n", formatEncodings(meta.Encodings))
	fmt.Fprintf(w, "      values: %d\n", meta.NumValues)
	fmt.Fprintf(w, "      total uncompressed size: %d\n", meta.TotalUncompressedSize)
	fmt.Fprintf(w, "      total compressed size: %d\n", meta.TotalCompressedSize)
	fmt.Fprintf(w, "      data page offset: %d\n", meta.DataPageOffset)
	fmt.Fprint(w, "      index page offset: ")
	if meta.IndexPageOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *meta.IndexPageOffset)
	}
	fmt.Fprint(w, "      dictionary page offset: ")
	if meta.DictionaryPageOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *meta.DictionaryPageOffset)
	}
	fmt.Fprint(w, "      bloom filter offset: ")
	if meta.BloomFilterOffset == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *meta.BloomFilterOffset)
	}
	fmt.Fprint(w, "      bloom filter length: ")
	if meta.BloomFilterLength == nil {
		fmt.Fprintln(w, "<none>")
	} else {
		fmt.Fprintln(w, *meta.BloomFilterLength)
	}
}

func runSchema(args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: parq schema <file.parquet>")
	}
	path := args[0]
	reader, err := parquet.OpenFile(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	schema, err := reader.Schema()
	if err != nil {
		return err
	}
	printSchema(stdout, schema)

	return nil
}

func printSchema(w io.Writer, schema *parquet.Schema) {
	if schema == nil || schema.Root == nil {
		fmt.Fprintln(w, "schema: <empty>")
		return
	}

	fmt.Fprintln(w, "schema:")
	printSchemaNode(w, schema.Root, 0)
}

func printSchemaNode(w io.Writer, node *parquet.SchemaNode, depth int) {
	indent := strings.Repeat("  ", depth)

	if len(node.Children) == 0 {
		fmt.Fprintf(w, "%s- %s", indent, node.Name)
		if node.RepetitionType != nil {
			fmt.Fprintf(w, " %s", *node.RepetitionType)
		}
		if node.Type != nil {
			fmt.Fprintf(w, " %s", *node.Type)
		}
		fmt.Fprintln(w)
		return
	}

	fmt.Fprintf(w, "%s- %s", indent, node.Name)
	if node.RepetitionType != nil {
		fmt.Fprintf(w, " %s", *node.RepetitionType)
	}
	fmt.Fprintln(w, " group")

	for _, child := range node.Children {
		printSchemaNode(w, child, depth+1)
	}
}

func formatStringSlice(values []string) string {
	if len(values) == 0 {
		return "<empty>"
	}
	return strings.Join(values, ".")
}

func formatEncodings(encodings []parquet.Encoding) string {
	if len(encodings) == 0 {
		return "<empty>"
	}

	parts := make([]string, 0, len(encodings))
	for _, encoding := range encodings {
		parts = append(parts, encoding.String())
	}
	return strings.Join(parts, ", ")
}

func runRows(args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: parq rows <file.parquet>")
	}
	path := args[0]
	reader, err := parquet.OpenFile(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	scanner, err := reader.Scanner()
	if err != nil {
		return fmt.Errorf("get scanner: %w", err)
	}
	if err := scanner.Rows(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	return nil
}

func runVersion(args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: parq version")
	}
	fmt.Fprintln(stdout, "parq dev")
	return nil
}

func runUsage(w io.Writer) {
	fmt.Fprintln(w, `parq is a fast Parquet parser written in Go.

Usage:
  parq <command>

Commands:
  metadata <file.parquet> Print full metadata of a given Parquet file
  schema <file.parquet>   Print schema of a given Parquet file
  rows <file.parquet>     Print decoded rows
  version       		  Print version
  help                    Print help

Examples:
  parq version`)
}
