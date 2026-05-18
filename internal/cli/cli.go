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
	fmt.Fprintf(w, "  row groups: %d\n", meta.NumRowGroups)

	printSchema(w, meta.Schema)
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
  version       		  Print version
  help                    Print help

Examples:
  parq version`)
}
