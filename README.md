# Parquet-go

A fast Parquet parser written in Golang.

## Quick start

Build:

```sh
go build ./cmd/parq
```

Get metadata:

```sh
$ ./parq metadata users.parquet
metadata:
  version: 1
  rows: 3
  row groups: 1
schema:
- duckdb_schema REQUIRED group
  - id OPTIONAL INT32
  - name OPTIONAL BYTE_ARRAY
```

Get schema only:

```sh
$ ./parq schema users.parquet
schema:
- duckdb_schema REQUIRED group
  - id OPTIONAL INT32
  - name OPTIONAL BYTE_ARRAY
```
