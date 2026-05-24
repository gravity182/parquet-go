# Parquet parser

## Phase 0

Metadata, schema parsing.

## Phase 1

Data parsing:

1. PLAIN encoding
2. uncompressed pages
3. required flat columns only

## Phase 2

Repeated, optional columns, nested columns.

## Phase 3

Dictionary page.

Index page, but deprecated and rare.

## Phase 4

Compression.

## Phase 5

Encodings

## Filters

Filter out pages based on the column chunk-level statistics, or page-level column index + offset index.
