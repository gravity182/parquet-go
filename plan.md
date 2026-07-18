# Parquet parser

## Roadmap

### Phase 0

Metadata, schema parsing.

### Phase 1

Data parsing:

1. PLAIN encoding
2. uncompressed pages
3. required flat columns only

### Phase 2

Column reader API.

Repeated, optional columns, nested columns.

### Phase 3

Dictionary page.

Index page, but deprecated and rare.

### Phase 4

Support all compression codecs.

### Phase 5

Support all encodings.

### Phase 6

Filter out pages based on the column chunk-level statistics, or page-level column index + offset index.

---

## Datasets

- Flat data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Structs: https://docs.overturemaps.org/getting-data/
