## Hardwood column reader

- https://hardwood.dev/1.0.0.CR2/how-to/column-reader/
- https://www.morling.dev/blog/improved-column-reader-api-geospatial-support-hardwood-1-0-0-cr1-available/

Example:

```java
try (ColumnReader fare = reader.columnReader("fare_amount")) {
      double sum = 0;

      while (fare.nextBatch()) {
          int count = fare.getRecordCount();
          double[] values = fare.getDoubles();
          Validity nulls = fare.getLeafValidity();

          for (int i = 0; i < count; i++) {
              if (nulls == Validity.NO_NULLS || !nulls.isNull(i)) {
                  sum += values[i];
              }
          }
      }
  }
```

### #430 feat(reader): ColumnReader layer-model rework

The ColumnReader columnar surface is reshaped around a uniform per-layer model:

    Each OPTIONAL group, LIST, and MAP along the column's schema chain contributes one layer; REQUIRED groups don't. STRUCT layers are exposed (closing the struct-null vs. field-null gap from feat(reader): distinguish struct-null from field-null on flat ColumnReader #436).
    Per-layer accessors: getLayerCount(), getLayerKind(int) (STRUCT or REPEATED), getLayerValidity(int) (set bit = present, sparse-null for all-present), getLayerOffsets(int) (sentinel-suffixed, REPEATED only).
    Leaf accessors: getLeafValidity() plus typed value arrays. Varlength leaves use getBinaryValues() + getBinaryOffsets() (capacity-sized bytes, sentinel-suffixed offsets); getBinaries() and getStrings() retained as convenience wrappers that allocate per row.
    Raw def-/rep-level escape hatch: getDefinitionLevels() / getRepetitionLevels() for the long tail.
    getElementNulls, getLevelNulls, getEmptyListMarkers, getNestingDepth removed; empty-list state is encoded structurally as offsets[i+1] - offsets[i] == 0.
    Validity polarity flips to set bit = present end-to-end (workers, internal storage, public API).
    Internal: NestedBatch / BatchExchange.Batch re-keyed onto layer-indexed validity; varlength values slot becomes (byte[], int[]). RowReader's public API is unchanged.

### #436 feat(reader): distinguish struct-null from field-null on flat ColumnReader

Problem

For a flat leaf inside an OPTIONAL non-repeated group ancestor, the leaf's definition levels encode three states:
def-level meaning
0 the ancestor group is null (struct null)
1 the ancestor group exists, this field is null (field null)
2 this field has a value

Hardwood's flat ColumnReader.getElementNulls() collapses every def < maxDefinitionLevel into a single null bit, so states {0, 1} merge. A consumer that needs to faithfully reconstruct user-visible shapes — "the struct is null" vs "all of the struct's fields happen to be individually null" — can't tell them apart.
