ColumnReader API:

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
