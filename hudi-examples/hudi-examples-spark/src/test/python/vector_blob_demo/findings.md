# Findings — Hudi 1.2.0 VECTOR + BLOB + Lance demo issues

Tracking UX papercuts and latent bugs surfaced while building the
[`vector_blob_demo/`](.) scripts. Each section is a candidate patch to flag
with the Hudi team — none block the demo today (workarounds are in the
scripts) but each would make the real surface cleaner.

---

## 1. BLOB write UX gap (SQL + DataFrame)

## TL;DR

Writing a Hudi **BLOB INLINE** value today forces users to hand-construct all
three fields of the BLOB struct (`type`, `data`, `reference`) even though the
Avro schema declares `data` and `reference` as nullable with
`NULL_DEFAULT_VALUE`. The "set what you care about, leave the other branch
null" ergonomic is what the schema intends, but the write paths enforce
strict 3-field arity.

This shows up in **both** the SQL and DataFrame demos in
[`vector_blob_demo/`](.) and is a real UX papercut for end users.

## What users have to write today

### SQL — `hudi_sql_vector_blob_demo.py`

```sql
INSERT INTO pets_sql_lance
SELECT
  ...,
  named_struct(
      'type',      'INLINE',
      'data',      image_bytes_raw,
      'reference', cast(null as struct<external_path:string,
                                       offset:bigint,
                                       length:bigint,
                                       managed:boolean>)  -- mandatory noise
  ) AS image_bytes,
  ...
FROM staging_pets
```

### DataFrame — `hudi_lance_vector_blob_demo.py`

```python
def inline_blob_struct(bytes_col):
    return struct(
        lit("INLINE").alias("type"),
        bytes_col.cast("binary").alias("data"),
        lit(None).cast(BLOB_REFERENCE_CAST).alias("reference"),  # mandatory noise
    )
```

Symmetric for OUT_OF_LINE — users have to pass `data = cast(null as binary)`
even though `data` is the INLINE branch.

## What users should be able to write

### SQL

```sql
INSERT INTO t VALUES
  (1, named_struct('type', 'INLINE', 'data', cast(X'010203' as binary)));

INSERT INTO t VALUES
  (1, named_struct(
        'type', 'OUT_OF_LINE',
        'reference', named_struct('external_path','s3://...',
                                  'offset', 0,
                                  'length', 1024,
                                  'managed', false)));
```

### DataFrame

```python
# INLINE — one field, no null cast
struct(lit("INLINE").alias("type"), bytes_col.cast("binary").alias("data"))

# OUT_OF_LINE — two fields
struct(
    lit("OUT_OF_LINE").alias("type"),
    struct(...).alias("reference"),
)
```

## Why it fails today

### SQL path

- Enforcement point:
  [`TableOutputResolver.resolveColumnsByPosition`](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TableOutputResolver.scala)
  (Spark Catalyst, built-in), invoked from
  [`InsertIntoHoodieTableCommand.coerceQueryOutputColumns`](../../../../../../hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/InsertIntoHoodieTableCommand.scala)
  via the Hudi rule [`ResolveImplementationsEarly`](../../../../../../hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala).
- Error:
  `[INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS] ... Struct 'payload' missing fields: 'reference'`
- Verified empirically 2026-04-24 by dropping the `reference` line from
  [`TestCreateTable.scala:2390-2397`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/ddl/TestCreateTable.scala:2390)
  and running the test — fails with exactly that error.

### DataFrame path

- Enforcement point:
  [`HoodieSchema.java:2787`](../../../../../../hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchema.java:2787)
  — `BlobLogicalType.validate` rejects the Avro schema if
  `schema.getFields()` doesn't equal the canonical 3-field `Blob.BLOB_FIELDS`
  list.
- Error: `IllegalArgumentException: Blob logical type cannot be applied to schema: ...`

### Root cause

- The Avro BLOB record ([`HoodieSchema.java:2880-2896`](../../../../../../hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchema.java:2880))
  declares `data` and `reference` both as `createNullableSchema(...)` with
  `Schema.Field.NULL_DEFAULT_VALUE`. At the Avro level, a null value for
  either is entirely valid — this is the storage intent.
- Spark's `CreateNamedStruct` produces a struct whose **type** is derived
  from the arguments passed (only the fields you name). So
  `named_struct('type','INLINE','data', X)` has type
  `struct<type:string, data:binary>` — 2 fields.
- Neither Spark nor Hudi auto-pads the source struct to match the target.
  Spark does strict positional match; Hudi validates exact field equality.

Avro-level nullability + write-path strict arity = UX gap.

## Proposed fix

A new Hudi Spark analysis rule that runs **before**
`TableOutputResolver.resolveColumnsByPosition` in the SQL path and **before**
Avro schema extraction in the DataFrame path. For each column targeting a
BLOB type:

1. If the source is a `CreateNamedStruct` (SQL) or a struct expression (DF)
   with fewer than 3 fields, pad it by wrapping in a `CreateNamedStruct` that
   fills the missing subfields with typed null literals:
   - Missing `data` → `Literal(null, BinaryType)`
   - Missing `reference` → `Literal(null, <4-field reference struct type>)`
2. Also pad missing inner fields in the `reference` struct if users provide
   a partial reference (e.g. `external_path` only, with null `offset`,
   `length`, `managed`).

### Implementation sketch

- New file: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/ResolveBlobStructPadding.scala`
- Hook it into [`HoodieAnalysis.scala:389-408`](../../../../../../hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala:389)
  in the `extendedResolutionRules` chain, ordered **before**
  `ResolveImplementationsEarly` (so Spark's resolver sees the padded plan).
- Symmetric DataFrame-side: during Spark-struct → Avro-schema conversion in
  `AvroConversionUtils` (or whichever Hudi helper converts), detect BLOB
  target columns (via target-table schema) and pad the source struct to
  match.

### Tests to add

- SQL: cut down
  [`TestCreateTable.scala`'s two BLOB INSERT tests](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/ddl/TestCreateTable.scala:2368)
  to the proposed shorter form once the rule lands.
- DataFrame: add a variant to `TestBlobSupport.scala` that writes a 2-field
  `struct("INLINE", bytes)` and asserts the write succeeds and round-trips.
- Symmetric OUT_OF_LINE coverage.

## Acceptance criteria

Demo scripts in this folder should drop the null-branch boilerplate:

- SQL demo: delete the `'reference', cast(null as struct<...>)` line
- DataFrame demo: delete the `lit(None).cast(BLOB_REFERENCE_CAST).alias(BLOB_FIELD_REFERENCE)` line
  and the `BLOB_REFERENCE_CAST` constant

Both still produce identical on-disk output. Both still read back identically
(CONTENT and DESCRIPTOR modes unaffected — this is purely a write ergonomics
change).

## Non-goals

- No change to the on-disk BLOB format.
- No change to the Avro schema shape.
- No change to read-side behavior (CONTENT / DESCRIPTOR modes).
- Not introducing a new UDF (`hudi_inline_blob(...)`) — the padding rule
  makes plain `named_struct(...)` work, which is the more idiomatic SQL.

## Related code paths (for whoever picks this up)

- [`HoodieSchema.java` — BLOB_FIELDS, createBlobFields, BlobLogicalType.validate](../../../../../../hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchema.java)
- [`HoodieAnalysis.scala` — analysis rule registration](../../../../../../hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala)
- [`InsertIntoHoodieTableCommand.scala` — alignQueryOutput / coerceQueryOutputColumns](../../../../../../hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/InsertIntoHoodieTableCommand.scala)
- [`BlobTestHelpers.scala` — canonical DataFrame-side BLOB struct builder](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/blob/BlobTestHelpers.scala)
- [`TestCreateTable.scala:2368-2427` — current BLOB INSERT tests](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/ddl/TestCreateTable.scala:2368)

---

## 2. `LanceRecordIterator` throws on empty projection (COUNT(*), EXISTS, etc.)

### TL;DR

`SELECT COUNT(*) FROM <lance-backed hudi table>` fails with:

```
Lance batch column count 14 does not match expected Spark schema size 0
  for file: .../category=Abyssinian/....lance
  at org.apache.hudi.io.storage.LanceRecordIterator.hasNext(LanceRecordIterator.java:124)
```

Any query shape that triggers Spark's "no columns needed, just count rows"
optimization (COUNT(*), EXISTS, `CREATE TABLE AS SELECT 1 FROM ...`) blows
up on a Lance-backed Hudi table. Parquet-backed tables work fine.

### Why it happens

[`LanceRecordIterator.java:122-127`](../../../../../../hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/io/storage/LanceRecordIterator.java:122)
has a strict equality check when building `ColumnVector[]`:

```java
StructField[] sparkFields = sparkSchema.fields();
if (sparkFields.length != fieldVectors.size()) {
  throw new HoodieException("Lance batch column count " + fieldVectors.size()
      + " does not match expected Spark schema size " + sparkFields.length + ...);
}
```

When Spark's optimizer prunes all columns for an aggregate-only read (COUNT,
EXISTS), the request arrives with `sparkSchema.fields().length == 0`, but
the Lance file's batch always has the full column set. The reader sees
`0 != 14` and throws.

The Parquet reader handles this naturally — `ParquetFileFormat` has a
zero-column fast path where it just yields N empty rows (where N is the
row count) so the aggregate can count them without reading any data. Lance
needs the equivalent.

### Workaround in the demo

Use `COUNT(<named_col>)` instead of `COUNT(*)`. On a non-null primary key
the two are semantically equivalent, but the former forces Spark to
request one column, satisfying the check.

[`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) uses
`COUNT(image_id)` with an inline comment explaining why.

### Proposed fix

In `LanceRecordIterator.hasNext()`:
- If `sparkSchema.fields().length == 0`, skip the `ColumnVector[]` build entirely.
- Still call `arrowReader.loadNextBatch()` to advance, and yield empty rows
  matching the Arrow `VectorSchemaRoot.getRowCount()` so downstream count
  aggregators work.
- Add a test in
  [`TestLanceDataSource.scala`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/functional/TestLanceDataSource.scala)
  exercising `spark.sql("SELECT COUNT(*) FROM …")` over a Lance-backed table
  and `df.count()` on the same.

### Related code paths

- [`LanceRecordIterator.java`](../../../../../../hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/io/storage/LanceRecordIterator.java)
- [`HoodieSparkLanceReader.java`](../../../../../../hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/io/storage/HoodieSparkLanceReader.java)
- [`TestLanceDataSource.scala`](../../../../../../hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/hudi/functional/TestLanceDataSource.scala)
