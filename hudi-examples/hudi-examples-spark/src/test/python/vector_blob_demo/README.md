# Hudi VECTOR + BLOB + Vector Search demo (PySpark + Lance)

End-to-end PySpark demo that exercises three Hudi 1.2.0 features together on
the Oxford-IIIT Pet dataset:

1. **VECTOR type** — embedding column is annotated with
   `hudi_type = "VECTOR(<dim>)"`.
2. **BLOB type (INLINE)** — image bytes are written as a Hudi BLOB struct
   tagged with `hudi_type = "BLOB"`.
3. **Vector search** — cosine similarity top-K via the
   `hudi_vector_search` SQL table-valued function, backed by Lance files.

## Two variants

The folder ships two scripts that produce the **same output** but expose the
Hudi surface differently. Pick whichever fits the audience:

| File | Hudi surface | Best for |
|---|---|---|
| [`hudi_lance_vector_blob_demo.py`](hudi_lance_vector_blob_demo.py) | DataFrame API — `StructField(..., metadata={"hudi_type": "VECTOR(N)"})`, `stamp_blob_metadata`, `struct(lit(...), ...)` | Library-style integration; seeing how the Python DataFrame API composes the VECTOR/BLOB logical types under the hood |
| [`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) | Spark SQL — `CREATE TABLE ... (embedding VECTOR(1024), image_bytes BLOB, ...) USING hudi`, `INSERT INTO ... SELECT ... named_struct('type','INLINE', ...)`, `SELECT ... FROM hudi_vector_search(...)` | Live demos; SQL-first users; showing the Hudi 1.2.0 DDL/DML surface the way it's documented |

Both share the same venv, jars, env vars, and output panel style. They write
to different table paths (`/tmp/hudi_{format}_pets` vs
`/tmp/hudi_sql_{format}_pets`) so you can run them back-to-back without
collision.

## Prereqs

- Java 11
- Python **3.12** (PySpark 3.5 does NOT support Python 3.13/3.14)
- Hudi Spark bundle built from this branch
- Lance Spark bundle jar

## 1. Build the Hudi bundle

From the repo root:

```bash
mvn clean package -pl packaging/hudi-spark-bundle -am -DskipTests -Dspark3.5
```

Produces `packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar`.
The demo's `HUDI_BUNDLE_JAR` env var defaults to exactly this path, so you
don't need to export it unless your jar lives elsewhere.

## 2. Grab the Lance bundle

Hudi's pom pins `org.lance:lance-spark-3.5_2.12:0.4.0` (see
`lance.spark.connector.version` in the root `pom.xml`). Download the matching
**bundle** jar:

- Artifact: `org.lance:lance-spark-bundle-3.5_2.12:0.4.0`
- Maven Central: <https://central.sonatype.com/artifact/org.lance/lance-spark-bundle-3.5_2.12/0.4.0>
- File name: `lance-spark-bundle-3.5_2.12-0.4.0.jar`

## 3. Create a Python 3.12 venv + install deps

```bash
cd hudi-examples/hudi-examples-spark/src/test/python/vector_blob_demo

# Homebrew: brew install python@3.12 if you don't have it yet
python3.12 -m venv .venv
source .venv/bin/activate

python --version     # sanity check: must be 3.12.x, not 3.13+
pip install --upgrade pip
pip install -r requirements.txt
```

`torch` + `torchvision` is the heaviest install (~800 MB); first run takes a
few minutes.

## 4. Run

```bash
# Point at the Lance bundle you downloaded
export LANCE_BUNDLE_JAR=~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar

# Start small to verify correctness — 100 images runs in under a minute
export HUDI_LANCE_DEMO_N=100

# DataFrame variant
python hudi_lance_vector_blob_demo.py

# ...or the SQL variant (same deps, different surface)
python hudi_sql_vector_blob_demo.py
```

Once it works, crank it up:

```bash
export HUDI_LANCE_DEMO_N=1000
python hudi_lance_vector_blob_demo.py        # or hudi_sql_vector_blob_demo.py
```

### Run the same demo against Parquet base files

The Hudi VECTOR + BLOB + vector search path is format-agnostic — flip the
base file format with one env var:

```bash
export HUDI_BASE_FILE_FORMAT=parquet
python hudi_lance_vector_blob_demo.py        # or hudi_sql_vector_blob_demo.py
```

Table path and panel filename auto-rename to `/tmp/hudi_parquet_pets` (or
`/tmp/hudi_sql_parquet_pets` for the SQL script) and the corresponding
`outputs/hudi_{...}_parquet_results.png` so you can diff runs side by side.
`LANCE_BUNDLE_JAR` is still required on the classpath (the Hudi spark bundle
has compile-time references to Lance classes) — nothing actually writes Lance
files in this mode.

### Open the result panel

```bash
open /Users/rahil/workplace/hudi/hudi-examples/hudi-examples-spark/src/test/python/vector_blob_demo/outputs/hudi_lance_results.png
```

An ideal run shows the query image on the left and the top-5 nearest
neighbors (by cosine similarity on the image embedding) to its right — for a
Sphynx query you should see other short-haired cats (Siamese, Russian Blue,
etc.) with similarity scores in the 0.3–0.5 range at N=100, tighter at N=1000.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `HUDI_BUNDLE_JAR` | `<repo>/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar` | Hudi spark bundle |
| `LANCE_BUNDLE_JAR` | **required even for Parquet runs** | Lance spark bundle (shipped on classpath regardless of base file format) |
| `HUDI_BASE_FILE_FORMAT` | `lance` | Set to `parquet` to write Parquet base files instead |
| `HUDI_LANCE_DEMO_N` | `1000` | Number of images to sample |
| `PYSPARK_DRIVER_MEMORY` | `4g` | Driver JVM heap — bump to `8g`+ for N≥2000 |
| `HUDI_LANCE_DEMO_OUTDIR` | `./outputs` | Where query/top-K PNGs land |

## What the run produces

- Hudi table at `/tmp/hudi_lance_pets`
  - `.hoodie/` with one commit on the timeline
  - `<breed>/*.lance` files per partition (37 breed categories)
- `./outputs/query.png`, `./outputs/top1.png` … `top5.png`
- `./outputs/hudi_lance_results.png` — combined panel (the one to open)

## Verifying the logical-type tags landed

```python
spark.read.format("hudi").load("/tmp/hudi_lance_pets").schema.json()
```

Look for:
- `embedding`   → metadata `{"hudi_type": "VECTOR(1024)"}` (dim depends on the backbone)
- `image_bytes` → metadata `{"hudi_type": "BLOB"}`, struct fields `type`, `data`, `reference`

## Switching BLOB read mode

Default is `hoodie.read.blob.inline.mode=CONTENT` (returns inline bytes as
written). To exercise the descriptor path added in commit `7aea0f72d8e7`,
change the `.config(...)` line in `create_spark()` to `DESCRIPTOR` — results
will come back as `OUT_OF_LINE`-shaped references you then resolve via
`read_blob()`.

## How the script works

The script is a single file —
[`hudi_lance_vector_blob_demo.py`](hudi_lance_vector_blob_demo.py) — organized
into numbered sections. Here's what each one does and why.

### Pre-JVM env setup (top of file, before any `pyspark` import)

```python
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault("PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)
```

In `local[*]` mode the driver JVM IS the executor. Driver heap is set **when
the JVM launches**, not via `SparkSession.config()` later — so
`PYSPARK_SUBMIT_ARGS` must be in `os.environ` before `import pyspark` triggers
JVM launch. 4 GB handles ~1000 rows with room to spare; bump to 8g for larger.

### `CONFIG` + Hudi schema constants

Knobs grouped at the top. The constants under `HUDI_TYPE_METADATA_KEY` and
`BLOB_*` mirror values from `HoodieSchema.java` in `hudi-common`:

| Python constant | Hudi source |
|---|---|
| `HUDI_TYPE_METADATA_KEY = "hudi_type"` | `HoodieSchema.TYPE_METADATA_FIELD` |
| `BLOB_TYPE_INLINE = "INLINE"` | `HoodieSchema.Blob.INLINE` |
| `BLOB_FIELD_TYPE/DATA/REFERENCE` | `HoodieSchema.Blob.TYPE/INLINE_DATA_FIELD/EXTERNAL_REFERENCE` |

If those change in Hudi, update these four constants.

### Section 1 — `create_spark()`

Every Spark config line has a purpose:

| Config | Why |
|---|---|
| `spark.jars` | Ships the Hudi + Lance bundles to the classpath |
| `spark.serializer = KryoSerializer` | Required by Hudi — bombs with default Java serializer |
| `spark.sql.extensions = HoodieSparkSessionExtension` | Registers Hudi's SQL rules, including the vector search TVF |
| `spark.sql.catalog.spark_catalog = HoodieCatalog` | Makes `CREATE TABLE` aware of Hudi |
| `spark.sql.session.timeZone = UTC` | Determinism |
| `hoodie.read.blob.inline.mode = CONTENT` | Explicit default — flip to `DESCRIPTOR` to exercise the new descriptor read path |
| `spark.default.parallelism = 2`, `spark.sql.shuffle.partitions = 2` | On macOS, too many Python workers streaming BLOB bytes saturates localhost socket buffers; 2 workers is plenty for a demo |

### Section 2 — `load_dataset()`

Pulls N random images from torchvision's Oxford-IIIT Pet (37 dog/cat breeds).
Each row becomes a Python dict with the PNG bytes in a *staging* column called
`image_bytes_raw`. The BLOB struct shape is applied later — simpler to write
the dict with a flat binary field than to hand-build nested dicts.

### Section 3 — Embedding model

Uses `timm.create_model("mobilenetv3_small_100", pretrained=True, num_classes=0)`.
`num_classes=0` strips the classifier head so `model(x)` returns feature
vectors, not predictions. `sklearn.preprocessing.normalize` L2-normalizes the
embeddings so cosine distance = `1 - dot_product`. Returns
`(data, embedding_dim)` — the dim (1024 for this backbone) is data-driven and
becomes the `N` in `VECTOR(N)`.

### Section 4 — Writing to Hudi

Three functions work together:

**`build_staging_schema(embedding_dim)`** builds the `StructType`.  The
`embedding` field carries `metadata={"hudi_type": f"VECTOR({N})"}` directly —
`StructField.metadata` survives `createDataFrame` untouched, so this is how
the VECTOR tag reaches the write path.

**`inline_blob_struct(bytes_col)`** constructs a `struct<type, data, reference>`
column expression. Mirrors `BlobTestHelpers.inlineBlobStructCol` in the Scala
test code. The `reference` field is a null cast to the full
`struct<external_path, offset, length, managed>` shape so Spark's schema
inference doesn't produce a `struct<>` mismatch.

**`stamp_blob_metadata(df, "image_bytes")`** re-selects every column and
stamps `hudi_type=BLOB` on the target via `col(name).alias(name, metadata={...})`.
PySpark gives you no other way to attach `Metadata` to an existing struct
column.

**`write_to_hudi(spark, data, embedding_dim)`** ties them together:
1. `createDataFrame(data, schema)` — flat `image_bytes_raw`, VECTOR-tagged `embedding`
2. `withColumn("image_bytes", inline_blob_struct(...))` + `drop("image_bytes_raw")`
3. `stamp_blob_metadata(..., "image_bytes")`
4. `.write.format("hudi")` with these options (the interesting ones):
   - `hoodie.table.base.file.format = lance` — the switch from Parquet to Lance
   - `hoodie.datasource.write.partitionpath.field = category_sanitized` — 37 breed partitions
   - `hoodie.write.record.merge.custom.implementation.classes = DefaultSparkRecordMerger` — required to merge Spark rows with Hudi's logical types

### Section 5 — `find_similar()`

Builds a literal SQL string: `ARRAY(f1, f2, ...)` (1024 floats inlined into
the query). The `hudi_vector_search` TVF requires the query vector to be a
constant expression, so a scalar subquery won't work — the literal is the
documented pattern, matching `TestHoodieVectorSearchFunction.scala`.

Calls:
```sql
SELECT image_id, category, image_bytes, _hudi_distance
FROM hudi_vector_search('<path>', 'embedding', ARRAY(...), k+1, 'cosine')
ORDER BY _hudi_distance
```

Notes:
- Asks for `top_k + 1` because the query image is itself in the corpus
  (distance ≈ 0) and gets skipped in the result loop.
- Reads the image bytes out of the struct with `row["image_bytes"]["data"]`
  — in CONTENT mode the BLOB comes back as the full struct, and the inline
  bytes live in the `data` field.

### Section 6 — `visualize_and_save()`

Pure matplotlib. Saves `query.png`, `top1.png` … `top5.png`, and a combined
panel at `hudi_lance_results.png`. Uses the `Agg` backend so it runs headless.

### `main()`

Linear flow — no branches:

```
create_spark()
  → load_dataset()
  → create_embedding_model() + generate_embeddings()
  → write_to_hudi()
  → (pick random row as query)
  → find_similar()
  → visualize_and_save()
  → spark.stop()
```

## How the SQL script works

[`hudi_sql_vector_blob_demo.py`](hudi_sql_vector_blob_demo.py) reaches the
same end state — a partitioned Hudi table with a VECTOR embedding column, a
BLOB image column, and a vector similarity query — but every Hudi-touching
line is a SQL string rather than a DataFrame transform. The Python↔SQL
bridge is a Spark **temp view**.

### Steps 1–3 — identical to the DataFrame variant

Pre-JVM env setup, `create_spark()`, dataset loading, and embedding
generation are copied verbatim. The interesting divergence starts at step 4.

### Step 4 — register the Python data as a Spark temp view

```python
spark.createDataFrame(data, schema=staging_schema) \
     .createOrReplaceTempView("staging_pets")
```

No Hudi metadata is attached here — `image_bytes_raw` stays plain
`BinaryType`, `embedding` stays plain `ArrayType(FloatType)`. The Hudi
logical types come from the **target table's DDL**, not the source.

### Step 5 — `CREATE TABLE ... USING hudi` (SQL)

```sql
CREATE TABLE pets_sql_lance (
    image_id            STRING,
    category            STRING,
    category_sanitized  STRING,
    label               INT,
    description         STRING,
    image_bytes         BLOB           COMMENT 'Pet image bytes (INLINE)',
    width               INT,
    height              INT,
    embedding           VECTOR(1024)   COMMENT 'Image embedding for ANN search'
) USING hudi
PARTITIONED BY (category_sanitized)
LOCATION '/tmp/hudi_sql_lance_pets'
TBLPROPERTIES (
    primaryKey = 'image_id',
    preCombineField = 'image_id',
    type = 'cow',
    'hoodie.table.base.file.format' = 'lance'
)
```

`VECTOR(1024)` and `BLOB` are first-class Hudi-extended SQL types. The
parser at
[`HoodieSpark3_5ExtendedSqlAstBuilder.scala:2612-2626`](../../../../../../hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/parser/HoodieSpark3_5ExtendedSqlAstBuilder.scala)
rewrites them into the right Spark types **and** stamps `hudi_type` metadata
automatically — so the DataFrame demo's `stamp_blob_metadata()` /
`StructField(metadata=...)` gymnastics simply aren't needed here.

### Step 6 — `INSERT INTO ... SELECT` with `named_struct` (SQL)

```sql
INSERT INTO pets_sql_lance
SELECT
    image_id, category, category_sanitized, label, description,
    named_struct(
        'type',      'INLINE',
        'data',      image_bytes_raw,
        'reference', cast(null as struct<external_path:string,
                                         offset:bigint,
                                         length:bigint,
                                         managed:boolean>)
    ) AS image_bytes,
    width, height,
    embedding
FROM staging_pets
```

`named_struct` constructs the BLOB INLINE value in SQL — same shape the
DataFrame demo builds with `struct(lit("INLINE").as("type"), ...)`. The
`embedding` column passes through as-is because Hudi accepts `ARRAY<FLOAT>`
for a `VECTOR(N)` column.

### Step 7 — `hudi_vector_search` (SQL)

Exactly the same TVF as in the DataFrame demo:

```sql
SELECT image_id, category, image_bytes, _hudi_distance
FROM hudi_vector_search(
    '/tmp/hudi_sql_lance_pets',
    'embedding',
    ARRAY(0.0123, 0.4567, ...),   -- 1024 floats inlined from the query embedding
    6,                             -- k + 1 (the query image itself is also in the corpus)
    'cosine'
)
ORDER BY _hudi_distance
```

### Step 8 — visualization

Reused verbatim from the DataFrame variant.

### `main()` flow

```
create_spark()
  → load_dataset()
  → create_embedding_model() + generate_embeddings()
  → register_staging_view()              # Python → Spark temp view
  → create_hudi_table_sql()              # CREATE TABLE ... VECTOR/BLOB
  → insert_into_hudi_sql()               # INSERT INTO ... SELECT named_struct(...)
  → spark.sql("SELECT ... LIMIT 5")      # preview
  → find_similar_sql()                   # hudi_vector_search TVF
  → visualize_and_save()
  → spark.stop()
```

All five Hudi-facing steps (CREATE, INSERT, preview SELECT, vector search,
and the COUNT validation inside the INSERT helper) are raw SQL strings the
viewer can read top-to-bottom.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `RecursionError: Stack overflow` during `createDataFrame` | Python 3.13+ | Use Python 3.12 — PySpark 3.5 doesn't support 3.13+ |
| `java.lang.OutOfMemoryError: Java heap space` during write | Default driver heap too small for N>~300 | `export PYSPARK_DRIVER_MEMORY=8g` |
| `java.net.SocketException: No buffer space available` | macOS socket buffer saturated by parallel Python workers streaming BLOB bytes | Script sets `spark.default.parallelism=2` — if it still fires, drop to 1 or `sudo sysctl -w kern.ipc.maxsockbuf=16777216` |
| `query vector must be a constant expression` | Query vector passed as subquery | Use `ARRAY(f1, f2, …)` literal — script does this |
| Demo starts but hangs on `OxfordIIITPet` download | First-run 800 MB dataset download | Wait; lands in `~/.cache/torchvision/` and is cached for subsequent runs |
