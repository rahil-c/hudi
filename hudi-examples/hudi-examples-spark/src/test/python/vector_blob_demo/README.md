# Hudi VECTOR + BLOB + Vector Search demo (PySpark + Lance)

End-to-end PySpark demo that exercises three Hudi 1.2.0 features together on
the Oxford-IIIT Pet dataset:

1. **VECTOR type** — embedding column is annotated with
   `hudi_type = "VECTOR(<dim>)"`.
2. **BLOB type (INLINE)** — image bytes are written as a Hudi BLOB struct
   tagged with `hudi_type = "BLOB"`.
3. **Vector search** — cosine similarity top-K via the
   `hudi_vector_search` SQL table-valued function, backed by Lance files.

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

python hudi_lance_vector_blob_demo.py
```

Once it works, crank it up:

```bash
export HUDI_LANCE_DEMO_N=1000
python hudi_lance_vector_blob_demo.py
```

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
| `LANCE_BUNDLE_JAR` | **required** | Lance spark bundle |
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

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `RecursionError: Stack overflow` during `createDataFrame` | Python 3.13+ | Use Python 3.12 — PySpark 3.5 doesn't support 3.13+ |
| `java.lang.OutOfMemoryError: Java heap space` during write | Default driver heap too small for N>~300 | `export PYSPARK_DRIVER_MEMORY=8g` |
| `java.net.SocketException: No buffer space available` | macOS socket buffer saturated by parallel Python workers streaming BLOB bytes | Script sets `spark.default.parallelism=2` — if it still fires, drop to 1 or `sudo sysctl -w kern.ipc.maxsockbuf=16777216` |
| `query vector must be a constant expression` | Query vector passed as subquery | Use `ARRAY(f1, f2, …)` literal — script does this |
| Demo starts but hangs on `OxfordIIITPet` download | First-run 800 MB dataset download | Wait; lands in `~/.cache/torchvision/` and is cached for subsequent runs |
