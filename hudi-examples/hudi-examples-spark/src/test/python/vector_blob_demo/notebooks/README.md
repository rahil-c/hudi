# Hudi VECTOR + BLOB demos — Jupyter notebooks

Notebook variants of the three `.py` demos in the parent folder. Same end
state, same Spark / Hudi / Lance jars, but code + narrative + output panels
live inline in one scrollable artifact and feature toggles are plain Python
variables at the top of each notebook.

The original `.py` scripts in the parent folder are unchanged — they stay
the canonical scriptable references and `run_demos.sh` still drives them.

## Setup

Use the **same venv** as the `.py` scripts. From the parent folder:

```bash
cd ../    # back into vector_blob_demo/
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt    # adds jupyter + ipykernel for this folder
```

Set the same env vars the `.py` scripts use (only `LANCE_BUNDLE_JAR` is
strictly required, and only when running with `BASE_FILE_FORMAT = "lance"`):

```bash
export LANCE_BUNDLE_JAR=~/Downloads/lance-spark-bundle-3.5_2.12-0.4.0.jar
# Optional — defaults to <repo>/packaging/hudi-spark-bundle/target/...
# export HUDI_BUNDLE_JAR=/abs/path/to/hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar
```

Launch JupyterLab from this folder so the notebooks find their working dir:

```bash
cd notebooks/
jupyter lab
```

## Notebooks

### `01_blob_reader.ipynb`

Demonstrates Hudi's BLOB type as a **reference** rather than as bytes —
how a tiny Hudi table can point at unstructured data sitting elsewhere, and
how `read_blob()` resolves the reference to bytes at query time.

Toggle variables (top of notebook):

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
BLOB_MODE         = "out_of_line"  # "out_of_line" or "inline"
INLINE_READ_MODE  = "content"      # "content" or "descriptor"
                                   # (only meaningful when BLOB_MODE == "inline")
N_SAMPLES         = 100
```

Storyboard:
- `BLOB_MODE = "out_of_line"` → Hudi table holds `(external_path, offset, length)`
  references; bytes live in `/tmp/pets_blob_container.bin`. Footprint ratio
  shows the Hudi table at <1% of the container.
- `BLOB_MODE = "inline"` + `INLINE_READ_MODE = "content"` → bytes embedded in
  the Hudi base files; `image_bytes.data` returns raw PNG bytes directly.
- `BLOB_MODE = "inline"` + `INLINE_READ_MODE = "descriptor"` → `image_bytes.data`
  is null, `image_bytes.reference.*` is synthesized to point at the underlying
  base file, and `read_blob()` materializes bytes lazily.

### `02_sql_vector_search.ipynb`

Pure-SQL walkthrough of `CREATE TABLE ... (embedding VECTOR(N), image_bytes BLOB, ...) USING hudi`,
`INSERT INTO ... SELECT named_struct('type','INLINE', ...)`, and the
`hudi_vector_search` TVF for cosine similarity top-K.

Toggle variables:

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
N_SAMPLES         = 256
```

### `03_dataframe_vector_search.ipynb`

Same end state as `02`, but built through the PySpark DataFrame API rather
than SQL. Useful for seeing how `StructField(metadata={"hudi_type": "VECTOR(N)"})`
and the BLOB struct shape compose under the hood.

Toggle variables:

```python
BASE_FILE_FORMAT  = "parquet"      # "parquet" or "lance"
N_SAMPLES         = 256
```

## How toggles work

Each notebook starts with a small **toggles cell** containing plain Python
variables. Edit the values and `Run All`. The variables flow into the same
`CONFIG` dict the `.py` scripts build from `os.getenv(...)` — runtime
behavior is identical to the script with the matching env vars set.

`LANCE_BUNDLE_JAR` is read from the environment (it's a host-specific file
location, not a feature toggle). The notebooks fail fast with a clear error
if `BASE_FILE_FORMAT = "lance"` but the env var is unset.

## Sharing `/tmp/` paths with the `.py` scripts

The notebooks write to the same `/tmp/hudi_*_pets/`, `/tmp/pets_blob_container.bin`,
and `/tmp/staging_pets_*.parquet` paths as the `.py` scripts. That's
intentional — every notebook starts with a **cleanup cell** that wipes
those paths (mirroring `clean()` in `run_demos.sh`), so running notebooks
back-to-back with `./run_demos.sh` is fine in either order.

If you ever skip the cleanup cell on a re-run, you may hit `EOFException` in
`BatchedBlobReader` because old Hudi commits reference offsets past EOF in
a freshly overwritten container file. Always Run All from the top, or at
minimum re-run the cleanup cell before re-running the rest.
