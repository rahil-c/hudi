"""
Hudi VECTOR + BLOB + Vector Search demo on Lance files.

End-to-end flow:
  1) Load Oxford-IIIT Pet (cats & dogs, real photos) via torchvision.
  2) Generate image embeddings with a timm backbone.
  3) Write rows into a Hudi table backed by Lance files, with:
       - `embedding`   tagged as Hudi VECTOR(<dim>)
       - `image_bytes` tagged as Hudi BLOB (INLINE)
  4) Run cosine similarity search via the `hudi_vector_search` SQL TVF.
  5) Save the query image, top-K neighbors, and a combined panel figure.

Env vars:
  HUDI_BUNDLE_JAR   (defaults to the repo's packaging/hudi-spark-bundle target)
  LANCE_BUNDLE_JAR  (required — org.lance:lance-spark-bundle-3.5_2.12:0.4.0, matches Hudi pom)
"""

import io
import os
import sys
from pathlib import Path

# MUST run before any `pyspark` import — local-mode driver heap is fixed at
# JVM launch time and cannot be raised via SparkSession.config() later.
# Override with:  export PYSPARK_DRIVER_MEMORY=12g
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
import timm
from sklearn.preprocessing import normalize
from PIL import Image

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from torchvision.datasets import OxfordIIITPet  # noqa: E402

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct


# ======================================================
# CONFIGURATION
# ======================================================

# Switch the Hudi base file format with HUDI_BASE_FILE_FORMAT=parquet|lance (default lance).
_file_format = os.getenv("HUDI_BASE_FILE_FORMAT", "lance").lower()
if _file_format not in ("lance", "parquet"):
    sys.exit(f"ERROR: HUDI_BASE_FILE_FORMAT must be 'lance' or 'parquet', got '{_file_format}'")

CONFIG = {
    "dataset": "OxfordIIITPet",
    # Separate table paths per format so you can run both back-to-back without collision.
    "table_path": f"/tmp/hudi_{_file_format}_pets",
    "table_name": f"pets_{_file_format}",
    "base_file_format": _file_format,
    # Override via env var for quick dry-runs, e.g. HUDI_LANCE_DEMO_N=200
    "n_samples": int(os.getenv("HUDI_LANCE_DEMO_N", "1000")),
    "top_k": 5,
    "embedding_model": "mobilenetv3_small_100",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "panel_filename": f"hudi_{_file_format}_results.png",
    "log_level": "ERROR",
    "hide_progress": True,
}

# Matches HoodieSchema.TYPE_METADATA_FIELD in hudi-common.
HUDI_TYPE_METADATA_KEY = "hudi_type"

# Matches HoodieSchema.Blob constants in hudi-common.
BLOB_TYPE_INLINE = "INLINE"
BLOB_FIELD_TYPE = "type"
BLOB_FIELD_DATA = "data"
BLOB_FIELD_REFERENCE = "reference"
BLOB_REFERENCE_CAST = (
    "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"
)


# ======================================================
# UTILITIES
# ======================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_png_bytes(img_bytes: bytes, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "wb") as f:
        f.write(img_bytes)


def default_hudi_bundle_jar() -> str:
    # This file lives at <repo>/hudi-examples/hudi-examples-spark/src/test/python/vector_blob_demo/
    # parents[0]=vector_blob_demo, [1]=python, [2]=test, [3]=src, [4]=hudi-examples-spark,
    # [5]=hudi-examples, [6]=repo root.
    repo_root = Path(__file__).resolve().parents[6]
    return str(
        repo_root
        / "packaging"
        / "hudi-spark-bundle"
        / "target"
        / "hudi-spark3.5-bundle_2.12-1.2.0-SNAPSHOT.jar"
    )


def resolve_jars() -> str:
    hudi_jar = os.getenv("HUDI_BUNDLE_JAR", default_hudi_bundle_jar())
    lance_jar = os.getenv("LANCE_BUNDLE_JAR")
    if not lance_jar:
        sys.exit(
            "ERROR: LANCE_BUNDLE_JAR is not set. Matching Hudi's pom, grab "
            "org.lance:lance-spark-bundle-3.5_2.12:0.4.0 from "
            "https://central.sonatype.com/artifact/org.lance/lance-spark-bundle-3.5_2.12/0.4.0 "
            "and export LANCE_BUNDLE_JAR=/abs/path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar."
        )
    for label, path in [("HUDI_BUNDLE_JAR", hudi_jar), ("LANCE_BUNDLE_JAR", lance_jar)]:
        if not Path(path).is_file():
            sys.exit(f"ERROR: {label} does not exist at {path}")
    return f"{hudi_jar},{lance_jar}"


# ======================================================
# 1. SPARK SESSION SETUP
# ======================================================

def create_spark() -> SparkSession:
    """Initialize Spark with Hudi + Lance support."""
    jars = resolve_jars()

    builder = (
        SparkSession.builder.appName("Hudi-Vector-Blob-Demo")
        .config("spark.jars", jars)
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Explicit default — CONTENT returns inline bytes as written.
        # Switch to DESCRIPTOR to defer byte reads via read_blob().
        .config("hoodie.read.blob.inline.mode", "CONTENT")
        # Low parallelism in local mode: the Python source list flows to JVM
        # as a PythonRDD, and downstream collects push records BACK to Python
        # workers through localhost sockets. One worker per core overwhelms
        # macOS socket buffers on binary (BLOB) payloads. 2 is plenty for a demo.
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if CONFIG.get("hide_progress", True):
        builder = builder.config("spark.ui.showConsoleProgress", "false")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(CONFIG.get("log_level", "ERROR"))
    return spark


# ======================================================
# 2. LOAD DATASET (Oxford-IIIT Pet)
# ======================================================

def load_dataset(n_samples):
    """Load Oxford-IIIT Pet (trainval split), return list[dict] + class names."""
    print(f"Loading dataset: Oxford-IIIT Pet ({n_samples} samples)...")

    root = os.path.expanduser("~/.cache/torchvision")
    ds = OxfordIIITPet(root=root, split="trainval", download=True)
    class_names = ds.classes

    rng = np.random.default_rng()
    n = min(n_samples, len(ds))
    indices = rng.choice(len(ds), size=n, replace=False)

    data = []
    for idx in indices:
        img, label = ds[int(idx)]
        img = img.convert("RGB")

        bio = io.BytesIO()
        img.save(bio, format="PNG")
        img_bytes = bio.getvalue()

        w, h = img.size
        category = class_names[label] if isinstance(class_names, list) else str(label)
        safe_category = category.replace("/", "_")

        data.append(
            {
                "image_id": f"pets_{int(idx):06d}",
                "category": category,
                "category_sanitized": safe_category,
                "label": int(label),
                "description": f"{category} from Oxford-IIIT Pet",
                "image_bytes_raw": img_bytes,
                "width": int(w),
                "height": int(h),
            }
        )

    print(f"✓ Loaded {len(data)} images from Oxford-IIIT Pet")
    return data, class_names


# ======================================================
# 3. EMBEDDING MODEL (timm)
# ======================================================

def create_embedding_model():
    print(f"Loading embedding model: {CONFIG['embedding_model']}...")
    model = timm.create_model(
        CONFIG["embedding_model"],
        pretrained=True,
        num_classes=0,
    )
    model.eval()

    data_config = timm.data.resolve_model_data_config(model)
    transform = timm.data.create_transform(**data_config, is_training=False)
    print("✓ Model loaded")
    return model, transform


def generate_embeddings(data, model, transform):
    print(f"Generating embeddings for {len(data)} images...")

    images = []
    for item in data:
        img = Image.open(io.BytesIO(item["image_bytes_raw"])).convert("RGB")
        images.append(transform(img))

    batch = torch.stack(images)
    with torch.no_grad():
        feats = model(batch).detach().cpu().numpy()

    feats = normalize(feats)

    for i, item in enumerate(data):
        item["embedding"] = feats[i].tolist()

    print(f"✓ Generated embeddings (dimension: {feats.shape[1]})")
    return data, int(feats.shape[1])


# ======================================================
# 4. WRITE TO HUDI TABLE WITH LANCE FORMAT
# ======================================================

def inline_blob_struct(bytes_col):
    """Mirror of BlobTestHelpers.inlineBlobStructCol in PySpark."""
    return struct(
        lit(BLOB_TYPE_INLINE).alias(BLOB_FIELD_TYPE),
        bytes_col.cast("binary").alias(BLOB_FIELD_DATA),
        lit(None).cast(BLOB_REFERENCE_CAST).alias(BLOB_FIELD_REFERENCE),
    )


def stamp_blob_metadata(df, column_name: str):
    """Attach `hudi_type = BLOB` metadata on a struct column.

    In PySpark the only way to put Metadata on an existing column is via
    `Column.alias(name, metadata=...)`, so we re-select every column and
    rewrite the blob one through alias.
    """
    blob_meta = {HUDI_TYPE_METADATA_KEY: "BLOB"}
    projections = []
    for field in df.schema.fields:
        if field.name == column_name:
            projections.append(col(field.name).alias(field.name, metadata=blob_meta))
        else:
            projections.append(col(field.name))
    return df.select(*projections)


def stage_to_parquet_with_pyarrow(data, embedding_dim: int, staging_path: str) -> None:
    """
    Write the Python data to a Parquet file **directly via PyArrow**, bypassing
    Spark entirely for the staging step.

    Why not `spark.createDataFrame(data).write.parquet(...)`? Because that still
    builds a `PythonRDD` internally — its tasks stream pickled rows to Python
    workers through localhost sockets during the write. On macOS the kernel
    socket-buffer (mbuf) pool is small; at ~1000 binary rows it saturates and
    the write fails with `No buffer space available (Write failed)`.

    PyArrow writes the Parquet file in-process from Python, then
    `spark.read.parquet(...)` loads it JVM-natively. There is no `PythonRDD`
    in the lineage at any point, so no localhost socket traffic ever happens
    and the macOS mbuf issue simply can't occur.
    """
    arrow_schema = pa.schema(
        [
            pa.field("image_id", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("category_sanitized", pa.string(), nullable=False),
            pa.field("label", pa.int32(), nullable=False),
            pa.field("description", pa.string(), nullable=True),
            pa.field("image_bytes_raw", pa.binary(), nullable=False),
            pa.field("width", pa.int32(), nullable=False),
            pa.field("height", pa.int32(), nullable=False),
            pa.field(
                "embedding",
                pa.list_(pa.float32(), list_size=embedding_dim),
                nullable=False,
            ),
        ]
    )

    columns = {
        "image_id": [d["image_id"] for d in data],
        "category": [d["category"] for d in data],
        "category_sanitized": [d["category_sanitized"] for d in data],
        "label": [int(d["label"]) for d in data],
        "description": [d.get("description") for d in data],
        "image_bytes_raw": [d["image_bytes_raw"] for d in data],
        "width": [int(d["width"]) for d in data],
        "height": [int(d["height"]) for d in data],
        "embedding": [d["embedding"] for d in data],
    }
    table = pa.table(columns, schema=arrow_schema)
    pq.write_table(table, staging_path)


def write_to_hudi(spark, data, embedding_dim: int):
    print("\nWriting to Hudi table (Lance base file format, VECTOR + BLOB)...")

    # Stage through local Parquet written by PyArrow to break the PythonRDD
    # lineage entirely. See stage_to_parquet_with_pyarrow() for why.
    staging_parquet_path = f"/tmp/staging_{CONFIG['table_name']}_parquet.parquet"
    print(f"Staging Python data → Parquet at {staging_parquet_path} (PyArrow, no Spark)...")
    stage_to_parquet_with_pyarrow(data, embedding_dim, staging_parquet_path)
    df = spark.read.parquet(staging_parquet_path)

    # PyArrow doesn't write Spark's schema JSON footer, so StructField metadata
    # didn't survive. Re-stamp the `embedding` column with VECTOR(N) metadata
    # so Hudi recognizes it at write time.
    vector_meta = {HUDI_TYPE_METADATA_KEY: f"VECTOR({embedding_dim})"}
    df = df.select(
        *[
            col(f.name).alias(f.name, metadata=vector_meta)
            if f.name == "embedding"
            else col(f.name)
            for f in df.schema.fields
        ]
    )

    # Project `image_bytes_raw` into the BLOB INLINE struct shape, then drop the raw column.
    df = df.withColumn("image_bytes", inline_blob_struct(col("image_bytes_raw")))
    df = df.drop("image_bytes_raw")
    df = stamp_blob_metadata(df, "image_bytes")

    hudi_options = {
        "hoodie.table.name": CONFIG["table_name"],
        "hoodie.datasource.write.recordkey.field": "image_id",
        "hoodie.datasource.write.precombine.field": "image_id",
        "hoodie.datasource.write.partitionpath.field": "category_sanitized",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.table.base.file.format": CONFIG["base_file_format"],
        "hoodie.write.record.merge.custom.implementation.classes": (
            "org.apache.hudi.DefaultSparkRecordMerger"
        ),
    }

    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(CONFIG["table_path"])
    )

    count = df.count()
    print(f"✓ Wrote {count} records to Hudi table at {CONFIG['table_path']}")
    return df


# ======================================================
# 5. SIMILARITY SEARCH USING HUDI VECTOR SEARCH
# ======================================================

def find_similar(spark, query_embedding):
    """Find similar images using hudi_vector_search TVF.

    The TVF requires the query vector to be a constant expression, so we
    inline the embedding as a literal `ARRAY(f1, f2, ...)` — matches the
    pattern in TestHoodieVectorSearchFunction.scala.
    """
    print(f"\nPerforming similarity search (top {CONFIG['top_k']})...")

    array_literal = "ARRAY(" + ", ".join(f"{float(v)}" for v in query_embedding) + ")"

    query = f"""
        SELECT image_id, category, image_bytes, _hudi_distance
        FROM hudi_vector_search(
            '{CONFIG['table_path']}',
            'embedding',
            {array_literal},
            {CONFIG['top_k'] + 1},
            'cosine'
        )
        ORDER BY _hudi_distance
    """

    rows = spark.sql(query).collect()

    results = []
    for row in rows:
        distance = float(row["_hudi_distance"])

        # Skip the query image itself (distance ~ 0 for L2-normalized embeddings).
        if distance < 0.001:
            continue
        if len(results) >= CONFIG["top_k"]:
            break

        # With BLOB INLINE + CONTENT mode, `image_bytes` is a struct<type,data,reference>.
        blob = row["image_bytes"]
        inline_bytes = blob[BLOB_FIELD_DATA] if blob is not None else None

        results.append(
            {
                "image_id": row["image_id"],
                "category": row["category"],
                "image_bytes": bytes(inline_bytes) if inline_bytes is not None else b"",
                "similarity": 1.0 - distance,
            }
        )

    print(f"✓ Found {len(results)} similar images using Hudi vector search")
    return results


# ======================================================
# 6. VISUALIZATION
# ======================================================

def visualize_and_save(query_image_bytes, query_category, results):
    print("\nCreating visualization and saving images...")

    out_dir = Path(CONFIG["output_dir"])
    ensure_dir(out_dir)

    query_path = out_dir / "query.png"
    save_png_bytes(query_image_bytes, query_path)

    result_paths = []
    for i, r in enumerate(results, 1):
        p = out_dir / f"top{i}.png"
        save_png_bytes(r["image_bytes"], p)
        result_paths.append(str(p.resolve()))

    n_results = len(results)
    fig, axes = plt.subplots(1, n_results + 1, figsize=(3 * (n_results + 1), 3.2))

    query_img = Image.open(io.BytesIO(query_image_bytes)).convert("RGB")
    axes[0].imshow(query_img)
    axes[0].set_title(f"QUERY\n{query_category}", fontweight="bold")
    axes[0].axis("off")

    for i, result in enumerate(results):
        img = Image.open(io.BytesIO(result["image_bytes"])).convert("RGB")
        axes[i + 1].imshow(img)
        axes[i + 1].set_title(f"{result['category']}\nSim: {result['similarity']:.3f}")
        axes[i + 1].axis("off")

    plt.tight_layout()
    panel_path = out_dir / CONFIG["panel_filename"]
    plt.savefig(str(panel_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("✓ Saved files:")
    print(f"  Query image: {query_path.resolve()}")
    for i, p in enumerate(result_paths, 1):
        print(f"  Top{i}: {p}")
    print(f"  Panel: {panel_path.resolve()}")

    return {
        "query": str(query_path.resolve()),
        "tops": result_paths,
        "panel": str(panel_path.resolve()),
    }


# ======================================================
# MAIN
# ======================================================

def main():
    print("\n" + "=" * 80)
    print("HUDI VECTOR + BLOB + VECTOR SEARCH DEMO (Oxford-IIIT Pet)")
    print("=" * 80 + "\n")

    spark = create_spark()

    data, _class_names = load_dataset(CONFIG["n_samples"])

    model, transform = create_embedding_model()
    data, embedding_dim = generate_embeddings(data, model, transform)

    df = write_to_hudi(spark, data, embedding_dim)

    print("\nSample rows:")
    df.select("image_id", "category", "description").show(5, truncate=False)

    query_idx = np.random.randint(len(data))
    query_item = data[query_idx]
    print(f"\nQuery: {query_item['image_id']} ({query_item['category']})")

    results = find_similar(spark, query_item["embedding"])

    print("\nTop matches:")
    for i, r in enumerate(results, 1):
        print(
            f"  {i}. {r['image_id']} - {r['category']} "
            f"(similarity: {r['similarity']:.3f})"
        )

    saved = visualize_and_save(
        query_item["image_bytes_raw"],
        query_item["category"],
        results,
    )

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:   {CONFIG['dataset']} ({CONFIG['n_samples']} images)")
    print(f"✓ Model:     {CONFIG['embedding_model']} (dim={embedding_dim})")
    print(f"✓ Storage:   Hudi table, {CONFIG['base_file_format']} base file format")
    print("✓ Types:     embedding = VECTOR(%d), image_bytes = BLOB (INLINE)" % embedding_dim)
    print("✓ Search:    hudi_vector_search, cosine distance, L2-normalized")
    print(f"✓ Table:     {CONFIG['table_path']}")
    print(f"✓ Panel:     {saved['panel']}")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
