"""
Hudi VECTOR + BLOB + Vector Search demo — **Spark SQL variant**.

Same shape as `hudi_lance_vector_blob_demo.py`, but every Hudi-touching
operation is a SQL statement so viewers see the actual DDL/DML surface:

  CREATE TABLE ... (embedding VECTOR(1024), image_bytes BLOB, ...) USING hudi
  INSERT  INTO ... SELECT ..., named_struct('type', 'INLINE', ...) FROM staging
  SELECT  ... FROM hudi_vector_search('<path>', 'embedding', ARRAY(...), k, 'cosine')

Image loading (torchvision) and embedding generation (timm) stay in Python —
those cannot be SQL. The bridge between the two is a Spark temp view.

Env vars (same as the DataFrame variant):
  HUDI_BUNDLE_JAR         (defaults to repo's packaging/hudi-spark-bundle target)
  LANCE_BUNDLE_JAR        (required)
  HUDI_BASE_FILE_FORMAT   (default 'lance'; set to 'parquet' to use Parquet)
  HUDI_LANCE_DEMO_N       (default 1000; number of images to ingest)
  PYSPARK_DRIVER_MEMORY   (default '4g')
  HUDI_LANCE_DEMO_OUTDIR  (default './outputs')
"""

import io
import os
import sys
from pathlib import Path

# MUST run before any `pyspark` import — local-mode driver heap is fixed at
# JVM launch time and cannot be raised via SparkSession.config() later.
_driver_mem = os.getenv("PYSPARK_DRIVER_MEMORY", "4g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--driver-memory {_driver_mem} --conf spark.driver.maxResultSize=2g pyspark-shell",
)

import numpy as np
import torch
import timm
from sklearn.preprocessing import normalize
from PIL import Image

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from torchvision.datasets import OxfordIIITPet  # noqa: E402

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ======================================================
# CONFIGURATION
# ======================================================

_file_format = os.getenv("HUDI_BASE_FILE_FORMAT", "lance").lower()
if _file_format not in ("lance", "parquet"):
    sys.exit(f"ERROR: HUDI_BASE_FILE_FORMAT must be 'lance' or 'parquet', got '{_file_format}'")

CONFIG = {
    "dataset": "OxfordIIITPet",
    # Separate paths from the DataFrame variant so both can coexist on disk.
    "table_path": f"/tmp/hudi_sql_{_file_format}_pets",
    "table_name": f"pets_sql_{_file_format}",
    "base_file_format": _file_format,
    "n_samples": int(os.getenv("HUDI_LANCE_DEMO_N", "1000")),
    "top_k": 5,
    "embedding_model": "mobilenetv3_small_100",
    "output_dir": os.getenv("HUDI_LANCE_DEMO_OUTDIR", "./outputs"),
    "panel_filename": f"hudi_sql_{_file_format}_results.png",
    "log_level": "ERROR",
    "hide_progress": True,
}

# The cast target used inside the BLOB INLINE struct's null reference field.
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
    # parents[0]=vector_blob_demo, [1]=python, [2]=test, [3]=src,
    # [4]=hudi-examples-spark, [5]=hudi-examples, [6]=repo root.
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
    """Identical Spark session to the DataFrame variant."""
    jars = resolve_jars()

    builder = (
        SparkSession.builder.appName("Hudi-SQL-Vector-Blob-Demo")
        .config("spark.jars", jars)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("hoodie.read.blob.inline.mode", "CONTENT")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if CONFIG.get("hide_progress", True):
        builder = builder.config("spark.ui.showConsoleProgress", "false")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(CONFIG.get("log_level", "ERROR"))
    return spark


# ======================================================
# 2. LOAD DATASET (Oxford-IIIT Pet) — pure Python
# ======================================================

def load_dataset(n_samples):
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
# 3. EMBEDDING MODEL (timm) — pure Python
# ======================================================

def create_embedding_model():
    print(f"Loading embedding model: {CONFIG['embedding_model']}...")
    model = timm.create_model(
        CONFIG["embedding_model"], pretrained=True, num_classes=0
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
# 4. BRIDGE: register the Python data as a Spark temp view
# ======================================================

STAGING_VIEW = "staging_pets"


def register_staging_view(spark: SparkSession, data):
    """
    Push the Python list of dicts into a Spark temp view so the rest of the
    demo can SELECT from it. No Hudi metadata is needed here — the target
    Hudi table's DDL carries the VECTOR/BLOB types, and INSERT INTO will
    type-check/shape the values against that schema.
    """
    schema = StructType(
        [
            StructField("image_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("category_sanitized", StringType(), False),
            StructField("label", IntegerType(), False),
            StructField("description", StringType(), True),
            StructField("image_bytes_raw", BinaryType(), False),
            StructField("width", IntegerType(), False),
            StructField("height", IntegerType(), False),
            StructField(
                "embedding",
                ArrayType(FloatType(), containsNull=False),
                nullable=False,
            ),
        ]
    )

    spark.createDataFrame(data, schema=schema).createOrReplaceTempView(STAGING_VIEW)
    print(f"✓ Registered Spark temp view: {STAGING_VIEW}")


# ======================================================
# 5. CREATE TABLE — SQL
# ======================================================

def create_hudi_table_sql(spark: SparkSession, embedding_dim: int):
    table = CONFIG["table_name"]
    print(f"\nDDL: CREATE TABLE {table} ...")

    # Drop first so repeat runs are idempotent (also wipes the table_path dir
    # since the catalog's managed-path semantics follow LOCATION).
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ddl = f"""
        CREATE TABLE {table} (
            image_id            STRING,
            category            STRING,
            category_sanitized  STRING,
            label               INT,
            description         STRING,
            image_bytes         BLOB           COMMENT 'Pet image bytes (INLINE)',
            width               INT,
            height              INT,
            embedding           VECTOR({embedding_dim})
                                               COMMENT 'Image embedding for ANN search'
        ) USING hudi
        PARTITIONED BY (category_sanitized)
        LOCATION '{CONFIG['table_path']}'
        TBLPROPERTIES (
            primaryKey = 'image_id',
            preCombineField = 'image_id',
            type = 'cow',
            'hoodie.table.base.file.format' = '{CONFIG['base_file_format']}'
        )
    """
    print(ddl.strip())
    spark.sql(ddl)
    print(f"✓ Created table {table} at {CONFIG['table_path']}")


# ======================================================
# 6. INSERT INTO ... SELECT — SQL
# ======================================================

def insert_into_hudi_sql(spark: SparkSession):
    table = CONFIG["table_name"]
    print(f"\nDML: INSERT INTO {table} SELECT ... FROM {STAGING_VIEW}")

    # `named_struct('type', 'INLINE', 'data', <bytes>, 'reference', null)` is
    # the canonical shape for a Hudi BLOB INLINE value (matches the pattern
    # in TestCreateTable.scala's BLOB INSERT test).
    insert = f"""
        INSERT INTO {table}
        SELECT
            image_id,
            category,
            category_sanitized,
            label,
            description,
            named_struct(
                'type',      'INLINE',
                'data',      image_bytes_raw,
                'reference', cast(null as {BLOB_REFERENCE_CAST})
            ) AS image_bytes,
            width,
            height,
            embedding
        FROM {STAGING_VIEW}
    """
    print(insert.strip())
    spark.sql(insert)

    count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
    print(f"✓ Inserted {count} records into {table}")


# ======================================================
# 7. SIMILARITY SEARCH — SQL (hudi_vector_search TVF)
# ======================================================

def find_similar_sql(spark: SparkSession, query_embedding):
    print(f"\nDQL: hudi_vector_search(...) top {CONFIG['top_k']}")

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
    # Only print the shape — the literal itself is 1024 floats wide.
    print(
        query.replace(array_literal, "ARRAY(<1024 floats inlined from query embedding>)").strip()
    )

    rows = spark.sql(query).collect()

    results = []
    for row in rows:
        distance = float(row["_hudi_distance"])
        # Skip the query row itself (distance ~ 0 on L2-normalized embeddings).
        if distance < 0.001:
            continue
        if len(results) >= CONFIG["top_k"]:
            break

        # BLOB INLINE + CONTENT mode: `image_bytes` is a struct, the bytes are in `data`.
        blob = row["image_bytes"]
        inline_bytes = blob["data"] if blob is not None else None

        results.append(
            {
                "image_id": row["image_id"],
                "category": row["category"],
                "image_bytes": bytes(inline_bytes) if inline_bytes is not None else b"",
                "similarity": 1.0 - distance,
            }
        )

    print(f"✓ Found {len(results)} similar images")
    return results


# ======================================================
# 8. VISUALIZATION
# ======================================================

def visualize_and_save(query_image_bytes, query_category, results):
    print("\nCreating visualization and saving images...")

    out_dir = Path(CONFIG["output_dir"])
    ensure_dir(out_dir)

    query_path = out_dir / f"query_sql_{CONFIG['base_file_format']}.png"
    save_png_bytes(query_image_bytes, query_path)

    result_paths = []
    for i, r in enumerate(results, 1):
        p = out_dir / f"top{i}_sql_{CONFIG['base_file_format']}.png"
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

    print(f"✓ Panel: {panel_path.resolve()}")
    return {"panel": str(panel_path.resolve())}


# ======================================================
# MAIN
# ======================================================

def main():
    print("\n" + "=" * 80)
    print("HUDI VECTOR + BLOB + VECTOR SEARCH DEMO (SQL variant) — Oxford-IIIT Pet")
    print("=" * 80 + "\n")

    spark = create_spark()

    data, _class_names = load_dataset(CONFIG["n_samples"])
    model, transform = create_embedding_model()
    data, embedding_dim = generate_embeddings(data, model, transform)

    register_staging_view(spark, data)
    create_hudi_table_sql(spark, embedding_dim)
    insert_into_hudi_sql(spark)

    print("\nSample rows (SELECT via SQL):")
    spark.sql(
        f"SELECT image_id, category, description FROM {CONFIG['table_name']} LIMIT 5"
    ).show(truncate=False)

    query_idx = np.random.randint(len(data))
    query_item = data[query_idx]
    print(f"\nQuery: {query_item['image_id']} ({query_item['category']})")

    results = find_similar_sql(spark, query_item["embedding"])

    print("\nTop matches:")
    for i, r in enumerate(results, 1):
        print(
            f"  {i}. {r['image_id']} - {r['category']} "
            f"(similarity: {r['similarity']:.3f})"
        )

    saved = visualize_and_save(
        query_item["image_bytes_raw"], query_item["category"], results
    )

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Dataset:   {CONFIG['dataset']} ({CONFIG['n_samples']} images)")
    print(f"✓ Model:     {CONFIG['embedding_model']} (dim={embedding_dim})")
    print(f"✓ Storage:   Hudi table, {CONFIG['base_file_format']} base file format")
    print(f"✓ Path:      {CONFIG['table_path']}")
    print("✓ Types:     embedding = VECTOR(%d), image_bytes = BLOB (INLINE)" % embedding_dim)
    print("✓ Surface:   All Hudi ops via Spark SQL (CREATE / INSERT / hudi_vector_search)")
    print(f"✓ Panel:     {saved['panel']}")
    print("=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
