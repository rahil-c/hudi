---
title: "Release 1.2.0"
sidebar_position: 1
toc: true
last_modified_at:
---

# [Release 1.2.0](https://github.com/apache/hudi/releases/tag/release-1.2.0) ([docs](/docs/quick-start-guide))

Apache Hudi 1.2.0 expands Hudi beyond traditional structured analytics into AI/ML and unstructured-data workloads. The release introduces a richer logical type system, native vector search, first-class blob storage, and Lance as a new base-file format.

* * *

## New Logical Types

Hudi 1.2.0 introduces a set of new logical types defined in RFC-99 (Hudi Type System), extending Hudi's canonical schema with native support for vectors, semi-structured data, and unstructured byte payloads. These types are backed by Arrow-native representations and are designed to map cleanly onto Parquet and Lance physical storage so engines can push down predicates and avoid unnecessary materialization.

### VECTOR

Hudi 1.2.0 adds a `VECTOR` logical type for storing fixed-dimension dense vectors — the core building block for embedding-driven workloads such as retrieval-augmented generation and similarity search. A `VECTOR` is parameterized by an element type (`FLOAT`, `DOUBLE`, or `INT8`) and a fixed `dimension`, and is encoded on disk as a Parquet `FIXED_LEN_BYTE_ARRAY` with byte-stream-split encoding or as a Lance `FixedSizeList`, both of which preserve the vector layout for fast scan and SIMD-friendly processing.

By giving Hudi a first-class vector type rather than treating embeddings as generic arrays, downstream services — vector search (see `hudi_vector_search()` below), indexing, and clustering — can rely on a stable schema contract and skip per-row dimension validation. See RFC-99 and its vector appendix for the full type specification and benchmarks.

### VARIANT

Hudi 1.2.0 adds a `VARIANT` logical type for semi-structured, JSON-like data with flexible schema-on-read. A `VARIANT` value is stored as a binary-encoded `value` payload alongside a key-dictionary `metadata` blob, and may optionally include a `typed_value` projection of frequently accessed fields shredded out into native typed columns. Predicates and projections over those shredded fields are pushed down at the column level, so queries like `WHERE tool_calls[0].name = 'search'` can hit `typed_value` only and skip the binary payload entirely.

`VARIANT` is intended for use cases like event logs, tool-call traces, and tracking data where each row carries a heterogeneous JSON-like document but a stable subset of fields is queried hot. See RFC-99 and its variant appendix for the encoding and shredding rules.

### BLOB

Hudi 1.2.0 adds a `BLOB` logical type for storing unstructured byte payloads — images, documents, audio, model artifacts — directly inside Hudi tables alongside structured columns. A `BLOB` is encoded as a struct with a storage `type` (`INLINE` or `OUT_OF_LINE`), an inline `data` field for small payloads, and a `reference` carrying `(path, offset, length, managed)` for out-of-line storage. The writer dynamically decides between inline and out-of-line based on configurable size thresholds (e.g., `< 1 MB` inline, `> 16 MB` out-of-line), so callers do not have to manage the split themselves.

Readers return blob references lazily by default, deferring byte materialization until explicitly requested by `read_blob()` (see below). All Hudi table services — cleaning, clustering, compaction — operate over BLOB columns, and the `managed` flag on a reference tells the cleaner whether an out-of-line payload is owned by Hudi or by the user. See RFC-99 for the type and RFC-100 for the storage and access model.

* * *

## New Spark SQL Functions

### read_blob()

Hudi 1.2.0 adds the `read_blob()` Spark SQL function for materializing the raw bytes of a `BLOB` column. By default, scans over BLOB columns return only metadata (a `reference` pointer for `OUT_OF_LINE` storage, or a Lance descriptor for `INLINE` storage on Lance), which keeps shuffle-heavy queries cheap and avoids dragging large payloads through stages that do not need them. Calling `read_blob(col)` materializes the underlying bytes in a single pass, transparently handling both inline reads and external `pread` calls for out-of-line references.

```sql
SELECT id, url, read_blob(image_blob) AS image_bytes
FROM my_table;
```

The function is governed by `hoodie.read.blob.inline.mode`, which defaults to `DESCRIPTOR` (metadata-only for inline rows) and can be set to `CONTENT` to eagerly materialize inline bytes. The `INLINE` + `DESCRIPTOR` + Lance combination is intentionally unsupported by `read_blob()` and throws a clear error, since the synthesized reference for that case is an internal pointer into Lance's storage layout rather than user-facing metadata. See RFC-100 for the full read-mode matrix.

### hudi_vector_search()

Hudi 1.2.0 adds `hudi_vector_search()`, a Spark table-valued function for distributed k-nearest-neighbor search over `VECTOR` columns. The function takes a target table, an embedding column, a query vector, a `k`, and a distance metric (`cosine`, `dot`, or `l2`), and returns the matching rows along with a synthesized `_distance` column that callers can sort or filter on. Results compose with normal SQL — `WHERE`, `JOIN`, `ORDER BY` — so retrieval logic and downstream business rules can live in the same query.

```sql
SELECT *
FROM hudi_vector_search(
  table           => 'products',
  embedding_col   => 'embedding',
  query_vector    => ARRAY(0.12F, -0.03F, 0.81F, ...),
  k               => 10,
  distance_metric => 'cosine'
);
```

A batch form of the function takes a `base_table` / `query_table` pair and, for each row in the base table, returns the top-`k` nearest rows from the query table — useful for bulk recommendation, deduplication, and offline retrieval-evaluation jobs. The current implementation performs a distributed brute-force scan and assumes embeddings are already populated in the table; a dedicated vector-index implementation is planned in a follow-up RFC. See RFC-102 for the full specification.

* * *

## New File Format Support

### Lance

Hudi 1.2.0 adds Lance as a supported base-file format, alongside the existing Parquet, ORC, and HFile formats. Lance is a modern columnar format with first-class support for vectors and blobs — its `FixedSizeList` encoding matches Hudi's `VECTOR` type without an extra copy, and its native blob encoding exposes cheap `(file, offset, length)` descriptors that let `BLOB` reads defer byte materialization in the same way that out-of-line storage does. This makes Lance a natural fit for AI/ML tables built on the new logical types introduced in this release.

Lance can be configured per-table or per-column-group, so a single Hudi table can keep structured columns in Parquet while routing vector and blob columns into Lance for better scan performance. Lance integration is currently tracked under issue [#14127](https://github.com/apache/hudi/issues/14127), and a small number of Lance-side limitations remain — see RFC-100 for the current support matrix and known gaps.
