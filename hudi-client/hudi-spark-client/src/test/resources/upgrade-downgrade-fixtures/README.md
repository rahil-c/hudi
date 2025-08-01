# Hudi Upgrade/Downgrade Test Fixtures

This directory contains pre-created MOR Hudi tables from different releases used for testing upgrade/downgrade functionality.

## Fixture Tables

| Directory | Hudi Version | Table Version | Description |
|-----------|--------------|---------------|-------------|
| `hudi-v4-table/` | 0.11.1       | 4 | Basic MOR table with base files and log files |
| `hudi-v5-table/` | 0.12.2       | 5 | Basic MOR table with base files and log files |
| `hudi-v6-table/` | 0.14.0       | 6 | Basic MOR table with base files and log files |
| `hudi-v8-table/` | 1.0.2        | 8 | Basic MOR table with base files and log files |
| `hudi-v9-table/` | 1.1.0        | 9 | Basic MOR table with base files and log files |

## Table Schema

All fixture tables use a consistent simple schema:
- `id` (string) - Record identifier
- `name` (string) - Record name  
- `ts` (long) - Timestamp
- `partition` (string) - Partition value

## Table Structure

Each fixture table contains:
- 2-3 base files (parquet)
- 2-3 log files 
- Multiple committed instants
- 1 pending/failed write (for rollback testing)
- Basic .hoodie metadata structure

## Generating Fixtures

### Prerequisites
- Docker installed
- Internet connection (for downloading Hudi bundles via Maven)

### Generation Process

Use the `generate-fixtures.sh` script to create all fixture tables:

```bash
./generate-fixtures.sh
```

**Note**: The script will create fixture tables in the `mor-tables/` directory. Each fixture generation may take several minutes as it downloads Hudi bundles and creates table data.

### Docker Images and Compatibility Matrix

The script uses official Docker Hub Spark images with Hudi bundles downloaded via the `--packages` flag:

| Hudi Version | Table Version | Spark Version | Scala Version | Docker Image |
|--------------|---------------|---------------|---------------|-------------|
| 0.11.1       | 4             | 3.2.4         | 2.12          | apache/spark:v3.2.4 |
| 0.12.2       | 5             | 3.3.1         | 2.12          | apache/spark:3.3.1 |
| 0.14.0       | 6             | 3.4.1         | 2.12          | apache/spark:3.4.1 |
| 1.0.2        | 8             | 3.5.4         | 2.12          | apache/spark:3.5.4 |
| 1.1.0        | 9             | 3.5.4         | 2.12          | apache/spark:3.5.4 |

### Manual Generation Example

#### Hudi 0.11.1 (Version 4)
```bash
# Use official Docker Hub Spark image with cgroup compatibility and writable Ivy cache
mkdir -p /tmp/ivy-cache
docker run -it --rm \
  --cgroupns=host \
  -e JAVA_OPTS="-Djdk.internal.reflect.useDirectMethodHandle=false" \
  -v $(pwd):/workspace \
  -v /tmp/ivy-cache:/tmp/.ivy2 \
  apache/spark:v3.2.4 \
  /opt/spark/bin/spark-shell \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.jars.ivy=/tmp/.ivy2' \
  --conf 'spark.sql.warehouse.dir=/tmp/spark-warehouse' \
  --conf 'spark.driver.extraJavaOptions=-Djdk.internal.reflect.useDirectMethodHandle=false' \
  --conf 'spark.executor.extraJavaOptions=-Djdk.internal.reflect.useDirectMethodHandle=false' \
  --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.1 << 'EOF'

// Initialize Spark imports and session
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

// Get or create SparkSession (should already be created by spark-shell)
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

val tableName = "hudi_v4_test_table"
val basePath = "/workspace/hudi-v4-table"

// Create simple test data with consistent schema
val testData = Seq(
  ("id1", "Alice", 1000L, "2023-01-01"),
  ("id2", "Bob", 2000L, "2023-01-01"),  
  ("id3", "Charlie", 3000L, "2023-01-02"),
  ("id4", "David", 4000L, "2023-01-02"),
  ("id5", "Eve", 5000L, "2023-01-03")
)

val df = testData.toDF("id", "name", "ts", "partition")

// Write initial batch (creates base files)
df.write.format("hudi").
  option(PRECOMBINE_FIELD.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition").
  option(TABLE_NAME.key, tableName).
  option("hoodie.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Overwrite).
  save(basePath)

// Write update batch (creates log files)
val updateData = Seq(
  ("id1", "Alice_Updated", 1001L, "2023-01-01"),
  ("id2", "Bob_Updated", 2001L, "2023-01-01"),
  ("id6", "Frank", 6000L, "2023-01-03")
)

val updateDf = updateData.toDF("id", "name", "ts", "partition")

updateDf.write.format("hudi").
  option(PRECOMBINE_FIELD.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition").
  option(TABLE_NAME.key, tableName).
  option("hoodie.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "upsert").
  mode(SaveMode.Append).
  save(basePath)

System.exit(0)
EOF
```

#### Other Versions
For other versions, use the same pattern but with the appropriate Docker image and Hudi bundle version from the compatibility matrix above. For example:
- **Hudi 0.12.2**: `docker run -it --rm -v $(pwd):/workspace apache/spark:3.3.1 /opt/spark/bin/spark-shell --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 ...`
- **Hudi 0.14.0**: `docker run -it --rm -v $(pwd):/workspace apache/spark:3.4.1 /opt/spark/bin/spark-shell --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0 ...`
- **Hudi 1.0.2**: `docker run -it --rm -v $(pwd):/workspace apache/spark:3.5.4 /opt/spark/bin/spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 ...`

## Testing Usage

The `TestUpgradeDowngradeFixtures` class automatically loads these fixtures and tests:

1. **Round-trip Operations**: Upgrade one version up, then downgrade back to original
2. **Auto-upgrade Disabled**: Verify upgrade is skipped when auto-upgrade is disabled  
3. **Rollback/Compaction**: Validate rollback and compaction behavior during operations

## Notes

- Fixtures are copied to temporary directories during testing to avoid modifications
- Each fixture should be self-contained with all necessary metadata
- Keep fixtures minimal but realistic (small data sizes for fast tests)
- Ensure consistent schema across all versions for compatibility testing

## Technical Details

### Docker Image Strategy
- Uses official Apache Spark Docker images (`apache/spark:vX.Y.Z` or `apache/spark:X.Y.Z`)
- Downloads Hudi bundles dynamically via Maven `--packages` flag
- Uses full path `/opt/spark/bin/spark-shell` for reliable execution
- Configures writable Ivy cache directory (`/tmp/.ivy2`) to avoid permission issues
- Sets custom Spark warehouse directory for temporary data
- Includes cgroup v2 compatibility flags (`--cgroupns=host`) for modern Docker environments
- Adds Java options to prevent cgroup-related NullPointerExceptions
- Follows Hudi-Spark compatibility matrix for version selection
- Each fixture uses the latest stable Spark version for that Hudi release
- Automatic image pulling ensures images are available before use

### Bundle Resolution
- Hudi bundles are resolved at runtime: `org.apache.hudi:hudi-sparkX.Y-bundle_Z.W:HUDI_VERSION`
- No need for pre-built custom Docker images
- Ensures compatibility between Spark and Hudi versions

### Hudi-Spark Compatibility Matrix
Based on official Hudi documentation:
- **1.0.x** → 3.5.x (default)
- **0.15.x** → 3.5.x (default) 
- **0.14.x** → 3.4.x (default)
- **0.13.x** → 3.3.x (default)
- **0.12.x** → 3.3.x (default)
- **0.11.x** → 3.2.x (default)

### Essential Spark Configurations
The script includes essential Spark configurations for proper Hudi functionality:
- **KryoSerializer**: `spark.serializer=org.apache.spark.serializer.KryoSerializer` - Better performance and compatibility
- **HoodieCatalog**: `spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog` - Enables Hudi catalog integration
- **HoodieSparkSessionExtension**: `spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension` - Hudi-specific SQL extensions
- **Ivy Cache**: `spark.jars.ivy=/tmp/.ivy2` - Writable directory for dependency resolution
- **Warehouse Directory**: `spark.sql.warehouse.dir=/tmp/spark-warehouse` - Writable location for temporary data

### Troubleshooting
Common issues and their solutions:
- **Permission Denied**: Script uses volume mounts to writable temporary directories
- **Ivy Cache Errors**: Custom Ivy cache location prevents FileNotFoundException during dependency resolution
- **spark-shell Not Found**: Uses absolute path `/opt/spark/bin/spark-shell` for reliable execution
- **CgroupV2 NullPointerException**: Uses `--cgroupns=host` and Java options to bypass cgroup v2 compatibility issues
- **SparkSession Not Found**: Scala scripts properly initialize SparkSession and import necessary Spark SQL classes
- **toDF Method Not Found**: Scripts include `spark.implicits._` import for DataFrame conversion methods

## Maintenance

When new Hudi versions are released:
1. Add new fixture directory for the new table version
2. Update `TestUpgradeDowngradeFixtures.fixtureVersions()` method
3. Update this README with the new version mapping
4. Update the compatibility matrix in `generate-fixtures.sh`
5. Regenerate fixtures if schema changes