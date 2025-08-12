package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.ParquetSchemaUtil;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Imports each subfolder under /data as a separate Iceberg table in the "nyc" namespace,
 * aggregating all .parquet/.prq files found recursively.
 *
 * NOTE: Parquet UINT_64 columns are coerced to signed LONG (INT64 with no logical annotation)
 * to keep ingestion zero-rewrite. If your data contains values > Long.MAX_VALUE, they will
 * appear negative. In that case, rewrite those files to DECIMAL(20,0) backed by FIXED bytes.
 */
public class ImportParquetFolders {

  // Change these two if desired
  private static final String WAREHOUSE_URI   = "file:///warehouse";
  private static final String TABLE_NAMESPACE = "nyc";

  public static void main(String[] args) throws Exception {

    Path dataDir = Paths.get("/data");
    if (!Files.isDirectory(dataDir)) {
      System.err.println("No /data folder mounted"); System.exit(1);
    }

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, WAREHOUSE_URI);

    // Ensure the namespace exists so HiveServer2/Superset can list it
    Namespace ns = Namespace.of(TABLE_NAMESPACE);
    if (!catalog.namespaceExists(ns)) {
      catalog.createNamespace(ns);
    }

    Namespace def = Namespace.of("default");
    if (!catalog.namespaceExists(def)) {
        catalog.createNamespace(def);
    }

    // ── iterate over each immediate sub-folder ──
    try (Stream<Path> folders = Files.list(dataDir)) {
      folders.filter(Files::isDirectory).forEach(dir -> {
        try {
          importFolder(dir, catalog, conf);
        } catch (Exception e) {
          System.err.printf("✖  %s  (%s)%n", dir.getFileName(), e.getMessage());
        }
      });
    }
  }

  /** Aggregate every .parquet/.prq file in <dir> into one Iceberg table named after the folder */
  private static void importFolder(Path dir, HadoopCatalog catalog, Configuration conf) throws Exception {

    // Collect parquet files under this folder (recursive) - now includes .prq files
    List<File> files;
    try (Stream<Path> walk = Files.walk(dir)) {
      files = walk.filter(p -> isParquetFile(p.toString()))
                  .map(Path::toFile)
                  .collect(Collectors.toList());
    }
    if (files.isEmpty()) {                // skip empty folders like "test/"
      System.out.printf("ℹ  %s: no parquet files, skipping%n", dir.getFileName());
      return;
    }

    String tableName = dir.getFileName().toString().toLowerCase();
    TableIdentifier id = TableIdentifier.of(TABLE_NAMESPACE, tableName);

    // Create or load table (derive schema only on first load)
    Table table;
    if (catalog.tableExists(id)) {
      table = catalog.loadTable(id);
    } else {
      Schema schema = deriveSchema(files.get(0), conf, tableName);
      table = catalog.createTable(id, schema, PartitionSpec.unpartitioned());
    }

    AppendFiles append = table.newAppend();
    long totalRows = 0;

    for (File parquet : files) {
      long rows = rowCount(parquet, conf);
      DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath(parquet.toURI().toString())
          .withFileSizeInBytes(parquet.length())
          .withRecordCount(rows)
          .withFormat(FileFormat.PARQUET)
          .build();
      append.appendFile(df);
      totalRows += rows;
    }

    append.commit();
    System.out.printf("✔  %-20s → %s  (%,d files | %,d rows)%n",
        dir.getFileName(), id, files.size(), totalRows);
  }

  /** Check if a file is a parquet file (supports both .parquet and .prq extensions) */
  private static boolean isParquetFile(String filePath) {
    String lowerPath = filePath.toLowerCase();
    return lowerPath.endsWith(".parquet") || lowerPath.endsWith(".prq");
  }

  /**
   * Extract Iceberg schema from the first parquet file in a folder.
   * Before converting with ParquetSchemaUtil, coerce any UINT_64 primitives
   * to plain INT64 (no logical annotation) so Iceberg treats them as LONG.
   */
  private static Schema deriveSchema(File parquet, Configuration conf, String tableName) throws Exception {
    try (ParquetFileReader r = ParquetFileReader.open(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquet.getAbsolutePath()), conf))) {

      MessageType original = r.getFooter().getFileMetaData().getSchema();
      List<String> coercedCols = new ArrayList<>();

      MessageType transformed = coerceUint64ToLong(original, coercedCols);

      if (!coercedCols.isEmpty()) {
        System.out.printf(
            "ℹ  %s: coercing UINT_64 → LONG for columns: %s%n",
            tableName, String.join(", ", coercedCols));
      }

      return ParquetSchemaUtil.convert(transformed);
    }
  }

  /** Count records in a parquet file (used for DataFiles metadata) */
  private static long rowCount(File parquet, Configuration conf) throws Exception {
    try (ParquetFileReader r = ParquetFileReader.open(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquet.getAbsolutePath()), conf))) {
      return r.getRecordCount();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Schema transformation helpers
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Returns a new MessageType where any primitive field annotated as UINT_64
   * is rebuilt as a plain INT64 (no logical annotation). Others unchanged.
   */
  private static MessageType coerceUint64ToLong(MessageType ps, List<String> coercedCols) {
    List<Type> newFields = new ArrayList<>();
    for (Type f : ps.getFields()) {
      newFields.add(transformType(f, coercedCols, f.getName()));
    }
    return new MessageType(ps.getName(), newFields);
  }

  private static Type transformType(Type t, List<String> coercedCols, String path) {
    if (t.isPrimitive()) {
      PrimitiveType pt = t.asPrimitiveType();
      if (isUInt64(pt)) {
        coercedCols.add(path);
        // Rebuild as INT64 with NO logical/original annotation, preserve repetition & name
        return Types.primitive(PrimitiveTypeName.INT64, pt.getRepetition())
            .named(t.getName());
      } else {
        return t; // leave other primitives unchanged
      }
    } else {
      // Rebuild group and recurse on children, preserving logical/original annotations where present.
      GroupType gt = t.asGroupType();
      List<Type> children = new ArrayList<>();
      for (Type child : gt.getFields()) {
        children.add(transformType(child, coercedCols, path + "." + child.getName()));
      }

      Types.GroupBuilder<GroupType> gb = Types.buildGroup(gt.getRepetition());
      LogicalTypeAnnotation l = gt.getLogicalTypeAnnotation();
      OriginalType o = gt.getOriginalType();
      if (l != null) {
        gb = gb.as(l);
      } else if (o != null) {
        gb = gb.as(o);
      }
      return gb.addFields(children.toArray(new Type[0]))
               .named(gt.getName());
    }
  }

  /** Detect Parquet UINT_64 via either the new logical annotation or the legacy original type. */
  private static boolean isUInt64(PrimitiveType pt) {
    LogicalTypeAnnotation l = pt.getLogicalTypeAnnotation();
    if (l instanceof IntLogicalTypeAnnotation) {
      IntLogicalTypeAnnotation il = (IntLogicalTypeAnnotation) l;
      if (!il.isSigned() && il.getBitWidth() == 64) {
        return true;
      }
    }
    OriginalType ot = pt.getOriginalType();
    return ot == OriginalType.UINT_64;
  }
}
