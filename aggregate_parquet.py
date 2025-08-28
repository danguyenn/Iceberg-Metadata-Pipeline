#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Iterable, List

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq

SUPPORTED_EXTS = {".parquet", ".prq"}

def find_parquet_files(root: Path) -> List[str]:
    paths = []
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
            paths.append(str(p))
    return paths

# ---------- Sanitization (batch-wise) ----------
def target_field_type(name: str, ftype: pa.DataType) -> pa.DataType:
    # 'timestamp' column special-case: epoch micros to TIMESTAMP(us, UTC)
    if name == "timestamp":
        if pa.types.is_uint64(ftype) or (pa.types.is_decimal128(ftype) and ftype.scale == 0 and ftype.precision >= 18):
            return pa.timestamp("us", tz="UTC")
        return ftype
    # All other UINT64 -> DECIMAL(20,0)
    if pa.types.is_uint64(ftype):
        return pa.decimal128(20, 0)
    return ftype

def build_sanitized_schema(schema: pa.Schema) -> pa.Schema:
    new_fields = []
    for f in schema:
        new_fields.append(pa.field(f.name, target_field_type(f.name, f.type), nullable=f.nullable, metadata=f.metadata))
    return pa.schema(new_fields, metadata=schema.metadata)

def cast_batch_like_schema(batch: pa.RecordBatch, target_schema: pa.Schema) -> pa.RecordBatch:
    arrays = []
    for i, field in enumerate(batch.schema):
        arr = batch.column(i)
        tgt_field = target_schema.field(i)
        if field.type.equals(tgt_field.type):
            arrays.append(arr)
            continue

        name = field.name
        # timestamp special-case (epoch micros in UINT64/DECIMAL -> TIMESTAMP(us, UTC)):
        if name == "timestamp" and pa.types.is_timestamp(tgt_field.type):
            if pa.types.is_uint64(field.type):
                arr = pc.cast(pc.cast(arr, pa.int64()), tgt_field.type)  # uint64 -> int64 -> timestamp(us, UTC)
            elif pa.types.is_decimal128(field.type) and field.type.scale == 0 and field.type.precision >= 18:
                arr = pc.cast(pc.cast(arr, pa.int64()), tgt_field.type)  # decimal(20,0) -> int64 -> timestamp
            else:
                arr = pc.cast(arr, tgt_field.type)
            arrays.append(arr)
            continue

        # generic UINT64 -> DECIMAL(20,0)
        if pa.types.is_uint64(field.type) and pa.types.is_decimal128(tgt_field.type):
            arrays.append(pc.cast(arr, tgt_field.type))
            continue

        arrays.append(pc.cast(arr, tgt_field.type))

    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)

# ---------- Dataset batch iteration (PyArrow-version safe) ----------
def yield_batches(dataset: ds.Dataset, columns, batch_rows: int) -> Iterable[pa.RecordBatch]:
    """
    Return an iterator of RecordBatch that works across PyArrow versions.
    Preference order:
      1) dataset.scanner(...).to_reader()/to_batches()
      2) dataset.scan(...).to_reader()/to_batches()
      3) dataset.to_batches(...)
      4) dataset.to_table(...).to_batches(...)
    """
    # 1) Older/most common: .scanner()
    if hasattr(dataset, "scanner"):
        scanner = dataset.scanner(columns=columns, use_threads=True, batch_size=batch_rows)
        if hasattr(scanner, "to_reader"):
            return scanner.to_reader()
        if hasattr(scanner, "to_batches"):
            return scanner.to_batches()

    # 2) Some builds expose .scan(...)
    if hasattr(dataset, "scan"):
        scanner = dataset.scan(columns=columns, use_threads=True, batch_size=batch_rows)
        if hasattr(scanner, "to_reader"):
            return scanner.to_reader()
        if hasattr(scanner, "to_batches"):
            return scanner.to_batches()

    # 3) Direct method on Dataset (available in several versions)
    if hasattr(dataset, "to_batches"):
        return dataset.to_batches(columns=columns, batch_size=batch_rows)

    # 4) Fallback: materialize a table, then batch (uses more memory)
    tbl = dataset.to_table(columns=columns, use_threads=True)
    return tbl.to_batches(max_chunksize=batch_rows)

# ---------- Aggregation ----------
def aggregate(
    in_dir: Path,
    out_file: Path,
    sanitize: bool,
    overwrite: bool,
    batch_rows: int,
    row_group_rows: int,
    compression: str,
):
    files = find_parquet_files(in_dir)
    if not files:
        raise SystemExit(f"No parquet files found under: {in_dir}")

    if out_file.exists() and not overwrite:
        raise FileExistsError(f"Output file exists: {out_file}. Use --overwrite to replace it.")

    # Build a dataset over explicit file list
    dataset = ds.dataset(files, format="parquet", partitioning=None, ignore_prefixes=[".", "_"])

    # Decide writer schema (optionally sanitized)
    base_schema = dataset.schema
    target_schema = build_sanitized_schema(base_schema) if sanitize else base_schema

    out_file.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(
        where=str(out_file),
        schema=target_schema,
        compression=compression if compression != "none" else None,
        version="2.6",
        use_dictionary=True,
    )

    try:
        batch_iter = yield_batches(dataset, columns=None, batch_rows=batch_rows)
        total_batches = 0
        total_rows = 0

        for batch in batch_iter:
            if sanitize:
                batch = cast_batch_like_schema(batch, target_schema)
            tbl = pa.Table.from_batches([batch], schema=target_schema)
            writer.write_table(tbl, row_group_size=row_group_rows)
            total_batches += 1
            total_rows += batch.num_rows

        print(f"✔ Wrote {out_file}")
        print(f"   Files read: {len(files)}")
        print(f"   Batches written: {total_batches}")
        print(f"   Rows written: {total_rows:,}")
        print(f"   Schema (output): {target_schema}")
    finally:
        writer.close()

def resolve_out_path(out_arg: Path) -> Path:
    """
    If --out points to a directory (existing or not), write to <dir>/aggregated.parquet.
    If --out points to a filename (.parquet/.prq), use that file.
    Else, treat as directory.
    """
    suffix = out_arg.suffix.lower()
    if suffix in (".parquet", ".prq"):
        out_arg.parent.mkdir(parents=True, exist_ok=True)
        return out_arg

    out_dir = out_arg
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "aggregated.parquet"
    print(f"ℹ Detected directory for --out; writing to: {out_file}")
    return out_file

def main():
    ap = argparse.ArgumentParser(
        description="Aggregate many Parquet files into ONE Parquet file (streaming)."
    )
    ap.add_argument("--in", dest="in_dir", required=True, help="Input directory to scan recursively")
    ap.add_argument(
        "--out",
        dest="out_path",
        required=True,
        help=(
            "Output path. Provide a .parquet filename to write that file, "
            "OR a directory to place 'aggregated.parquet' inside it."
        ),
    )
    ap.add_argument("--sanitize", action="store_true",
                    help="Apply UINT64→DECIMAL(20,0), and 'timestamp' (epoch micros)→TIMESTAMP(us, UTC)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")
    ap.add_argument("--batch-rows", type=int, default=250_000,
                    help="Scanner batch size (rows per batch) [default: 250k]")
    ap.add_argument("--row-group-rows", type=int, default=250_000,
                    help="Target Parquet row group size in rows [default: 250k]")
    ap.add_argument("--compression", default="snappy",
                    choices=["snappy", "zstd", "gzip", "brotli", "lz4", "none"],
                    help="Parquet compression codec [default: snappy]")
    args = ap.parse_args()

    in_dir = Path(args.in_dir).resolve()
    raw_out = Path(args.out_path).resolve()
    out_file = resolve_out_path(raw_out)

    aggregate(
        in_dir=in_dir,
        out_file=out_file,
        sanitize=args.sanitize,
        overwrite=args.overwrite,
        batch_rows=args.batch_rows,
        row_group_rows=args.row_group_rows,
        compression=args.compression,
    )

if __name__ == "__main__":
    main()
