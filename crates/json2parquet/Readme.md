# JSON to Parquet

[![Crates.io](https://img.shields.io/crates/v/json2parquet.svg)](https://crates.io/crates/json2parquet)

Convert JSON/JSONL files to [Apache Parquet](https://parquet.apache.org/). This package is part of [Arrow CLI tools](https://github.com/domoritz/arrow-tools).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/arrow-tools/releases.

### With Homebrew

```
brew install domoritz/homebrew-tap/json2parquet
```

### With Cargo

```
cargo install json2parquet
```

## With [Cargo B(inary)Install](https://github.com/cargo-bins/cargo-binstall)

To avoid re-compilation and speed up installation, you can install this tool with `cargo binstall`:

```
cargo binstall json2parquet
```

## Usage

```
Usage: json2parquet [OPTIONS] <JSON> <PARQUET>

Arguments:
  <JSON>     Input JSON file, stdin if not present
  <PARQUET>  Output file

Options:
  -s, --schema-file <SCHEMA_FILE>
          File with Arrow schema in JSON format
      --max-read-records <MAX_READ_RECORDS>
          The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed
  -c, --compression <COMPRESSION>
          Set the compression [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4-raw]
  -e, --encoding <ENCODING>
          Sets encoding for any column [possible values: plain, plain-dictionary, rle, rle-dictionary, delta-binary-packed, delta-length-byte-array, delta-byte-array, byte-stream-split]
      --data-page-size-limit <DATA_PAGE_SIZE_LIMIT>
          Sets data page size limit
      --dictionary-page-size-limit <DICTIONARY_PAGE_SIZE_LIMIT>
          Sets dictionary page size limit
      --write-batch-size <WRITE_BATCH_SIZE>
          Sets write batch size
      --max-row-group-size <MAX_ROW_GROUP_SIZE>
          Sets max size for a row group
      --created-by <CREATED_BY>
          Sets "created by" property
      --dictionary
          Sets flag to enable/disable dictionary encoding for any column
      --statistics <STATISTICS>
          Sets flag to enable/disable statistics for any column [possible values: none, chunk, page]
  -p, --print-schema
          Print the schema to stderr
  -n, --dry
          Only print the schema
  -h, --help
          Print help
  -V, --version
          Print version
```

The --schema-file option uses the same file format as --dry and --print-schema.

## Examples

For usage examples, see the [`csv2parquet` examples](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet#examples) which shares a similar interface.

## Limitations

Since we use the Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

```json
{ "a": 42, "b": true }
{ "a": 12, "b": false }
{ "a": 7, "b": true }
```
