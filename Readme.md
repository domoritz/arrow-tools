# JSON to Parquet

[![Crates.io](https://img.shields.io/crates/v/json2parquet.svg)](https://crates.io/crates/json2parquet)
[![Rust](https://github.com/domoritz/json2parquet/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/json2parquet/actions/workflows/rust.yml)

Convert JSON files to [Apache Parquet](https://parquet.apache.org/). You may also be interested in [csv2parquet](https://github.com/domoritz/csv2parquet), [json2arrow](https://github.com/domoritz/json2arrow), or [csv2arrow](https://github.com/domoritz/csv2arrow).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/json2parquet/releases/.

### With Cargo

```
cargo install json2parquet
```

## Usage

```
USAGE:
    json2parquet [OPTIONS] <JSON> <PARQUET>

ARGS:
    <JSON>       Input JSON file
    <PARQUET>    Output file

OPTIONS:
    -c, --compression <COMPRESSION>
            Set the compression [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4,
            zstd]

        --created-by <CREATED_BY>
            Sets "created by" property

        --data-pagesize-limit <DATA_PAGESIZE_LIMIT>
            Sets data page size limit

        --dictionary
            Sets flag to enable/disable dictionary encoding for any column

        --dictionary-pagesize-limit <DICTIONARY_PAGESIZE_LIMIT>
            Sets dictionary page size limit

    -e, --encoding <ENCODING>
            Sets encoding for any column [possible values: plain, rle, bit-packed,
            delta-binary-packed, delta-length-byte-array, delta-byte-array, rle-dictionary]

    -h, --help
            Print help information

        --max-read-records <MAX_READ_RECORDS>
            The number of records to infer the schema from. All rows if not present. Setting
            max-read-records to zero will stop schema inference and all columns will be string typed

        --max-row-group-size <MAX_ROW_GROUP_SIZE>
            Sets max size for a row group

        --max-statistics-size <MAX_STATISTICS_SIZE>
            Sets max statistics size for any column. Applicable only if statistics are enabled

    -n, --dry
            Only print the schema

    -p, --print-schema
            Print the schema to stderr

    -s, --schema-file <SCHEMA_FILE>
            File with Arrow schema in JSON format

        --statistics <STATISTICS>
            Sets flag to enable/disable statistics for any column [possible values: none, chunk,
            page]

    -V, --version
            Print version information

        --write-batch-size <WRITE_BATCH_SIZE>
            Sets write batch size
```

The --schema-file option uses the same file format as --dry and --print-schema.

## Limitations

Since we use the Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

```json
{ "a": 42, "b": true }
{ "a": 12, "b": false }
{ "a": 7, "b": true }
```
