# JSON to Parquet

[![Crates.io](https://img.shields.io/crates/v/json2parquet.svg)](https://crates.io/crates/json2parquet)
[![Rust](https://github.com/domoritz/json2parquet/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/json2parquet/actions/workflows/rust.yml)

Convert JSON files to Apache Parquet. You may also be interested in [csv2parquet](https://github.com/domoritz/csv2parquet), [json2arrow](https://github.com/domoritz/json2arrow), or [csv2arrow](https://github.com/domoritz/csv2arrow).

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
    json2parquet [FLAGS] [OPTIONS] <JSON> <PARQUET>

ARGS:
    <JSON>       Input JSON file
    <PARQUET>    Output file

FLAGS:
        --dictionary      Sets flag to enable/disable dictionary encoding for any column
    -n, --dry             Only print the schema
    -h, --help            Prints help information
    -p, --print-schema    Print the schema to stderr
        --statistics      Sets flag to enable/disable statistics for any column
    -V, --version         Prints version information

OPTIONS:
    -c, --compression <compression>
            Set the compression [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4,
            zstd]

        --created-by <created-by>                                  Sets "created by" property
        --data-pagesize-limit <data-pagesize-limit>                Sets data page size limit
        --dictionary-pagesize-limit <dictionary-pagesize-limit>    Sets dictionary page size limit
    -e, --encoding <encoding>
            Sets encoding for any column [possible values: plain, rle, bit-packed, delta-binary-
            packed, delta-length-byte-array, delta-byte-array, rle-dictionary]

        --max-read-records <max-read-records>
            The number of records to infer the schema from. All rows if not present

        --max-row-group-size <max-row-group-size>                  Sets max size for a row group
        --max-statistics-size <max-statistics-size>
            Sets max statistics size for any column. Applicable only if statistics are enabled

        --write-batch-size <write-batch-size>                      Sets write batch size
```

## Limitations

Since we use teh Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

```json
{ "a": 42, "b": true }
{ "a": 12, "b": false }
{ "a": 7, "b": true }
```

## For Developers

To format the code, run

```bash
cargo clippy && cargo fmt
```
