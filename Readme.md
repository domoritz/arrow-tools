# CSV to Parquet

[![Crates.io](https://img.shields.io/crates/v/csv2parquet.svg)](https://crates.io/crates/csv2parquet)
[![Rust](https://github.com/domoritz/csv2parquet/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/csv2parquet/actions/workflows/rust.yml)

Convert CSV files to [Apache Parquet](https://parquet.apache.org/). You may also be interested in [json2parquet](https://github.com/domoritz/json2parquet), [csv2arrow](https://github.com/domoritz/csv2arrow), or [json2arrow](https://github.com/domoritz/json2arrow).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/csv2parquet/releases/.

### With Cargo

```
cargo install csv2parquet
```

## Usage

```
USAGE:
    csv2parquet [FLAGS] [OPTIONS] <CSV> <PARQUET>

ARGS:
    <CSV>        Input CSV file
    <PARQUET>    Output file

FLAGS:
        --dictionary      Sets flag to enable/disable dictionary encoding for any column
    -n, --dry             Only print the schema
        --help            Prints help information
    -p, --print-schema    Print the schema to stderr
        --statistics      Sets flag to enable/disable statistics for any column
    -V, --version         Prints version information

OPTIONS:
    -c, --compression <compression>
            Set the compression [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4,
            zstd]

        --created-by <created-by>                                  Sets "created by" property
        --data-pagesize-limit <data-pagesize-limit>                Sets data page size limit
    -d, --delimiter <delimiter>
            Set the CSV file's column delimiter as a byte character [default: ,]

        --dictionary-pagesize-limit <dictionary-pagesize-limit>    Sets dictionary page size limit
    -e, --encoding <encoding>
            Sets encoding for any column [possible values: plain, rle, bit-packed, delta-binary-
            packed, delta-length-byte-array, delta-byte-array, rle-dictionary]

    -h, --header <header>
            Set whether the CSV file has headers

        --max-read-records <max-read-records>
            The number of records to infer the schema from. All rows if not present.
            Setting max-read-records to zero will stop schema inference.  All columns
            will be string typed.

        --max-row-group-size <max-row-group-size>                  Sets max size for a row group
        --max-statistics-size <max-statistics-size>
            Sets max statistics size for any column. Applicable only if statistics are enabled

        --write-batch-size <write-batch-size>                      Sets write batch size
```

## For Developers

To format the code, run

```bash
cargo clippy && cargo fmt
```
