# CSV to Arrow

[![Crates.io](https://img.shields.io/crates/v/csv2arrow.svg)](https://crates.io/crates/csv2arrow)
[![Rust](https://github.com/domoritz/csv2arrow/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/csv2arrow/actions/workflows/rust.yml)

Convert CSV files to Apache Arrow. You may also be interested in [json2arrow](https://github.com/domoritz/json2arrow), [csv2parquet](https://github.com/domoritz/csv2parquet), or [json2parquet](https://github.com/domoritz/json2parquet).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/csv2arrow/releases/.

### With Cargo

```
cargo install csv2arrow
```

## Usage

```
USAGE:
    csv2arrow [OPTIONS] <CSV> [ARROW]

ARGS:
    <CSV>      Input CSV file
    <ARROW>    Output file, stdout if not present

OPTIONS:
    -d, --delimiter <DELIMITER>
            Set the CSV file's column delimiter as a byte character [default: ,]

    -h, --header <HEADER>
            Set whether the CSV file has headers

        --help
            Print help information

    -m, --max-read-records <MAX_READ_RECORDS>
            The number of records to infer the schema from. All rows if not present. Setting
            max-read-records to zero will stop schema inference and all columns will be string typed

    -n, --dry
            Only print the schema

    -p, --print-schema
            Print the schema to stderr

    -s, --schema-file <SCHEMA_FILE>
            File with Arrow schema in JSON format

    -V, --version
            Print version information
```

The --schema-file option uses the same file format as --dry and --print-schema.

## For Developers

To format the code, run

```bash
cargo clippy && cargo fmt
```
