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
Usage: csv2arrow [OPTIONS] <CSV> [ARROW]

Arguments:
  <CSV>    Input CSV file
  [ARROW]  Output file, stdout if not present

Options:
  -s, --schema-file <SCHEMA_FILE>
          File with Arrow schema in JSON format
  -m, --max-read-records <MAX_READ_RECORDS>
          The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed
      --header <HEADER>
          Set whether the CSV file has headers [possible values: true, false]
  -d, --delimiter <DELIMITER>
          Set the CSV file's column delimiter as a byte character [default: ,]
  -p, --print-schema
          Print the schema to stderr
  -n, --dry
          Only print the schema
  -h, --help
          Print help information
  -V, --version
          Print version information
```

The --schema-file option uses the same file format as --dry and --print-schema.
