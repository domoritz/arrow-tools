# JSON to Arrow

[![Crates.io](https://img.shields.io/crates/v/json2arrow.svg)](https://crates.io/crates/json2arrow)
[![Rust](https://github.com/domoritz/json2arrow/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/json2arrow/actions/workflows/rust.yml)

Convert JSON files to Apache Arrow. You may also be interested in [csv2arrow](https://github.com/domoritz/csv2arrow), [json2parquet](https://github.com/domoritz/json2parquet), or [csv2parquet](https://github.com/domoritz/csv2parquet).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/json2arrow/releases/.

### With Cargo

```
cargo install json2arrow
```

## Usage

```
Usage: json2arrow [OPTIONS] <JSON> [ARROW]

Arguments:
  <JSON>   Input JSON file
  [ARROW]  Output file, stdout if not present

Options:
  -s, --schema-file <SCHEMA_FILE>
          File with Arrow schema in JSON format
  -m, --max-read-records <MAX_READ_RECORDS>
          The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed
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

## Limitations

Since we use the Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

```json
{ "a": 42, "b": true }
{ "a": 12, "b": false }
{ "a": 7, "b": true }
```
