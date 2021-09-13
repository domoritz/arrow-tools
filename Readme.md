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
USAGE:
    json2arrow [FLAGS] [OPTIONS] <JSON> [ARROW]

ARGS:
    <JSON>     Input JSON file
    <ARROW>    Output file, stdout if not present

FLAGS:
    -h, --help       Prints help information
    -v, --verbose    Print the schema to stderr
    -V, --version    Prints version information

OPTIONS:
    -m, --max-read-records <max-read-records>
            The number of records to infer the schema from. All rows if not present

```

## Limitations

Since we use the Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

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
