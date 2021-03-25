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
    csv2arrow [FLAGS] [OPTIONS] <CSV> [ARROW]

ARGS:
    <CSV>      Input CSV file
    <ARROW>    Output file, stdout if not present

FLAGS:
    -n, --dry             Only print the schema
        --help            Prints help information
    -p, --print-schema    Print the schema to stderr
    -V, --version         Prints version information

OPTIONS:
    -d, --delimiter <delimiter>
            Set the CSV file's column delimiter as a byte character [default: ,]

    -h, --header <header>                        Set whether the CSV file has headers
    -m, --max-read-records <max-read-records>
            The number of records to infer the schema from. All rows if not present
```

## For Developers

To format the code, run

```bash
cargo clippy && cargo fmt
```
