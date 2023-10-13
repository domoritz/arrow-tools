# CSV to Arrow

[![Crates.io](https://img.shields.io/crates/v/csv2arrow.svg)](https://crates.io/crates/csv2arrow)

Convert CSV files to Apache Arrow. This package is part of [Arrow CLI tools](https://github.com/domoritz/arrow-tools).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/arrow-tools/releases.

### With Cargo

```
cargo install csv2arrow
```

## With [Cargo B(inary)Install](https://github.com/cargo-bins/cargo-binstall)

To avoid re-compilation and speed up installation, you can install this tool with `cargo binstall`:

```
cargo binstall csv2arrow
```

## Usage

```
Usage: csv2arrow [OPTIONS] <CSV> [ARROW]

Arguments:
  <CSV>    Input CSV file, stdin if not present
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
          Print help
  -V, --version
          Print version
```

The --schema-file option uses the same file format as --dry and --print-schema.

## Examples

For usage examples, see the [`csv2parquet` examples](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet#examples) which shares a similar interface.
