# JSON to Arrow

[![Crates.io](https://img.shields.io/crates/v/json2arrow.svg)](https://crates.io/crates/json2arrow)

Convert JSON files to Apache Arrow. This package is part of [Arrow CLI tools](https://github.com/domoritz/arrow-tools).

## Installation

### Download prebuilt binaries

You can get the latest releases from https://github.com/domoritz/arrow-tools/releases.

### With Cargo

```
cargo install json2arrow
```

## With [Cargo B(inary)Install](https://github.com/cargo-bins/cargo-binstall)

To avoid re-compilation and speed up installation, you can install this tool with `cargo binstall`:

```
cargo binstall json2arrow
```

## Usage

```
Usage: json2arrow [OPTIONS] <JSON> [ARROW]

Arguments:
  <JSON>   Input JSON file, stdin if not present
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
          Print help
  -V, --version
          Print version
```

The --schema-file option uses the same file format as --dry and --print-schema.

For examples of usage see [csv2parquet examples](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet#examples) as both programs share a similar interface.

## Limitations

Since we use the Arrow JSON loader, we are limited to what it supports. Right now, it supports JSON line-delimited files.

```json
{ "a": 42, "b": true }
{ "a": 12, "b": false }
{ "a": 7, "b": true }
```
