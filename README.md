# Arrow CLI Tools

[![Rust](https://github.com/domoritz/arrow-tools/actions/workflows/rust.yml/badge.svg)](https://github.com/domoritz/arrow-tools/actions/workflows/rust.yml)

A collection of handy CLI tools to convert CSV and JSON to [Apache Arrow](https://arrow.apache.org) and [Parquet](https://parquet.apache.org).

This repository contains five projects:
* [`csv2arrow`](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2arrow) to convert CSV files to Apache Arrow.
* [`csv2parquet`](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet) to convert CSV files to Parquet.
* [`json2arrow`](https://github.com/domoritz/arrow-tools/tree/main/crates/json2arrow) to convert JSON files to Apache Arrow.
* [`json2parquet`](https://github.com/domoritz/arrow-tools/tree/main/crates/json2parquet) to convert JSON files to Parquet.
* [`arrow-tools`](https://github.com/domoritz/arrow-tools/tree/main/crates/arrow-tools) shared utilities used by the other four packages.

For usage examples, see the [`csv2parquet` examples](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet#examples).

[Homebrew](https://brew.sh) formulas are pushed to [domoritz/homebrew-tap](https://github.com/domoritz/homebrew-tap) for every release.
