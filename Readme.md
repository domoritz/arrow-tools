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
Usage: csv2parquet [OPTIONS] <CSV> <PARQUET>

Arguments:
  <CSV>      Input CSV file
  <PARQUET>  Output file

Options:
  -s, --schema-file <SCHEMA_FILE>
          File with Arrow schema in JSON format
      --max-read-records <MAX_READ_RECORDS>
          The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed
      --header <HEADER>
          Set whether the CSV file has headers [possible values: true, false]
  -d, --delimiter <DELIMITER>
          Set the CSV file's column delimiter as a byte character [default: ,]
  -c, --compression <COMPRESSION>
          Set the compression [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd]
  -e, --encoding <ENCODING>
          Sets encoding for any column [possible values: plain, rle, bit-packed, delta-binary-packed, delta-length-byte-array, delta-byte-array, rle-dictionary]
      --data-pagesize-limit <DATA_PAGESIZE_LIMIT>
          Sets data page size limit
      --dictionary-pagesize-limit <DICTIONARY_PAGESIZE_LIMIT>
          Sets dictionary page size limit
      --write-batch-size <WRITE_BATCH_SIZE>
          Sets write batch size
      --max-row-group-size <MAX_ROW_GROUP_SIZE>
          Sets max size for a row group
      --created-by <CREATED_BY>
          Sets "created by" property
      --dictionary
          Sets flag to enable/disable dictionary encoding for any column
      --statistics <STATISTICS>
          Sets flag to enable/disable statistics for any column [possible values: none, chunk, page]
      --max-statistics-size <MAX_STATISTICS_SIZE>
          Sets max statistics size for any column. Applicable only if statistics are enabled
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

## Examples

### Convert a CSV to Parquet
```bash
csv2parquet data.csv data.parquet
```

### Convert a CSV with no `header` to Parquet
```bash
csv2parquet --header false <CSV> <PARQUET>
```

### Get the `schema` from a CSV with header
```bash
csv2parquet --header true --dry <CSV> <PARQUET>
```

### Convert a CSV using `schema-file` to Parquet

Below is an example of the `schema-file` content:

```json
{
  "fields": [
    {
      "name": "col1",
      "data_type": "Utf8",
      "nullable": false,
      "dict_id": 0,
      "dict_is_ordered": false,
      "metadata": {}
    },
    {
      "name": " col2",
      "data_type": "Utf8",
      "nullable": false,
      "dict_id": 0,
      "dict_is_ordered": false,
      "metadata": {}
    }
  ],
  " metadata": {}
}
```

Then add the schema-file `schema.json` in the command:
```
csv2parquet --header false --schema-file schema.json <CSV> <PARQUET>
```
