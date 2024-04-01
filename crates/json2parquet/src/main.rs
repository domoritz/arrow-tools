use arrow::json::ReaderBuilder;
use arrow::record_batch::RecordBatchReader;
use arrow_tools::seekable_reader::*;
use clap::{Parser, ValueHint};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel},
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(clap::ValueEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetCompression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
    LZ4_RAW,
}

#[derive(clap::ValueEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetEncoding {
    PLAIN,
    PLAIN_DICTIONARY,
    RLE,
    RLE_DICTIONARY,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    BYTE_STREAM_SPLIT,
}

#[derive(clap::ValueEnum, Clone)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum ParquetEnabledStatistics {
    None,
    Chunk,
    Page,
}

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input JSON file, stdin if not present.
    #[clap(name = "JSON", value_parser, value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file.
    #[clap(name = "PARQUET", value_parser, value_hint = ValueHint::AnyPath)]
    output: PathBuf,

    /// File with Arrow schema in JSON format.
    #[clap(short = 's', long, value_parser, value_hint = ValueHint::AnyPath)]
    schema_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(long)]
    max_read_records: Option<usize>,

    /// Set the compression.
    #[clap(short, long, value_parser)]
    compression: Option<ParquetCompression>,

    /// Sets encoding for any column.
    #[clap(short, long, value_parser)]
    encoding: Option<ParquetEncoding>,

    /// Sets data page size limit.
    #[clap(long)]
    data_page_size_limit: Option<usize>,

    /// Sets dictionary page size limit.
    #[clap(long)]
    dictionary_page_size_limit: Option<usize>,

    /// Sets write batch size.
    #[clap(long)]
    write_batch_size: Option<usize>,

    /// Sets max size for a row group.
    #[clap(long)]
    max_row_group_size: Option<usize>,

    /// Sets "created by" property.
    #[clap(long)]
    created_by: Option<String>,

    /// Sets flag to enable/disable dictionary encoding for any column.
    #[clap(long)]
    dictionary: bool,

    /// Sets flag to enable/disable statistics for any column.
    #[clap(long, value_parser)]
    statistics: Option<ParquetEnabledStatistics>,

    /// Sets max statistics size for any column. Applicable only if statistics are enabled.
    #[clap(long)]
    max_statistics_size: Option<usize>,

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short = 'n', long)]
    dry: bool,
}

fn main() -> Result<(), ParquetError> {
    let opts: Opts = Opts::parse();

    let mut file = File::open(&opts.input)?;

    let input: Box<dyn SeekRead> = if file.rewind().is_ok() {
        Box::new(file)
    } else {
        Box::new(SeekableReader::from_unbuffered_reader(
            file,
            opts.max_read_records,
        ))
    };

    let mut buf_reader = BufReader::new(input);

    let schema = if let Some(schema_def_file_path) = opts.schema_file {
        let schema_file = File::open(&schema_def_file_path).map_err(|error| {
            ParquetError::General(format!(
                "Error opening schema file: {schema_def_file_path:?}, message: {error}"
            ))
        })?;
        let schema: Result<arrow::datatypes::Schema, serde_json::Error> =
            serde_json::from_reader(schema_file);
        schema.map_err(|error| ParquetError::General(format!("Error reading schema json: {error}")))
    } else {
        arrow::json::reader::infer_json_schema_from_seekable(&mut buf_reader, opts.max_read_records)
            .map_err(|err| ParquetError::General(format!("Error inferring schema: {err}")))
            .map(|result| result.0)
    }?;

    if opts.print_schema || opts.dry {
        let json = serde_json::to_string_pretty(&schema).unwrap();
        eprintln!("Schema:");
        println!("{json}");
        if opts.dry {
            return Ok(());
        }
    }

    let output = File::create(opts.output)?;

    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new(schema_ref);
    let reader = builder.build(buf_reader)?;

    let mut props = WriterProperties::builder().set_dictionary_enabled(opts.dictionary);

    if let Some(statistics) = opts.statistics {
        let statistics = match statistics {
            ParquetEnabledStatistics::Chunk => EnabledStatistics::Chunk,
            ParquetEnabledStatistics::Page => EnabledStatistics::Page,
            ParquetEnabledStatistics::None => EnabledStatistics::None,
        };

        props = props.set_statistics_enabled(statistics);
    }

    if let Some(compression) = opts.compression {
        let compression = match compression {
            ParquetCompression::UNCOMPRESSED => Compression::UNCOMPRESSED,
            ParquetCompression::SNAPPY => Compression::SNAPPY,
            ParquetCompression::GZIP => Compression::GZIP(GzipLevel::default()),
            ParquetCompression::LZO => Compression::LZO,
            ParquetCompression::BROTLI => Compression::BROTLI(BrotliLevel::default()),
            ParquetCompression::LZ4 => Compression::LZ4,
            ParquetCompression::ZSTD => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::LZ4_RAW => Compression::LZ4_RAW,
        };

        props = props.set_compression(compression);
    }

    if let Some(encoding) = opts.encoding {
        let encoding = match encoding {
            ParquetEncoding::PLAIN => Encoding::PLAIN,
            ParquetEncoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
            ParquetEncoding::RLE => Encoding::RLE,
            ParquetEncoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
            ParquetEncoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ParquetEncoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            ParquetEncoding::BYTE_STREAM_SPLIT => Encoding::BYTE_STREAM_SPLIT,
        };

        props = props.set_encoding(encoding);
    }

    if let Some(size) = opts.write_batch_size {
        props = props.set_write_batch_size(size);
    }

    if let Some(size) = opts.data_page_size_limit {
        props = props.set_data_page_size_limit(size);
    }

    if let Some(size) = opts.dictionary_page_size_limit {
        props = props.set_dictionary_page_size_limit(size);
    }

    if let Some(size) = opts.dictionary_page_size_limit {
        props = props.set_dictionary_page_size_limit(size);
    }

    if let Some(size) = opts.max_row_group_size {
        props = props.set_max_row_group_size(size);
    }

    if let Some(created_by) = opts.created_by {
        props = props.set_created_by(created_by);
    }

    if let Some(size) = opts.max_statistics_size {
        props = props.set_max_statistics_size(size);
    }

    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(props.build()))?;

    for batch in reader {
        writer.write(&batch?)?;
    }

    writer.close().map(|_| ())
}
