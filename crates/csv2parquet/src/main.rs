use arrow::csv::{reader::Format, ReaderBuilder};
use arrow_tools::seekable_reader::*;
use clap::{Parser, ValueHint};
use flate2::read::MultiGzDecoder;
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel},
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
};
use regex::Regex;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Seek};

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
    /// Input CSV file, stdin if not present.
    #[clap(name = "CSV", value_parser, value_hint = ValueHint::AnyPath)]
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

    /// Set whether the CSV file has headers.
    #[clap(long, default_value = "true")]
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    #[clap(long)]
    delimiter: Option<char>,

    /// Specify an escape character.
    #[clap(long)]
    escape: Option<char>,

    /// Specify a custom quote character.
    #[clap(long)]
    quote: Option<char>,

    /// Specify a comment character.
    ///
    /// Lines starting with this character will be ignored
    #[clap(long)]
    comment: Option<char>,

    /// Provide a regex to match null values.
    #[clap(long)]
    null_regex: Option<Regex>,

    /// Set the compression.
    #[clap(short, long, value_enum)]
    compression: Option<ParquetCompression>,

    /// Sets encoding for any column.
    #[clap(short, long, value_enum)]
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
    dictionary: Option<bool>,

    /// Sets flag to enable/disable statistics for any column.
    #[clap(long, value_enum)]
    statistics: Option<ParquetEnabledStatistics>,

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

    let mut input: Box<dyn SeekRead> = if opts.input.extension() == Some(OsStr::new("gz")) {
        Box::new(SeekableReader::from_unbuffered_reader(
            MultiGzDecoder::new(file),
            opts.max_read_records,
        ))
    } else if file.rewind().is_ok() {
        Box::new(file)
    } else {
        Box::new(SeekableReader::from_unbuffered_reader(
            file,
            opts.max_read_records,
        ))
    };

    let mut format = Format::default();

    if let Some(header) = opts.header {
        format = format.with_header(header);
    }

    if let Some(delimiter) = opts.delimiter {
        format = format.with_delimiter(delimiter as u8);
    }

    if let Some(escape) = opts.escape {
        format = format.with_escape(escape as u8);
    }

    if let Some(quote) = opts.quote {
        format = format.with_quote(quote as u8);
    }

    if let Some(comment) = opts.comment {
        format = format.with_comment(comment as u8);
    }

    if let Some(regex) = opts.null_regex {
        format = format.with_null_regex(regex);
    }

    let schema = match opts.schema_file {
        Some(schema_def_file_path) => {
            let schema_file = match File::open(&schema_def_file_path) {
                Ok(file) => Ok(file),
                Err(error) => Err(ParquetError::General(format!(
                    "Error opening schema file: {schema_def_file_path:?}, message: {error}"
                ))),
            }?;
            let schema: Result<arrow::datatypes::Schema, serde_json::Error> =
                serde_json::from_reader(schema_file);
            match schema {
                Ok(schema) => Ok(schema),
                Err(err) => Err(ParquetError::General(format!(
                    "Error reading schema json: {err}"
                ))),
            }
        }
        _ => match format.infer_schema(&mut input, opts.max_read_records) {
            Ok((schema, _size)) => Ok(schema),
            Err(error) => Err(ParquetError::General(format!(
                "Error inferring schema: {error}"
            ))),
        },
    }?;

    if opts.print_schema || opts.dry {
        let json = serde_json::to_string_pretty(&schema).unwrap();
        eprintln!("Schema:");
        println!("{json}");
        if opts.dry {
            return Ok(());
        }
    }

    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new(schema_ref).with_format(format);

    input.rewind()?;

    let reader = builder.build(input)?;

    let output = File::create(opts.output)?;

    let mut props = WriterProperties::builder();

    if let Some(enabled) = opts.dictionary {
        props = props.set_dictionary_enabled(enabled);
    }

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

    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(props.build()))?;

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error.into()),
        }
    }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod test;
