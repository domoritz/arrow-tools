use arrow::{csv::reader::Format, csv::ReaderBuilder, error::ArrowError, ipc::writer::FileWriter};
use arrow_tools::{apply_schema_overrides, clap_comma_separated, seekable_reader::*};
use clap::{Parser, ValueHint};
use flate2::read::MultiGzDecoder;
use regex::Regex;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::io::stdout;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Seek, io::Write};

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input CSV file, stdin if not present.
    #[clap(name = "CSV", value_parser, value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file, stdout if not present.
    #[clap(name = "ARROW", value_parser, value_hint = ValueHint::AnyPath)]
    output: Option<PathBuf>,

    /// File with Arrow schema in JSON format.
    #[clap(short = 's', long, value_parser, value_hint = ValueHint::AnyPath)]
    schema_file: Option<PathBuf>,

    /// Comma separated list of Int32 columns. Use "*" or "__all__" to set Int32 as a default
    /// data type for integer columns. This parameter has a higher priority than --schema-file
    #[clap(long, value_parser=clap_comma_separated, value_name="COLUMNS")]
    i32: Option<HashSet<String>>,

    /// Comma separated list of Int64 columns. Int64 as the default data type for integer columns.
    /// This parameter has a higher priority than --schema-file
    #[clap(long, value_parser=clap_comma_separated, value_name="COLUMNS")]
    i64: Option<HashSet<String>>,

    /// Comma separated list of Float32 columns. Use "*" or "__all__" to set Float32 as a default
    /// data type for float columns. This parameter has a higher priority than --schema-file
    #[clap(long, value_parser=clap_comma_separated, value_name="COLUMNS")]
    f32: Option<HashSet<String>>,

    /// Comma separated list of Float64 columns. Float64 is the  default data type for float
    /// columns. This parameter has a higher priority than --schema-file
    #[clap(long, value_parser=clap_comma_separated, value_name="COLUMNS")]
    f64: Option<HashSet<String>>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(short, long)]
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

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short = 'n', long)]
    dry: bool,
}

fn main() -> Result<(), ArrowError> {
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

    let mut schema = match opts.schema_file {
        Some(schema_def_file_path) => {
            let schema_file = match File::open(&schema_def_file_path) {
                Ok(file) => Ok(file),
                Err(error) => Err(ArrowError::IoError(
                    format!(
                        "Error opening schema file: {schema_def_file_path:?}, message: {error}"
                    ),
                    error,
                )),
            }?;
            let schema: Result<arrow::datatypes::Schema, serde_json::Error> =
                serde_json::from_reader(schema_file);
            match schema {
                Ok(schema) => Ok(schema),
                Err(err) => Err(ArrowError::SchemaError(format!(
                    "Error reading schema json: {err}"
                ))),
            }
        }
        _ => match format.infer_schema(&mut input, opts.max_read_records) {
            Ok((schema, _size)) => Ok(schema),
            Err(error) => Err(ArrowError::SchemaError(format!(
                "Error inferring schema: {error}"
            ))),
        },
    }?;

    apply_schema_overrides(&mut schema, opts.i32, opts.i64, opts.f32, opts.f64)
        .map_err(ArrowError::SchemaError)?;

    if opts.print_schema || opts.dry {
        let json = serde_json::to_string_pretty(&schema).unwrap();
        eprintln!("Schema:\n");
        println!("{json}");
        if opts.dry {
            return Ok(());
        }
    }

    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new(schema_ref).with_format(format);

    input.rewind()?;

    let reader = builder.build(input)?;

    let output = match opts.output {
        Some(ref path) => File::create(path).map(|f| Box::new(f) as Box<dyn Write>)?,
        None => Box::new(stdout()) as Box<dyn Write>,
    };

    let mut writer = FileWriter::try_new(output, reader.schema().as_ref())?;

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error),
        }
    }

    writer.finish()
}

#[cfg(test)]
mod test;
