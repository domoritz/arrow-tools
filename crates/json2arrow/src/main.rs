use arrow::{error::ArrowError, ipc::writer::FileWriter, json::ReaderBuilder};
use clap::{Parser, ValueHint};
use std::io::{stdout, BufReader, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Write};

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input JSON file.
    #[clap(name = "JSON", value_parser, value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file, stdout if not present.
    #[clap(name = "ARROW", value_parser, value_hint = ValueHint::AnyPath)]
    output: Option<PathBuf>,

    /// File with Arrow schema in JSON format.
    #[clap(short = 's', long, value_parser, value_hint = ValueHint::AnyPath)]
    schema_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(short, long)]
    max_read_records: Option<usize>,

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short = 'n', long)]
    dry: bool,
}

fn main() -> Result<(), ArrowError> {
    let opts: Opts = Opts::parse();

    let mut input = File::open(opts.input)?;

    let schema = match opts.schema_file {
        Some(schema_def_file_path) => {
            let schema_file = match File::open(&schema_def_file_path) {
                Ok(file) => Ok(file),
                Err(error) => Err(ArrowError::IoError(format!(
                    "Error opening schema file: {schema_def_file_path:?}, message: {error}"
                ))),
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
        _ => {
            let mut buf_reader = BufReader::new(&input);

            match arrow::json::reader::infer_json_schema(&mut buf_reader, opts.max_read_records) {
                Ok(schema) => {
                    input.rewind()?;
                    Ok(schema)
                }
                Err(error) => Err(ArrowError::SchemaError(format!(
                    "Error inferring schema: {error}"
                ))),
            }
        }
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
    let builder = ReaderBuilder::new().with_schema(schema_ref);
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
