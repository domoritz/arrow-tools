use arrow::{csv::ReaderBuilder, error::ArrowError, ipc::writer::FileWriter};
use clap::{Clap, ValueHint};
use serde_json::to_string_pretty;
use std::io::stdout;
use std::path::PathBuf;
use std::{fs::File, io::Write};

#[derive(Clap)]
#[clap(version = "1.0", author = "Dominik Moritz <domoritz@gmail.com>")]
struct Opts {
    /// Input CSV file.
    #[clap(name = "CSV", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file, stdout if not present.
    #[clap(name = "ARROW", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    output: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present.
    #[clap(short, long)]
    max_read_records: Option<usize>,

    /// Set whether the CSV file has headers
    #[clap(short, long)]
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    #[clap(short, long, default_value = ",")]
    delimiter: char,

    /// Print the schema to stderr.
    #[clap(short, long)]
    print_schema: bool,

    /// Only print the schema
    #[clap(short='n', long)]
    dry: bool,
}

fn main() -> Result<(), ArrowError> {
    let opts: Opts = Opts::parse();

    let input = File::open(opts.input)?;

    let mut builder = ReaderBuilder::new()
        .has_header(opts.header.unwrap_or(true))
        .with_delimiter(opts.delimiter as u8);
    builder = builder.infer_schema(opts.max_read_records);

    let reader = builder.build(input)?;

    if opts.print_schema || opts.dry {
        let json = to_string_pretty(&reader.schema().to_json())?;
        eprintln!("Inferred Schema:\n{}", json);
    }

    if opts.dry {
        return Ok(());
    }

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

    Ok(())
}
