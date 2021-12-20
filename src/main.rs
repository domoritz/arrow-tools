use arrow::{error::ArrowError, ipc::writer::FileWriter, json::ReaderBuilder};
use clap::{Parser, ValueHint};
use serde_json::to_string_pretty;
use std::io::stdout;
use std::path::PathBuf;
use std::{fs::File, io::Write};

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input JSON file.
    #[clap(name = "JSON", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file, stdout if not present.
    #[clap(name = "ARROW", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    output: Option<PathBuf>,

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

    let input = File::open(opts.input)?;
    let builder = ReaderBuilder::new().infer_schema(opts.max_read_records);
    let reader = builder.build(input)?;

    if opts.print_schema || opts.dry {
        let json = to_string_pretty(&reader.schema().to_json())?;
        eprintln!("Inferred Schema:\n{}", json);

        if opts.dry {
            return Ok(());
        }
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
