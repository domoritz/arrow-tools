use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
fn get_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("csv2parquet")?;

    let assert = cmd
        .arg("../../data/simple.csv")
        .arg("-n")
        .arg("out.parquet")
        .assert();

    assert.success().stdout(predicate::str::contains(
        r#""fields": [
    {
      "name": "a",
      "data_type": "Int64",
      "nullable": true,
      "dict_id": 0,
      "dict_is_ordered": false,
      "metadata": {}
    },
    {
      "name": "b",
      "data_type": "Boolean",
      "nullable": true,
      "dict_id": 0,
      "dict_is_ordered": false,
      "metadata": {}
    }
  ]"#,
    ));

    Ok(())
}

#[test]
fn help() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("csv2parquet")?;

    let assert = cmd.arg("--help").assert();

    assert.success().stdout(predicate::str::contains(
        "Usage: csv2parquet [OPTIONS] <CSV> <PARQUET>",
    ));

    Ok(())
}
