use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
fn get_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("json2parquet")?;

    let assert = cmd
        .arg("../../data/simple.json")
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
fn get_schema_gz() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("json2parquet")?;

    let assert = cmd
        .arg("../../data/simple.json.gz")
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
fn apply_schema1() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("json2parquet")?;

    let assert = cmd
        .arg("../../data/simple.json.gz")
        .arg("--i32=__all__")
        .arg("-n")
        .arg("out.parquet")
        .assert();

    assert.success().stdout(predicate::str::contains(
        r#""fields": [
    {
      "name": "a",
      "data_type": "Int32",
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
fn apply_schema2() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("json2parquet")?;

    let assert = cmd
        .arg("../../data/simple.json.gz")
        .arg("--f32=a")
        .arg("-n")
        .arg("out.parquet")
        .assert();

    assert.success().stdout(predicate::str::contains(
        r#""fields": [
    {
      "name": "a",
      "data_type": "Float32",
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
    let mut cmd = Command::cargo_bin("json2parquet")?;

    let assert = cmd.arg("--help").assert();

    assert
        .success()
        .stdout(predicate::str::contains(if cfg!(windows) {
            "Usage: json2parquet.exe [OPTIONS] <JSON> <PARQUET>"
        } else {
            "Usage: json2parquet [OPTIONS] <JSON> <PARQUET>"
        }));

    Ok(())
}
