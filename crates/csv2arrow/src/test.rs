use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
fn get_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("csv2arrow")?;

    let assert = cmd.arg("../../data/simple.csv").arg("-n").assert();

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
    let mut cmd = Command::cargo_bin("csv2arrow")?;

    let assert = cmd.arg("../../data/simple.csv.gz").arg("-n").assert();

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
    let mut cmd = Command::cargo_bin("csv2arrow")?;

    let assert = cmd
        .arg("../../data/simple.csv.gz")
        .arg("--i32=__all__")
        .arg("-n")
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
    let mut cmd = Command::cargo_bin("csv2arrow")?;

    let assert = cmd
        .arg("../../data/simple.csv.gz")
        .arg("--f32=a")
        .arg("-n")
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
    let mut cmd = Command::cargo_bin("csv2arrow")?;

    let assert = cmd.arg("--help").assert();

    assert
        .success()
        .stdout(predicate::str::contains(if cfg!(windows) {
            "Usage: csv2arrow.exe [OPTIONS] <CSV> [ARROW]"
        } else {
            "Usage: csv2arrow [OPTIONS] <CSV> [ARROW]"
        }));

    Ok(())
}
