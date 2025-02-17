use super::seekable_reader::*;
use std::fs::File;
use std::io::{Read, Seek};
use crate::clap_comma_separated;

#[test]
fn seekable_reader() {
    let mut seekable_reader =
        SeekableReader::from_unbuffered_reader(File::open("../../data/simple.csv").unwrap(), None);
    let mut reader = File::open("../../data/simple.csv").unwrap();

    let mut buf1 = vec![0; 20];
    let mut buf2 = vec![0; 20];
    seekable_reader.read_exact(&mut buf1).unwrap();
    reader.read_exact(&mut buf2).unwrap();
    assert_eq!(buf1, buf2);

    seekable_reader.rewind().unwrap();
    let mut buf3 = vec![0; 20];
    seekable_reader.read_exact(&mut buf3).unwrap();
    assert_eq!(buf3, buf2);
}

#[test]
fn test_clap_comma_separated() {
    let cols = clap_comma_separated("foo,bar,baz").unwrap();
    assert!(cols.contains(&"foo".to_string()));
    assert!(cols.contains(&"bar".to_string()));
    assert!(cols.contains(&"baz".to_string()));
}
