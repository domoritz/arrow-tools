//! # Arrow-tools
//! This crate serves a general util library to go along
//! with all of the crates within the arrow-tools suite.

use arrow_schema::{DataType, Field, Fields, Schema};
use std::collections::{HashMap, HashSet};

pub mod seekable_reader {
    use std::fs;
    use std::io;

    /// A trait for a reader that can seek to a position
    pub trait SeekRead: io::Read + io::Seek {}

    pub struct SeekableReader<R> {
        inner: R,        // underlying reader
        buffer: Vec<u8>, // buffer for the first n lines
        buffered_bytes: usize,
        pos: usize,     // current position in the buffer
        seekable: bool, // whether seek is still possible
    }

    impl SeekRead for fs::File {}
    impl SeekRead for SeekableReader<fs::File> {}
    impl SeekRead for SeekableReader<flate2::read::MultiGzDecoder<fs::File>> {}

    const BUFFER_SIZE: usize = 8192;
    impl<R: std::io::Read> SeekableReader<R> {
        pub fn from_unbuffered_reader(reader: R, lines_to_buffer: Option<usize>) -> Self {
            let mut inner = reader;
            let mut buffer = Vec::<u8>::with_capacity(BUFFER_SIZE);
            let mut lines = 0;
            let mut bytes_read = 0;
            loop {
                let bytes_before = bytes_read;
                buffer.append(&mut vec![0; BUFFER_SIZE - (buffer.len() - bytes_read)]);
                bytes_read += inner
                    .read(&mut buffer[bytes_read..bytes_read + BUFFER_SIZE])
                    .unwrap();
                lines += buffer[bytes_before..bytes_read]
                    .iter()
                    .filter(|&&x| x == 10)
                    .count();
                if let Some(lines_to_buffer) = lines_to_buffer {
                    // +1 because there may be a header
                    if lines > lines_to_buffer + 1 {
                        break;
                    }
                }
                if bytes_read - bytes_before == 0 {
                    break;
                }
            }
            SeekableReader {
                inner,
                buffer,
                buffered_bytes: bytes_read,
                pos: 0,
                seekable: true,
            }
        }
    }

    impl<R: std::io::Read> std::io::Read for SeekableReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
            let buf_len = buf.len();
            if self.pos <= self.buffered_bytes {
                if self.buffered_bytes - self.pos < buf_len {
                    buf[..self.buffered_bytes - self.pos]
                        .copy_from_slice(&self.buffer[self.pos..self.buffered_bytes]);
                    let len_read = self.buffered_bytes - self.pos;
                    self.pos = self.buffered_bytes;
                    Ok(len_read)
                } else {
                    buf.copy_from_slice(&self.buffer[self.pos..self.pos + buf_len]);
                    self.pos += buf_len;
                    Ok(buf_len)
                }
            } else {
                self.seekable = false;
                self.inner.read(buf)
            }
        }
    }

    impl<R: io::Read> io::Seek for SeekableReader<R> {
        fn seek(&mut self, pos: io::SeekFrom) -> Result<u64, io::Error> {
            let error = Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seeking outside of buffer, please report to https://github.com/domoritz/arrow-tools/issues/new".to_string(),
            ));
            if self.seekable {
                match pos {
                    io::SeekFrom::Start(pos) => {
                        if pos >= self.buffered_bytes as u64 {
                            error
                        } else {
                            self.pos = pos as usize;
                            Ok(pos)
                        }
                    }
                    io::SeekFrom::Current(pos) => {
                        let new_pos = self.pos as i64 + pos;
                        if 0 <= new_pos && new_pos < self.buffered_bytes as i64 {
                            self.pos = new_pos as usize;
                            Ok(new_pos as u64)
                        } else {
                            error
                        }
                    }
                    io::SeekFrom::End(_) => error,
                }
            } else {
                error
            }
        }
    }
}

/// A clap helper function to parse comma separated columns as a HashSet
pub fn clap_comma_separated(arg: &str) -> Result<HashSet<String>, String> {
    if arg.is_empty() {
        Ok(HashSet::new())
    } else {
        let mut hashset = HashSet::new();
        for s in arg.split(',') {
            hashset.insert(s.to_string());
        }
        Ok(hashset)
    }
}

/// Applies user-provided data types to the schema
pub fn apply_schema_overrides(
    schema: &mut Schema,
    i32_cols: Option<HashSet<String>>,
    i64_cols: Option<HashSet<String>>,
    f32_cols: Option<HashSet<String>>,
    f64_cols: Option<HashSet<String>>,
) -> Result<(), String> {
    if i32_cols.is_none() && i64_cols.is_none() && f32_cols.is_none() && f64_cols.is_none() {
        // There is no need to make any changes to the current scheme
        return Ok(());
    }

    let mut default_int_type = DataType::Int64;
    let mut default_float_type = DataType::Float64;

    let i32_cols = i32_cols.unwrap_or_default();
    let i64_cols = i64_cols.unwrap_or_default();
    let f32_cols = f32_cols.unwrap_or_default();
    let f64_cols = f64_cols.unwrap_or_default();

    if i32_cols.contains("*") && i64_cols.contains("*") {
        return Err("i32 and i64 can't both be the default data types for integers".to_string());
    }
    if f32_cols.contains("*") && f64_cols.contains("*") {
        return Err("f32 and f64 can't both be the default data types for floats".to_string());
    }

    let mut overrides = HashMap::<String, DataType>::new();
    for c in i32_cols {
        if c == "*" || c == "__all__" {
            default_int_type = DataType::Int32;
            break;
        }
        overrides.insert(c, DataType::Int32);
    }
    for c in i64_cols {
        if c == "*" || c == "__all__" {
            default_int_type = DataType::Int64;
            break;
        }
        overrides.insert(c, DataType::Int64);
    }
    for c in f32_cols {
        if c == "*" || c == "__all__" {
            default_float_type = DataType::Float32;
            break;
        }
        overrides.insert(c, DataType::Float32);
    }
    for c in f64_cols {
        if c == "*" || c == "__all__" {
            default_float_type = DataType::Float64;
            break;
        }
        overrides.insert(c, DataType::Float64);
    }

    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let name = field.name();
        if let Some(datatype) = overrides.remove(name) {
            new_fields.push(Field::new(name, datatype, field.is_nullable()));
        } else {
            match field.data_type() {
                &DataType::Int64 => new_fields.push(Field::new(
                    name,
                    default_int_type.clone(),
                    field.is_nullable(),
                )),
                &DataType::Float64 => new_fields.push(Field::new(
                    name,
                    default_float_type.clone(),
                    field.is_nullable(),
                )),
                datatype => {
                    new_fields.push(Field::new(name, datatype.clone(), field.is_nullable()))
                }
            }
        }
    }
    schema.fields = Fields::from(new_fields);
    Ok(())
}

#[cfg(test)]
mod test;
