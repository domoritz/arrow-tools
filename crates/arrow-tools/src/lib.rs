//! # Arrow-tools
//! This crate serves a general util library to go along
//! with all of the crates within the arrow-tools suite.

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
            if self.pos < self.buffered_bytes {
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
