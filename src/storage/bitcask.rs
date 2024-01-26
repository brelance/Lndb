

use std::collections::btree_map::Values;
use std::fmt::write;
use std::fs;
use std::io::{SeekFrom, Seek, BufWriter, Write, Read, BufReader};
use std::iter::Scan;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::vec::Vec;
use fs4::FileExt;
use log::{info};
use super::Status;

use crate::error::Result;
use super::Engine;



struct BitCask {
    log: Log,
    keydir: KeyDir,
}

impl BitCask {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let keydir = log.build_keydir()?;
        Ok(Self {log, keydir})
    }

    pub fn new_with_compact(path: PathBuf, garbage_ratio: f64) -> Result<Self> {
        let mut bitcask = Self::new(path)?;
        let status = bitcask.status()?;

        if status.garbage_disk_size as f64 / status.total_disk_size as f64> garbage_ratio {
            log::info!(
                "Compacting {} to remove {:.3}MB garbage ({:.0}% of {:.3}MB)",
                bitcask.log.path.display(),
                status.garbage_disk_size / 1024 / 1024,
                garbage_ratio * 100.0,
                status.total_disk_size / 1024 / 1024
            );
            
            bitcask.compact();
        }

        Ok(bitcask)
    } 
}

impl Engine for BitCask {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        info!("Write key {:?}, value {:?}", key, value);
        let (value_pos, value_len)  = self.log.write_entry(key, Some(&*value))?;
        self.keydir.insert(key.to_vec(), (value_pos, value_len));
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value_pos, value_len)) = self.keydir.get(key) {
            Ok(Some(self.log.read_entry(*value_pos, *value_len)?))
        } else {
            Ok(None)
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
        where 
            Self: Sized {
        ScanIterator { inner: self.keydir.range(range), log: &mut self.log }
    }

    fn scan_dyn(
            &mut self,
            range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>)
        ) -> Box<dyn super::ScanIterator + '_> {
        Box::new(self.scan(range))
    }

    fn status(&self) -> Result<super::Status> {
        let keys = self.keydir.len() as u64;
        let total_disk_size = self.log.file.metadata()?.len();
        let size = self.keydir
            .iter()
            .fold(0, |size, (key, (_, value_len))|
            size + key.len() as u64 + *value_len as u64
        );
        let live_disk_size = size + 8 * keys as u64;
        let garbage_disk_size = total_disk_size - live_disk_size;
        let name = "Bitcask".to_string();
        Ok(Status {
            name,
            keys, 
            size, 
            total_disk_size, 
            live_disk_size, 
            garbage_disk_size 
        })
    }
    
}

impl BitCask {
    pub fn compact(&mut self) -> Result<()> {
        let mut temp_path = self.log.path.clone();
        temp_path.set_extension("new");

        let (mut new_log, new_keydir) = self.write_log(temp_path)?;

        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.keydir = new_keydir;
        Ok(())
    }

    fn write_log(&mut self, path: PathBuf) -> Result<(Log, KeyDir)> {
        let mut keydir = KeyDir::new();
        let mut log = Log::new(path)?;

        for (key, (value_pos, value_len)) in self.keydir.iter() {
            let value = log.read_entry(*value_pos, *value_len)?;
            let (pos, len) = log.write_entry(key, Some(&value))?;
            keydir.insert(key.to_vec(), (pos, len));
        }

        Ok((log, keydir))
    }
}


impl std::fmt::Display for BitCask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitcask")
    }
}


type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir);
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // file.try_lock_exclusive()?; use exclusive-lock
        

        Ok(Self {path, file})
    }

    fn write_entry(&mut self, key: &[u8], values: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = values.map_or(0, |v| v.len() as u32);
        let value_len_or_tombstone = values.map_or(-1, |v| v.len() as i32);
        info!("key_len {}, value_len_or_tombstone {}", key_len, value_len);
        
        let len: u32 = 4 + 4 + key_len + value_len;
        let pos = self.file.seek(SeekFrom::End(0))?;
        info!("files current position {}", pos);

        let mut w: BufWriter<&mut fs::File> = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tombstone.to_be_bytes())?;
        w.write_all(key)?;
        
        if let Some(values) = values {
            w.write_all(values)?;
        }
        
        w.flush()?;
        
        info!("current write position: {}; write length: {}", pos, len);
        Ok((pos + len as u64 - value_len as u64, value_len))
    }

    fn read_entry(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut value: Vec<u8> = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();

        let mut key_len_buf = [0u8; 4];
        let mut value_len_buf = [0u8; 4];

        let file_len = self.file.metadata()?.len();
        let mut reader = BufReader::new(&mut self.file);

        let mut pos = reader.seek(SeekFrom::Start(0))?;

        while pos < file_len {

            let result = || -> std::result::Result<(Vec<u8>, u64, Option<u32>), std::io::Error> {
                reader.read_exact(&mut key_len_buf)?;
                let key_len = u32::from_be_bytes(key_len_buf);

                reader.read_exact(&mut value_len_buf)?;
                let value_len_or_tombstone =  match i32::from_be_bytes(value_len_buf) {
                    l if l >= 0 => Some(l as u32),
                    _ => None
                };

                let value_pos = pos + 4 + 4 + key_len as u64;
                let mut key = vec![0; key_len as usize];
                reader.read_exact(&mut key);
                if let Some(value_len) = value_len_or_tombstone{
                    if value_len as u64 + value_pos > file_len {
                        return Err(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "value extends beyond end of file",
                            )
                        );
                    }
                    reader.seek_relative(value_len as i64)?;
                }

                Ok((key, value_pos, value_len_or_tombstone))
                
            }();

            match result {
                Ok((key, value_pos, Some(value_len))) => {
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }

                Ok((key, value_pos, None)) => {
                    keydir.remove(&key);
                    pos = value_pos;
                }

                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // log::error 
                    self.file.set_len(pos)?;
                    break;
                }

                Err(err) => return Err(err.into()),
            }
            
        }
        Ok(keydir)

    }

}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec <u8>, (u64, u32)>,
    log: &'a mut Log,
}


impl <'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (value_pos, value_len)) = item;
        Ok((key.clone(), self.log.read_entry(*value_pos, *value_len)?))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item: (&Vec<u8>, &(u64, u32))| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item|self.map(item))
    }
}

impl<'a> super::ScanIterator for ScanIterator<'a> {}

#[cfg(test)]
mod tests {
    use std::{path::Path, env, borrow::BorrowMut};
    use super::*;
    
    use tempdir::{self, TempDir};
    const TEST_DIR: &str = "src/storage/bitcask/test/";

    
    #[test]
    fn setup_log() -> Result<()> {
        
        let mut s: BitCask = BitCask::new(PathBuf::from(TEST_DIR).join("setup_log_test"))?;
        s.set(b"b", vec![0x01])?;
        s.set(b"b", vec![0x02])?;

        s.set(b"e", vec![0x05])?;
        s.delete(b"e")?;

        s.set(b"c", vec![0x00])?;
        s.delete(b"c")?;
        s.set(b"c", vec![0x03])?;

        s.set(b"", vec![])?;

        s.set(b"a", vec![0x01])?;

        s.delete(b"f")?;

        s.delete(b"d")?;
        s.set(b"d", vec![0x04])?;

        // Make sure the scan yields the expected results.
        assert_eq!(
            vec![
                (b"".to_vec(), vec![]),
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"c".to_vec(), vec![0x03]),
                (b"d".to_vec(), vec![0x04]),
            ],
            s.scan(..).collect::<Result<Vec<_>>>()?,
        );

        Ok(())
    }

    #[test]
    fn test_iterator_and_set() -> Result<()> {
        let temp_dir = TempDir::new("bitcask_test")
        .expect("Failed to create temporary directory");
        let temp_dir_path = temp_dir.path().join("set_test");

        let mut s: BitCask = BitCask::new(PathBuf::from(temp_dir_path))?;
        s.set(b"a", vec![0x01])?;
        s.set(b"b", vec![0x02])?;

        s.set(b"c", vec![0x03])?;

        assert_eq!(
            vec![
                vec![0x01],
                vec![0x02],
                vec![0x03],
            ],
            vec![
                s.get(b"a")?.unwrap(),
                s.get(b"b")?.unwrap(),
                s.get(b"c")?.unwrap(),
            ]
        );

        assert_eq!(
            vec![
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"c".to_vec(), vec![0x03]),
            ],
            s.scan(..).collect::<Result<Vec<_>>>()?
        );

        Ok(())

    }
    
    #[test]
    fn test_delete() -> Result<()> {
    //     let temp_dir = TempDir::new("bitcask_test")
    //     .expect("Failed to create temporary directory");
    //     let temp_dir_path: PathBuf = temp_dir.path().join("set_test");
        let mut s: BitCask = BitCask::new(PathBuf::from(TEST_DIR).join("delete_test_1"))?;
        s.set(b"a", vec![0x01])?;
        s.set(b"b", vec![0x02])?;
        s.set(b"c", vec![0x03])?;

        assert_eq!(vec![02], s.get(b"b")?.unwrap());

        s.delete(b"a")?;

        let mut t_s = BitCask::new(PathBuf::from(TEST_DIR).join("delete_test_1"))?;
        assert_eq!(None, t_s.get(b"a")?);
        assert_eq!(vec![02], t_s.get(b"b")?.unwrap());
        assert_eq!(vec![03], t_s.get(b"c")?.unwrap());

        Ok(())
    }

    #[test]
    fn test_crate() {
        use std::fs::File;

        let test_temp_dir = TEST_DIR;
        env::set_var("MY_CUSTOM_TEMP_DIR", test_temp_dir);

        let temp_dir = TempDir::new("bitcask_test")
            .expect("Failed to create temporary directory");

    // Get the path to the temporary directory
        let temp_dir_path = temp_dir.path();
        println!("Temporary directory path: {:?}", temp_dir_path);

        // Create a temporary file within the directory
        let temp_file_path = temp_dir_path.join("set_test");
        let mut temp_file = File::create(&temp_file_path)
            .expect("Failed to create temporary file");

        // Write some data to the temporary file
        let data = b"Hello, temporary world!";
        temp_file.write_all(data)
            .expect("Failed to write to temporary file");

        // Read the data back from the temporary file
        let mut contents = Vec::new();
        File::open(&temp_file_path)
            .expect("Failed to open temporary file")
            .read_to_end(&mut contents)
            .expect("Failed to read from temporary file");
        println!("Temporary file contents: {:?}", String::from_utf8_lossy(&contents));
    }
    
}
