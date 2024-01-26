#![feature(seek_seek_relative)]


mod bitcask;
use crate::error::Result;


pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
// impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}


pub trait Engine: std::fmt::Display + Send + Sync {
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where 
        Self: Sized;

    fn scan_dyn(
        &mut self,
        range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>)
    ) -> Box<dyn ScanIterator + '_>;

    fn status(&self) -> Result<Status>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    pub name: String,
    pub keys: u64,
    pub size: u64,
    pub total_disk_size: u64,
    pub live_disk_size: u64,
    pub garbage_disk_size: u64,
}