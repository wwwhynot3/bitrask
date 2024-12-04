#![allow(unused)]

use std::ffi::OsString;
use std::io::{IoSlice, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{result, vec};

use super::core::{ActiveBlock, BitRecord, BitraskIndex, StillBlock};
use core::{hash, time};
use crc32fast::Hasher;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{Error, Read},
    rc::Rc,
    str::Bytes,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task;

pub trait CrcCalculator {
    fn calculate_crc(data: &[u8]) -> u32;
}
pub struct CustomCrcCalculator {}
impl CrcCalculator for CustomCrcCalculator {
    fn calculate_crc(data: &[u8]) -> u32 {
        //temporary implementation, using crc32fast
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}
/**
 * A Class to handle Block IO asynchronously
 */
pub struct AsyncBlockIo {
    pub block_dir: String,
    // pub active_block: File,
    pub record_sender: mpsc::Sender<BitRecord>,
    pub max_record_count_in_active_block: usize,
    // pub record_receiver: mpsc::Receiver<Box<[u8]>>,
}
impl AsyncBlockIo {
    /**
     * you should pass a valid bitrask directory
     */
    pub fn from(
        dir: &str,
        channel_buffer_size: usize,
        max_record_count_in_active_block: usize,
    ) -> (Self, ActiveBlock) {
        // create the active block file

        // start a task to write records to the active block with mpsc
        let (record_sender, mut record_receiver) = mpsc::channel::<BitRecord>(channel_buffer_size);
        let obj = Self {
            block_dir: dir.to_string(),
            record_sender,
            max_record_count_in_active_block,
        };
        let active_block = task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(obj.read_active_block())
                .expect("Failed to read active block when initializing AsyncBlockIo")
        });
        task::spawn(Self::run(
            dir.to_string(),
            record_receiver,
            active_block.len(),
            max_record_count_in_active_block,
        ));
        (obj, active_block)
    }
    async fn run(
        block_dir: String,
        mut record_receiver: mpsc::Receiver<BitRecord>,
        mut current_record_count_in_active_block: usize,
        max_record_count_in_active_block: usize,
    ) {
        let result: io::Result<()> = async {
            let mut active_block = OpenOptions::new()
                .create(true)
                .append(true)
                .open(AsyncBlockIo::get_active_filename_with_dir(&block_dir).await?)
                .await?;
            while let Some(record) = record_receiver.recv().await {
                let len = record.len() as u64;
                let len_bytes = len.to_le_bytes();
                active_block.write_all(&len_bytes).await.unwrap();
                active_block.write_all(&record).await.unwrap();
                active_block.flush().await.unwrap();
                current_record_count_in_active_block += 1;
                if current_record_count_in_active_block >= max_record_count_in_active_block {
                    drop(active_block);
                    current_record_count_in_active_block = 0;
                    let new_name = AsyncBlockIo::get_order_filename_with_dir(&block_dir);
                    // println!("renaming active block to {}", new_name);
                    active_block = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(new_name)
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            eprintln!("Failed to open or create an active block: {}", e);
        }
    }
    pub fn get_ordered_filename() -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{:016X}", timestamp)
    }
    pub fn u64_to_filename(u: u64) -> String {
        format!("{:016X}", u)
    }
    /**
     * it should be the lexicographically largest filename in the directory otherwise returns "a ordered filename"
     */
    pub async fn get_active_filename_in_dir(&self) -> io::Result<PathBuf> {
        Self::get_active_filename_with_dir(&self.block_dir).await
    }
    pub fn get_order_filename_with_dir(dir: &str) -> String {
        format!("{}/{}", dir, Self::get_ordered_filename())
    }
    pub fn get_order_filename_in_dir(&self) -> String {
        format!("{}/{}", self.block_dir, Self::get_ordered_filename())
    }
    pub async fn get_active_filename_with_dir(dir: &str) -> io::Result<PathBuf> {
        let mut entries = fs::read_dir(dir).await?;
        let mut max_filename = match entries.next_entry().await? {
            Some(entry) => entry.path(),
            None => PathBuf::from(format!("{}/{}", dir, Self::get_ordered_filename())),
        };
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry.path();
            if (max_filename.lt(&filename)) {
                max_filename = filename;
            }
        }
        Ok(max_filename)
    }
    pub async fn write_still_block(&self, block: StillBlock, filename: &str) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .await?;
        let mut len_buffers: Vec<[u8; 8]> = Vec::with_capacity(block.len());
        let mut slices: Vec<IoSlice> = Vec::with_capacity(block.len() * 2);
        for record in &block {
            let len_bytes = (record.len() as u64).to_le_bytes();
            len_buffers.push(len_bytes);
        }
        for (record, len_buffer) in block.iter().zip(len_buffers.iter()) {
            slices.push(IoSlice::new(len_buffer));
            slices.push(IoSlice::new(record));
        }
        file.write_vectored(&slices).await?;
        Ok(())
    }
    pub async fn write_record_into_active_block(
        &self,
        record: BitRecord,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<BitRecord>> {
        let tx = self.record_sender.clone();
        tx.send(record).await?;
        Ok(())
    }
    pub async fn read_still_block(&self, filename: &str) -> io::Result<StillBlock> {
        let mut file = OpenOptions::new().read(true).open(filename).await?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        let mut block = Vec::new();
        let mut ptr = 0;
        while ptr < buff.len() {
            let len = (u64::from_le_bytes(buff[ptr..ptr + 8].try_into().unwrap())) as usize;
            ptr += 8;
            let record: Box<[u8]> = Box::from(&buff[ptr..ptr + len]);
            ptr += len;
            block.push(record);
        }
        Ok(block.into_boxed_slice())
    }
    pub async fn read_active_block(&self) -> io::Result<ActiveBlock> {
        let filename = self.get_active_filename_in_dir().await?;
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(filename)
            .await?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        let mut block = Vec::new();
        let mut ptr = 0;
        while ptr < buff.len() {
            let len = (u64::from_le_bytes(buff[ptr..ptr + 8].try_into().unwrap())) as usize;
            ptr += 8;
            let record: Box<[u8]> = Box::from(&buff[ptr..ptr + len]);
            ptr += len;
            block.push(record);
        }
        Ok(block)
    }
    pub async fn write_index(&self, index: &BitraskIndex) -> io::Result<()> {
        let encode = bincode::serialize(index).unwrap();
        let filename = format!("{}/bitrask.idx", self.block_dir);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .await?;
        file.write_all(&encode).await?;
        Ok(())
    }
    pub async fn read_index(&self) -> io::Result<BitraskIndex> {
        let filename = format!("{}/bitrask.idx", self.block_dir);
        let mut file = OpenOptions::new().read(true).open(filename).await?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        match buff.len() {
            0 => Ok(BitraskIndex::new()),
            _ => {
                let index = bincode::deserialize(&buff).expect("Failed to deserialize index");
                Ok(index)
            }
        }
    }
}

pub struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: std::hash::Hash + Eq + Clone, V> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        LruCache {
            capacity,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: K) -> Option<&V> {
        if self.map.contains_key(&key) {
            self.order.retain(|k| k != &key);
            self.order.push_front(key.clone());
            self.map.get(&key)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            self.order.retain(|k| k != &key);
        } else if self.map.len() == self.capacity {
            if let Some(old_key) = self.order.pop_back() {
                self.map.remove(&old_key);
            }
        }
        self.order.push_front(key.clone());
        self.map.insert(key, value);
    }
}
