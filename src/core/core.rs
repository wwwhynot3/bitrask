#![allow(unused)]
use scc::HashCache;
use serde::{Deserialize, Serialize};
use tokio::{runtime::Handle, sync::RwLock, task};

use crate::core::util::{CrcCalculator, CustomCrcCalculator};
use core::{hash, time};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{Error, Read},
    rc::Rc,
    str::Bytes,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use super::util::AsyncBlockIo;

pub trait Record {
    fn new(key: &[u8], data: &[u8]) -> Self;
    fn is_valid(&self) -> bool;
    fn timestamp(&self) -> u64;
    fn crc(&self) -> u32;
    fn key(&self) -> &[u8];
    fn data(&self) -> &[u8];
}
/**
 * 使用字节数组存储数据块
 * 潜在雷点：Box<u8> 与 Vec<u8> 的区别，选型
 * 由于方法返回需要在编译时确定大小，所以使用Box
 */
pub type BitRecord = Box<[u8]>;

impl Record for BitRecord {
    fn new(key: &[u8], data: &[u8]) -> Self {
        // todo!("abstract time method to a more efficient way");
        // 误：虽然计算机一般使用小端序，小端序的计算更快，但是为了可读性，这里使用大端序
        // 使用小端序，与现代计算机保持一致
        let mut res = Vec::with_capacity(4 + 8 + 8 + 8 + key.len() + data.len());
        res.extend_from_slice(&(0 as u32).to_le_bytes()); // crc
        res.extend_from_slice(
            &(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs())
            .to_le_bytes(),
        ); // timestamp
        res.extend_from_slice(&(key.len() as u64).to_le_bytes()); // key length
        res.extend_from_slice(&(data.len() as u64).to_le_bytes()); // data length
        res.extend_from_slice(key); // key
        res.extend_from_slice(data); // data
        let crc_code = CustomCrcCalculator::calculate_crc(&res[4..]);
        res[0..4].copy_from_slice(&crc_code.to_le_bytes());
        // println!("Record: {:?}", res);
        res.into_boxed_slice()
    }
    fn is_valid(&self) -> bool {
        let calculated_crc = CustomCrcCalculator::calculate_crc(&self[4..]);
        self.crc() == calculated_crc
    }
    fn crc(&self) -> u32 {
        u32::from_le_bytes(self[0..4].try_into().unwrap())
    }
    fn timestamp(&self) -> u64 {
        u64::from_le_bytes(
            self[4..12]
                .try_into()
                .expect("record error when parse timestamp"),
        )
    }
    fn key(&self) -> &[u8] {
        let key_len = u64::from_le_bytes(self[12..20].try_into().unwrap()) as usize;
        &self[28..28 + key_len]
    }

    fn data(&self) -> &[u8] {
        let key_len = u64::from_le_bytes(self[12..20].try_into().unwrap()) as usize;
        let data_len = u64::from_le_bytes(self[20..28].try_into().unwrap()) as usize;
        &self[28 + key_len..28 + key_len + data_len]
    }
}

pub type StillBlock = Box<[BitRecord]>;
pub trait Block {
    fn get_data(&self, index: usize) -> Option<&[u8]>;
}
#[derive(Debug)]
pub struct ActiveBlock {
    block_index: u64,
    block: RwLock<Vec<BitRecord>>,
}
impl Block for StillBlock {
    fn get_data(&self, index: usize) -> Option<&[u8]> {
        let record = self
            .get(index)
            .expect("record not found, maybe index is over this still block");
        match record.is_valid() {
            true => Some(record.data()),
            false => None,
        }
    }
}
impl ActiveBlock {
    pub fn new(block_index: u64) -> Self {
        Self {
            block_index,
            block: RwLock::new(Vec::new()),
        }
    }
    pub fn with_vec(block_index: u64, block: Vec<BitRecord>) -> Self {
        let len = block.len();
        Self {
            block_index,
            block: RwLock::from(block),
        }
    }
    pub fn get_block_index(&self) -> u64 {
        self.block_index
    }
    pub async fn append_record(&mut self, record: BitRecord) -> usize {
        let mut block = self.block.write().await;
        let len = block.len();
        block.push(record);
        len
    }
    pub async fn len(&self) -> usize {
        self.block.read().await.len()
    }
    pub async fn get_data(&self, index: usize) -> Option<Box<[u8]>> {
        let record = self.block.read().await;
        let record = record
            .get(index)
            .expect("record not found, maybe index is over this active block");
        match record.is_valid() {
            true => Some(Box::from(record.data())),
            false => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RecordPointer {
    block_index: u64,
    record_index: usize,
    timestamp: u64,
}
impl RecordPointer {
    fn new(block_index: u64, record_index: usize, timestamp: u64) -> Self {
        Self {
            block_index,
            record_index,
            timestamp,
        }
    }
}
pub type BitraskIndex = scc::HashMap<Box<[u8]>, RecordPointer>;
#[derive(Debug)]
pub struct BitraskManager {
    active_block: ActiveBlock,
    still_block_cache: HashCache<u64, StillBlock>,
    index: BitraskIndex,
    io: AsyncBlockIo,
}
impl BitraskManager {
    pub fn from_dir(
        dir: &str,
        channel_buffer_size: usize,
        block_cache_size: usize,
        max_record_count_in_active_block: usize,
    ) -> Self {
        let (io, active_block) =
            AsyncBlockIo::from(dir, channel_buffer_size, max_record_count_in_active_block);
        let index = task::block_in_place(|| Handle::current().block_on(io.read_index()))
            .expect("failed to read birask.idx");
        Self {
            active_block: active_block,
            still_block_cache: HashCache::with_capacity(0, block_cache_size),
            index,
            io,
        }
    }
    pub async fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let ptr = self.index.get(key)?;
        let ptr = ptr.get();
        // 检测是否在active_block中
        if ptr.block_index == self.active_block.get_block_index() {
            return self.active_block.get_data(ptr.record_index).await;
        }
        // 检测是否在still_block_cache中
        if let Some(block) = self.still_block_cache.get(&ptr.block_index) {
            return Some(Box::from(block.get().get(ptr.record_index)?.data()));
        }
        // 从硬盘中读取,并放入still_block_cache
        let block = self
            .io
            .read_still_block(ptr.block_index)
            .await
            .expect(&format!(
                "block {} not found",
                AsyncBlockIo::u64_to_filename(ptr.block_index)
            ));
        // println!("block: {:?}", block);
        let data = Box::from(block[ptr.record_index].data());
        // println!("data: {:?}", data);
        self.still_block_cache.put_async(ptr.block_index, block);
        Some(data)
    }
    pub async fn put(&mut self, key: &[u8], data: &[u8]) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let record = BitRecord::new(key, data);
        let record_index = self.active_block.append_record(record.clone()).await;
        let block_index = self.active_block.get_block_index();
        self.io.write_record_into_active_block(record).await;
        self.index.insert(
            Box::from(key),
            RecordPointer::new(block_index, record_index, timestamp),
        );
    }
    pub fn print_index(&self) {
        println!("index size: {}", self.index.len());
        // self.index.scan(|key, ptr| {
        //     println!(
        //         "key: {:?}, block_index: {}, record_index: {}, timestamp: {}",
        //         key, ptr.block_index, ptr.record_index, ptr.timestamp
        //     );
        // });
    }
}
impl Drop for BitraskManager {
    fn drop(&mut self) {
        task::block_in_place(|| {
            Handle::current().block_on(self.io.write_index(&self.index));
        });
    }
}
