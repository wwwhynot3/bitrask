#![allow(unused)]

use std::ffi::{OsStr, OsString};
use std::io::{IoSlice, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{result, vec};

use super::core::{ActiveBlock, BitRecord, BitraskIndex, StillBlock};
use core::{hash, time};
use crc32fast::Hasher;
use serde::de::value;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{Error, Read},
    rc::Rc,
    str::Bytes,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Handle, Runtime};
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
#[derive(Debug)]
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
        let (active_block, inital_size) = task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(obj.read_active_block())
                .expect("Failed to read active block when initializing AsyncBlockIo")
        });
        let active_block_filename = active_block.get_block_index();
        let dir_cp = dir.to_owned();
        task::spawn(Self::run(
            dir_cp.clone(),
            PathBuf::from(format!(
                "{}/{}",
                dir_cp,
                Self::u64_to_filename(active_block_filename)
            )),
            record_receiver,
            inital_size,
            max_record_count_in_active_block,
        ));
        (obj, active_block)
    }
    async fn run(
        block_dir: String,
        active_block_path: PathBuf,
        mut record_receiver: mpsc::Receiver<BitRecord>,
        mut current_record_count_in_active_block: usize,
        max_record_count_in_active_block: usize,
    ) {
        let result: io::Result<()> = async {
            let mut active_block = OpenOptions::new()
                .create(true)
                .append(true)
                .open(active_block_path)
                .await?;
            // println!("current count: {}", current_record_count_in_active_block);
            // println!("max count: {}", max_record_count_in_active_block);
            while let Some(record) = record_receiver.recv().await {
                let len = record.len() as u64;
                // println!("Record Len: {}", len);
                let len_bytes = len.to_le_bytes();
                // println!("Record Len Bytes: {:?}", len_bytes);
                active_block.write_all(&len_bytes).await.unwrap();
                active_block.write_all(&record).await.unwrap();
                active_block.flush().await.unwrap();
                current_record_count_in_active_block += 1;
                // println!(
                //     "write! current count: {}",
                //     current_record_count_in_active_block
                // );
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
    pub fn get_timestamp() -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{:016X}", timestamp)
    }
    pub fn u64_to_filename(u: u64) -> String {
        format!("{:016X}", u)
    }
    pub fn filename_to_u64(filename: &OsStr) -> u64 {
        u64::from_str_radix(filename.to_str().unwrap(), 16)
            .expect("Failed to convert filename to u64")
    }
    /**
     * it should be the lexicographically largest filename in the directory otherwise returns "a ordered filename"
     */

    pub fn get_order_filename_with_dir(dir: &str) -> String {
        format!("{}/{}", dir, Self::get_timestamp())
    }
    pub fn get_order_filename_in_dir(&self) -> String {
        format!("{}/{}", self.block_dir, Self::get_timestamp())
    }
    pub fn get_path_from_filename(filename: &str) -> PathBuf {
        PathBuf::from(filename)
    }
    pub async fn get_active_filename_in_dir(&self) -> io::Result<PathBuf> {
        Self::get_active_filename_with_dir(&self.block_dir).await
    }

    pub async fn get_active_filename_with_dir(dir: &str) -> io::Result<PathBuf> {
        let mut entries = fs::read_dir(dir).await?;
        let mut max_filename = entries.next_entry().await?;
        if max_filename.is_none() {
            return Ok(PathBuf::from(Self::get_order_filename_with_dir(dir)));
        }
        let mut max_filename = max_filename.unwrap().path();
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry.path();
            if (max_filename.lt(&filename) && filename.ne(&PathBuf::from(format!("{}/bitrask.idx", dir)))) {
                max_filename = filename;
            }
        }
        println!("Active Filename: {:?}", max_filename);
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
    pub async fn read_still_block(&self, block_index: u64) -> io::Result<StillBlock> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(PathBuf::from(Self::u64_to_filename(block_index)))
            .await?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        let mut block = Vec::new();
        let mut ptr = 0;
        while ptr < buff.len() {
            let len = (u64::from_le_bytes(
                buff[ptr..ptr + 8]
                    .try_into()
                    .expect("Failed to convert the Record length from bytes"),
            )) as usize;
            ptr += 8;
            let record: Box<[u8]> = Box::from(&buff[ptr..ptr + len]);
            ptr += len;
            block.push(record);
        }
        Ok(block.into_boxed_slice())
    }
    pub async fn read_active_block(&self) -> io::Result<(ActiveBlock, usize)> {
        let filename = self.get_active_filename_in_dir().await?;
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(filename.clone())
            .await?;
        // println!("Reading Active Block: {:?}", filename);
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        // println!("Read Active Block: {:?}", buff);
        let mut block = Vec::new();
        let mut ptr = 0;
        while ptr < buff.len() {
            // println!("Read Record Ptr: {:?}", &buff[ptr..ptr + 8].to_vec());
            let len = (u64::from_le_bytes(buff[ptr..ptr + 8].try_into().unwrap())) as usize;
            // println!("Read Record Len: {}", len);
            ptr += 8;
            let record: Box<[u8]> = Box::from(&buff[ptr..ptr + len]);
            ptr += len;
            block.push(record);
        }
        let initial_size = block.len();
        Ok((
            ActiveBlock::with_vec(
                Self::filename_to_u64(filename.file_name().expect(&format!(
                    "读取正在写入的文件{:?}错误,请检查Bitrask数据目录中是否存在无效目录",
                    filename
                ))),
                block,
            ),
            initial_size,
        ))
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
        let index_path = PathBuf::from(format!("{}/bitrask.idx", self.block_dir));
        println!("Reading Index: {:?}", index_path,);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(index_path)
            .await?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff).await?;
        let index = match buff.len() {
            0 => BitraskIndex::new(),
            _ => bincode::deserialize(&buff).expect("Failed to deserialize index"),
        };
        println!("Reading Index Finished");
        Ok(index)
    }
}
