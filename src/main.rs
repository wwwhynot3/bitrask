use tokio::time;

use bitrask::core::core::{AppendableBlock, BitRecord, Record};
use bitrask::core::util::AsyncBlockIo;
#[tokio::main]
async fn main() {
    let (io, mut active_block) = AsyncBlockIo::from("./data", 100, 100000);
    for i in 0..1000_000 {
        let i = i as u64;
        let record = BitRecord::new(&i.to_le_bytes(), &i.to_ne_bytes());
        active_block.append_record(record.clone());
        io.write_record_into_active_block(record).await.unwrap();
    }
    time::sleep(time::Duration::from_secs(3)).await;
}
