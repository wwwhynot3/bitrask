use std::sync::Arc;

use bitrask::core::core::BitraskManager;
use tokio::{sync::Barrier, task, time};

#[tokio::main]
async fn main() {
    // 初始化 BitraskManager
    // let mut manager = BitraskManager::from_dir("test_data", 100, 10, 100000);

    // 插入数据
    // let key = [4 as u8];
    // let data = [13 as u8];
    // manager.put(&key, &data).await;
    // manager.print_index();
    // for i in 0..1000000 {
    //     let key = u64::to_le_bytes(i as u64);
    //     let data = u64::to_le_bytes(i as u64);
    //     manager.put(&key, &data).await;
    // }
    // // 检索数据
    // let retrieved_data = manager.get(&key).await;
    // println!("{:?}", &retrieved_data.unwrap());
    // time::sleep(time::Duration::from_secs(10)).await;
    // assert_eq!(retrieved_data, Some(Box::from(data.as_slice())));
    // 初始化 BitraskManager
    let manager = Arc::new(tokio::sync::Mutex::new(BitraskManager::from_dir(
        "test_data",
        100,
        0,
        1,
    )));
    {
        manager.lock().await.print_index();
    }
    // 创建一个 Barrier 来同步多个任务的启动
    let barrier = Arc::new(Barrier::new(1));

    // 启动多个并发任务
    let mut handles = vec![];
    for i in 0..1 {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        let handle = task::spawn(async move {
            // 等待所有任务准备好
            barrier.wait().await;

            // 插入数据
            let key = u64::to_le_bytes(i as u64);
            let data = u64::to_le_bytes(i as u64);
            let mut manager: tokio::sync::MutexGuard<'_, BitraskManager> = manager.lock().await;
            manager.put(&key, &data).await;
            let retrieved_data = manager.get(&key).await;
            assert_eq!(retrieved_data, Some(Box::from(data.as_slice())));
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tokio::{sync::Barrier, task};

    #[tokio::test]
    async fn test_bitrask_concurrent_operations() {
        // 初始化 BitraskManager
        let manager = Arc::new(tokio::sync::Mutex::new(BitraskManager::from_dir(
            "test_data",
            100,
            10,
            1000,
        )));

        // 创建一个 Barrier 来同步多个任务的启动
        let barrier = Arc::new(Barrier::new(10));

        // 启动多个并发任务
        let mut handles = vec![];
        for i in 0..10 {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            let handle = task::spawn(async move {
                // 等待所有任务准备好
                barrier.wait().await;

                // 插入数据
                let key = u64::to_le_bytes(i as u64);
                let data = u64::to_le_bytes(i as u64);
                let mut manager = manager.lock().await;
                manager.put(&key, &data).await;
                let retrieved_data = manager.get(&key).await;
                assert_eq!(retrieved_data, Some(Box::from(data.as_slice())));
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            handle.await.unwrap();
        }
    }
}
