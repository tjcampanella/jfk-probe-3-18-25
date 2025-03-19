use std::{io, sync::Arc};

use reqwest::Client;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::Semaphore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = fs::read_to_string("jfk-3-18-25-file-urls.txt").await?;

    //Limit concurrency to not get rate limited
    let semaphore = Arc::new(Semaphore::new(64));

    let client = Client::new();
    let mut handles = Vec::new();
    for line in data.lines() {
        let url = line.to_string();
        let client = client.clone();
        let semaphore = semaphore.clone();
        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            if let Err(e) = download_file(&client, &url).await {
                eprintln!("Failed to download {}: {}", url, e);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

async fn download_file(client: &Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let filename = url.split("/").last();

    if let Some(filename) = filename {
        let resp = client.get(url).send().await?;
        let bytes = resp.bytes().await?;

        let mut file = File::create(format!("jfk-files/{filename}")).await?;
        file.write_all(&bytes).await?;
    } else {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "File path cannot be empty",
        )));
    }

    Ok(())
}
