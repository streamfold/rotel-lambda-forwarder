use aws_sdk_s3::Client as S3Client;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tower::BoxError;
use tracing::{debug, info};

use crate::init_crypto;

const PROCESSORS_DIR: &str = "/tmp/log_processors";

/// Downloads log processor files from URLs specified in the FORWARDER_OTLP_LOG_PROCESSORS
/// environment variable and returns a comma-separated list of their paths.
pub async fn setup_log_processors() -> Result<Option<String>, BoxError> {
    let urls_str = match std::env::var("FORWARDER_OTLP_LOG_PROCESSORS") {
        Ok(val) if !val.is_empty() => val,
        _ => {
            debug!("FORWARDER_OTLP_LOG_PROCESSORS not set, skipping log processor setup");
            return Ok(None);
        }
    };

    info!(
        urls = %urls_str,
        "Setting up log processors from FORWARDER_OTLP_LOG_PROCESSORS"
    );

    init_crypto();

    // Parse URLs from comma-separated list
    let urls: Vec<&str> = urls_str.split(',').map(|s| s.trim()).collect();
    if urls.is_empty() {
        return Ok(None);
    }

    // Setup the processors directory
    setup_processors_directory().await?;

    // Download all processor files
    let mut processor_paths = Vec::new();
    for (index, url) in urls.iter().enumerate() {
        let processor_num = index + 1;
        let filename = format!("processor_{:02}.py", processor_num);
        let filepath = PathBuf::from(PROCESSORS_DIR).join(&filename);

        info!(
            url = %url,
            path = %filepath.display(),
            "Downloading log processor"
        );

        download_processor(url, &filepath).await?;
        processor_paths.push(filepath.to_string_lossy().to_string());
    }

    // Return comma-separated list of paths
    let paths_str = processor_paths.join(",");
    info!(
        paths = %paths_str,
        "Log processors setup complete"
    );
    Ok(Some(paths_str))
}

/// Setup the processors directory, clearing it if it exists or creating it otherwise
async fn setup_processors_directory() -> Result<(), BoxError> {
    let dir_path = Path::new(PROCESSORS_DIR);

    if dir_path.exists() {
        debug!(
            path = %dir_path.display(),
            "Processors directory exists, clearing contents"
        );
        // Remove the directory and all its contents
        fs::remove_dir_all(dir_path).await?;
    }

    // Create the directory
    fs::create_dir_all(dir_path).await?;
    debug!(
        path = %dir_path.display(),
        "Processors directory created"
    );

    Ok(())
}

/// Download a processor file from a URL (http://, https://, or s3://)
async fn download_processor(url: &str, dest_path: &PathBuf) -> Result<(), BoxError> {
    if url.starts_with("s3://") {
        download_from_s3(url, dest_path).await
    } else if url.starts_with("http://") || url.starts_with("https://") {
        download_from_http(url, dest_path).await
    } else {
        Err(format!("Unsupported URL scheme: {}", url).into())
    }
}

/// Download a file from an HTTP/HTTPS URL using hyper
async fn download_from_http(url: &str, dest_path: &PathBuf) -> Result<(), BoxError> {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("no native root CA certificates found")
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = HyperClient::builder(TokioExecutor::new()).build(https);

    let uri: hyper::Uri = url.parse()?;
    let req = hyper::Request::builder()
        .uri(uri)
        .method("GET")
        .body(Empty::<Bytes>::new())?;

    let response = client.request(req).await?;

    if !response.status().is_success() {
        return Err(format!("HTTP request failed with status: {}", response.status()).into());
    }

    let body_bytes = response.into_body().collect().await?.to_bytes();

    // Write to file
    let mut file = fs::File::create(dest_path).await?;
    file.write_all(&body_bytes).await?;
    file.flush().await?;

    debug!(
        url = %url,
        path = %dest_path.display(),
        size = body_bytes.len(),
        "Downloaded file from HTTP"
    );

    Ok(())
}

/// Download a file from S3
async fn download_from_s3(url: &str, dest_path: &PathBuf) -> Result<(), BoxError> {
    // Parse s3://bucket/key
    let s3_path = url.strip_prefix("s3://").ok_or("Invalid S3 URL")?;
    let parts: Vec<&str> = s3_path.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(format!("Invalid S3 URL format: {}", url).into());
    }

    let bucket = parts[0];
    let key = parts[1];

    debug!(
        bucket = %bucket,
        key = %key,
        "Downloading from S3"
    );

    // Create S3 client
    let aws_config = aws_config::load_from_env().await;
    let s3_client = S3Client::new(&aws_config);

    // Download the object
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    // Stream the body to file
    let mut file = fs::File::create(dest_path).await?;
    let mut byte_stream = response.body;

    let mut total_bytes = 0;
    while let Some(chunk_result) = byte_stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk).await?;
        total_bytes += chunk.len();
    }

    file.flush().await?;

    debug!(
        url = %url,
        path = %dest_path.display(),
        size = total_bytes,
        "Downloaded file from S3"
    );

    Ok(())
}
