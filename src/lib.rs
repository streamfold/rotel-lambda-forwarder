pub mod aws_attributes;
pub mod events;
pub mod flowlogs;
pub mod forward;
pub mod init;
pub mod log_processors;
pub mod parse;
pub mod s3_cache;
pub mod tags;

static INIT_CRYPTO: std::sync::Once = std::sync::Once::new();
pub fn init_crypto() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("unable to initialize crypto provider")
    });
}
