use std::env;

use tower::BoxError;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry, filter::LevelFilter};

pub type LoggerGuard = tracing_appender::non_blocking::WorkerGuard;

// todo: match logging to the recommended lambda extension approach
pub fn setup() -> Result<LoggerGuard, BoxError> {
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?
        .add_directive("opentelemetry=warn".parse()?)
        .add_directive("opentelemetry_sdk=warn".parse()?);

    let is_json = env::var("AWS_LAMBDA_LOG_FORMAT")
        .unwrap_or_default()
        .to_uppercase()
        == "JSON";

    let layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_writer)
        // disable printing of the module
        .with_target(false)
        // cloudwatch will add time
        .without_time()
        // cloudwatch doesn't play nice with escape codes
        .with_ansi(false);

    if is_json {
        let file_layer = layer.json();

        let subscriber = Registry::default().with(filter).with(file_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    } else {
        let file_layer = layer.compact();

        let subscriber = Registry::default().with(filter).with(file_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }

    Ok(guard)
}
