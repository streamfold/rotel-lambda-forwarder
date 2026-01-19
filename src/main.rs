use clap::Parser;
use http::Response;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use lambda_runtime::Context;
use lambda_runtime::tracing;
use lambda_runtime_api_client::{Client as LambdaClient, body::Body};
use rotel::{
    init::{
        agent::Agent,
        args::{AgentRun, Exporter},
        wait,
    },
    topology::flush_control::FlushBroadcast,
};
use rotel_lambda_forwarder::events::LambdaEvent;
use rotel_lambda_forwarder::events::LambdaPayload;
use rotel_lambda_forwarder::{
    aws_attributes::AwsAttributes,
    forward::{self},
    init::{self, logging::LoggerGuard},
    tags::TagManager,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, env, process::ExitCode, sync::Arc};
use tokio::time::timeout_at;
use tokio::{select, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, trace, warn};

pub const SENDING_QUEUE_SIZE: usize = 10;
pub const LOGS_QUEUE_SIZE: usize = 50;

#[derive(Debug, Parser)]
#[command(name = "rotel-lambda-forwarder")]
#[command(bin_name = "rotel-lambda-forwarder")]
struct Arguments {
    // This is ignored in these options, but we keep it here to avoid an error on unknown
    // options
    #[arg(long)]
    env_file: Option<String>,

    #[arg(long, global = true, env = "ROTEL_ENVIRONMENT", default_value = "dev")]
    /// Environment
    environment: String,

    #[arg(long, env = "FORWARDER_S3_BUCKET")]
    /// S3 bucket for storing cache files
    s3_bucket: Option<String>,

    #[command(flatten)]
    agent_args: Box<AgentRun>,
}

// Minimal option to allow us to parse out the env from a file
#[derive(Debug, Parser)]
#[clap(ignore_errors = true)]
struct EnvFileArguments {
    #[arg(long, env = "ROTEL_ENV_FILE")]
    env_file: Option<String>,
}

fn main() -> ExitCode {
    let start_time = Instant::now();

    let env_opt = EnvFileArguments::parse();
    if let Some(env_file) = env_opt.env_file
        && let Err(e) = init::env::load_file(&env_file)
    {
        eprintln!("Can not load envfile: {}", e);
        return ExitCode::FAILURE;
    }

    let opt = Arguments::parse();

    let guard = match init::logging::setup() {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("ERROR: failed to setup logging: {}", e);
            return ExitCode::FAILURE;
        }
    };

    match run_forwarder(
        start_time,
        guard,
        opt.agent_args,
        &opt.environment,
        opt.s3_bucket,
    ) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Failed to run agent: {}", e);
            return ExitCode::from(1);
        }
    }

    ExitCode::SUCCESS
}

#[tokio::main]
async fn run_forwarder(
    start_time: Instant,
    log_guard: LoggerGuard,
    mut agent_args: Box<AgentRun>,
    env: &str,
    s3_bucket: Option<String>,
) -> Result<(), BoxError> {
    let (logs_tx, logs_rx) = rotel::bounded_channel::bounded(LOGS_QUEUE_SIZE);

    let (flush_logs_tx, flush_logs_sub) = FlushBroadcast::new().into_parts();
    let (flush_pipeline_tx, flush_pipeline_sub) = FlushBroadcast::new().into_parts();
    let (flush_exporters_tx, flush_exporters_sub) = FlushBroadcast::new().into_parts();

    let (init_wait_tx, init_wait_rx) = tokio::sync::oneshot::channel();
    let mut agent_join_set = JoinSet::new();
    let agent_cancel = CancellationToken::new();
    {
        // We control flushing manually, so set this to zero to disable the batch timer
        agent_args.batch.batch_timeout = Duration::ZERO;

        // We disable all receivers
        agent_args.receiver = None;
        agent_args.receivers = None;

        // Catch the default no config mode and default to the blackhole exporter
        // instead of failing to start
        if agent_args.exporter.is_none()
            && agent_args.exporters.is_none()
            && agent_args.otlp_exporter.base.endpoint.is_none()
            && agent_args.otlp_exporter.base.traces_endpoint.is_none()
            && agent_args.otlp_exporter.base.metrics_endpoint.is_none()
            && agent_args.otlp_exporter.base.logs_endpoint.is_none()
        {
            // todo: We should be able to startup with no config and not fail, identify best
            // default mode.
            info!("Automatically selecting blackhole exporter due to missing endpoint configs");
            agent_args.exporter = Some(Exporter::Blackhole);
        }

        let agent = Agent::new(
            agent_args,
            HashMap::new(),
            SENDING_QUEUE_SIZE,
            env.to_string(),
        )
        .with_logs_rx(logs_rx, flush_logs_sub)
        .disable_otlp_default_receiver() // only receive from logs_rx
        .with_pipeline_flush(flush_pipeline_sub)
        .with_exporters_flush(flush_exporters_sub)
        .with_init_complete_chan(init_wait_tx);
        let token = agent_cancel.clone();
        let agent_fut = async move { agent.run(token).await };

        agent_join_set.spawn(agent_fut);
    };

    let mut flusher = forward::Flusher::new(flush_logs_tx, flush_pipeline_tx, flush_exporters_tx);

    // Initialize TagManager
    let aws_config = aws_config::load_from_env().await;
    let cw_client = aws_sdk_cloudwatchlogs::Client::new(&aws_config);

    let (s3_client, s3_bucket_name) = if let Some(bucket) = s3_bucket {
        info!(bucket = %bucket, "Initializing tag manager with S3 cache");
        (Some(aws_sdk_s3::Client::new(&aws_config)), Some(bucket))
    } else {
        (None, None)
    };

    let mut tag_manager = TagManager::new(cw_client, s3_client, s3_bucket_name);

    // Load cache from S3 if available
    if let Err(e) = tag_manager.initialize().await {
        error!(error = %e, "Failed to initialize tag manager");
        // Don't fail startup, just log the error
    }

    let mut forwarder = forward::Forwarder::new(logs_tx, tag_manager);

    init_wait_rx
        .await
        .expect("Failed to wait for agent initialization");

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "Rotel Lambda Forwarder started in {}ms",
        start_time.elapsed().as_millis()
    );

    let shutdown_hook = || async move {
        // TODO: handle any last flushing
        std::mem::drop(log_guard);
    };

    // This allows us to catch shutdown signals. It is implemented by registering
    // a fake, no event, Lambda extension that will receive shutdown events.
    lambda_runtime::spawn_graceful_shutdown_handler(shutdown_hook).await;

    // Create Lambda API client
    let lambda_config = Arc::new(lambda_runtime::Config::from_env());
    let lambda_client = LambdaClient::builder()
        .build()
        .expect("Unable to create Lambda API client");

    loop {
        trace!("Waiting for next Lambda event");

        // Get next event from Lambda runtime API
        let req = lambda_runtime_api_client::build_request()
            .method("GET")
            .uri("/2018-06-01/runtime/invocation/next")
            .body(Body::empty())
            .expect("Unable to build next event request");

        let req_fut = lambda_client.call(req);

        select! {
            biased;

            resp = req_fut => {
                let resp = match resp {
                    Ok(resp) => resp,
                    Err(e) => {
                        tracing::error!("Error calling Lambda API: {}", e);
                        return Err(e);
                    }
                };

                handle_lambda_req(resp, &mut forwarder, &mut flusher, &lambda_client, lambda_config.clone()).await?;
            },

            e = wait::wait_for_any_task(&mut agent_join_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of Rotel agent."),
                    Err(e) => return Err(e),
                }
            },
        }
    }
}

/// Inlined lambda runtime loop that doesn't require Send
/// This directly implements the event loop without using lambda_runtime::run
/// which would require Send bounds
async fn handle_lambda_req(
    response: Response<Incoming>,
    forwarder: &mut forward::Forwarder,
    flusher: &mut forward::Flusher,
    client: &LambdaClient,
    config: Arc<lambda_runtime::Config>,
) -> Result<(), BoxError> {
    let (parts, body) = response.into_parts();

    #[cfg(debug_assertions)]
    if parts.status == http::StatusCode::NO_CONTENT {
        // No events available, continue waiting
        return Ok(());
    }

    // Extract request ID from headers
    let request_id = parts
        .headers
        .get("lambda-runtime-aws-request-id")
        .and_then(|v| v.to_str().ok())
        .ok_or("Missing Lambda request ID")?
        .to_string();

    tracing::trace!(request_id = %request_id, "Received Lambda event");

    let body_bytes = body.collect().await?.to_bytes();

    let context = Context::new(&request_id, config.clone(), &parts.headers)?;

    let deadline = match lambda_deadline_to_instant(context.deadline) {
        Some(deadline) => deadline,
        None => {
            error!(request_id = %request_id, context.deadline, "Deadline already exceeded");
            send_error_response(
                client,
                &request_id,
                "DeadlineExceeded",
                "Deadline already exceeded, increase Lambda deadline",
            )
            .await?;
            return Ok(());
        }
    };

    // Deserialize the event payload
    let event: LambdaPayload = match serde_json::from_slice(&body_bytes) {
        Ok(e) => e,
        Err(e) => {
            error!(request_id = %request_id, error = %e, "Failed to deserialize event");
            send_error_response(
                client,
                &request_id,
                "DeserializationError",
                &format!("Failed to deserialize event: {}", e),
            )
            .await?;
            return Ok(());
        }
    };

    let aws_attributes = AwsAttributes::new(&context);
    let lambda_event = LambdaEvent::new(event, aws_attributes, context);

    let result = forwarder.handle_event(deadline, lambda_event).await;

    // Send response or error to Lambda API
    let acker_waiter = match result {
        Ok(acker_waiter) => {
            tracing::trace!(request_id = %request_id, "Event handled successfully");
            acker_waiter
        }
        Err(e) => {
            tracing::error!(request_id = %request_id, error = ?e, "Error handling event");
            send_error_response(client, &request_id, "HandlerError", &format!("{:?}", e)).await?;
            return Ok(());
        }
    };

    let flush_start = Instant::now();
    if let Err(e) = flusher.flush(deadline).await {
        error!(request_id = %request_id, error = ?e,
            flush_duration = ?Instant::now().duration_since(flush_start), "Failed to flush pipeline");
        send_error_response(
            client,
            &request_id,
            "FlushFailed",
            "Failed to flush pipeline",
        )
        .await?;
        return Ok(());
    } else {
        debug!(flush_duration = ?Instant::now().duration_since(flush_start), "Successfully flushed pipeline");
    }

    //
    // Wait for acknowledgement of the message and all chunks. Will timeout
    // if we hit the deadline.
    //
    let wait_start = Instant::now();
    match timeout_at(deadline.into(), acker_waiter.wait()).await {
        Ok(Ok(())) => {
            debug!(
                wait_duration = ?Instant::now().duration_since(wait_start),
                "Successfully acknowledged export"
            );

            send_success_response(client, &request_id).await
        }
        Ok(Err(e)) => {
            error!(request_id = %request_id, error = ?e, "Failed to wait for acknowledgement");
            send_error_response(
                client,
                &request_id,
                "AcknowledgementFailed",
                "Failed to wait for acknowledgement",
            )
            .await
        }
        Err(_) => {
            error!(request_id = %request_id, "Timeout waiting for acknowledgement");
            send_error_response(
                client,
                &request_id,
                "AcknowledgementTimeout",
                "Timeout waiting for acknowledgement",
            )
            .await
        }
    }
}

/// Send a success response to the Lambda runtime API
async fn send_success_response(client: &LambdaClient, request_id: &str) -> Result<(), BoxError> {
    let response_body = serde_json::json!({});
    let response_bytes = serde_json::to_vec(&response_body)?;

    let req = lambda_runtime_api_client::build_request()
        .method("POST")
        .uri(format!(
            "/2018-06-01/runtime/invocation/{}/response",
            request_id
        ))
        .header("content-type", "application/json")
        .body(Body::from(response_bytes))
        .expect("Unable to build success response");

    client.call(req).await?;
    Ok(())
}

/// Send an error response to the Lambda runtime API
async fn send_error_response(
    client: &LambdaClient,
    request_id: &str,
    error_type: &str,
    error_message: &str,
) -> Result<(), BoxError> {
    let diagnostic = lambda_runtime::Diagnostic {
        error_type: error_type.to_string(),
        error_message: error_message.to_string(),
    };
    let error_body = serde_json::to_vec(&diagnostic)?;

    let req = lambda_runtime_api_client::build_request()
        .method("POST")
        .uri(format!(
            "/2018-06-01/runtime/invocation/{}/error",
            request_id
        ))
        .header("content-type", "application/json")
        .header("lambda-runtime-function-error-type", "unhandled")
        .body(Body::from(error_body))
        .expect("Unable to build error response");

    client.call(req).await?;
    Ok(())
}

// Give ourselves a bit of time before function terminates
const DEADLINE_BACKOFF_MS: u64 = 100;

/// Convert Lambda deadline (epoch milliseconds) to Instant
/// Returns None if deadline is in the past
fn lambda_deadline_to_instant(deadline_ms: u64) -> Option<Instant> {
    if deadline_ms <= DEADLINE_BACKOFF_MS {
        error!(deadline_ms, "Invalid deadline_ms supplied");
        return None;
    }

    let adjusted_deadline_ms = deadline_ms - DEADLINE_BACKOFF_MS;

    // Lambda deadline is Unix timestamp in milliseconds
    let deadline = UNIX_EPOCH + Duration::from_millis(adjusted_deadline_ms);
    let now = SystemTime::now();

    // Get duration from now until deadline
    deadline
        .duration_since(now)
        .ok()
        .map(|duration| Instant::now() + duration)
}
