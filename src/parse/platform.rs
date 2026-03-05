/// Represents the AWS platform/service that generated the logs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPlatform {
    Eks,
    Ecs,
    Rds,
    Lambda,
    Codebuild,
    Cloudtrail,
    VpcFlowLog,
    Unknown,
}

impl LogPlatform {
    /// Returns the platform string used in cloud.platform attribute
    pub fn as_str(&self) -> &'static str {
        match self {
            LogPlatform::Eks => "aws_eks",
            LogPlatform::Ecs => "aws_ecs",
            LogPlatform::Rds => "aws_rds",
            LogPlatform::Lambda => "aws_lambda",
            LogPlatform::Codebuild => "aws_codebuild",
            LogPlatform::Cloudtrail => "aws_cloudtrail",
            LogPlatform::VpcFlowLog => "aws_vpc_flow_log",
            LogPlatform::Unknown => "aws_unknown",
        }
    }
}

/// Represents the type of parser to use for log entries
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserType {
    Json,
    KeyValue,
    VpcLog,
    #[default]
    Unknown,
}

/// Errors that can occur during parsing
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("Failed to decode CloudWatch Logs data: {0}")]
    DecodeError(String),

    #[error("Failed to decompress CloudWatch Logs data: {0}")]
    DecompressionError(String),

    #[error("Failed to parse JSON: {0}")]
    JsonParseError(String),

    #[error("Invalid log format: {0}")]
    FormatParseError(String),

    #[error("Unable to parse EC2 flow log format")]
    FlowLogFormatError,
}
