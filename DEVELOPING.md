# Development Guide

This guide covers local development, building, and testing for rotel-lambda-forwarder.

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html)
- [Protocol Buffers Compiler (protoc)](https://grpc.io/docs/protoc-installation/)

## Building

### Development Build

To build the project for local development:

```bash
cargo lambda build
```

This creates a debug build that includes additional debugging information and faster compilation times.

### Production Build

To build the project for production deployment:

```bash
cargo lambda build --release
```

The release build is optimized for performance and produces smaller binaries.

Read more about building your lambda function in [the Cargo Lambda documentation](https://www.cargo-lambda.info/commands/build.html).

## Testing

### Unit Tests

Run the standard Rust unit tests:

```bash
cargo nextest run
```


## Makefile Commands

The project includes a Makefile with common commands:

### Build

```bash
make build
```

Runs `cargo lambda build --release`.

### Deploy

```bash
make deploy IAM_ROLE=arn:aws:iam::123456789012:role/lambda-execution-role
```

Builds and deploys the function to AWS.

## Additional Resources

- [Cargo Lambda Documentation](https://www.cargo-lambda.info/)
- [AWS Lambda Rust Runtime](https://github.com/awslabs/aws-lambda-rust-runtime)
- [Rotel Documentation](https://rotel.dev)
- [OpenTelemetry Logs Specification](https://opentelemetry.io/docs/specs/otel/logs/)
