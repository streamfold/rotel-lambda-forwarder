FROM ubuntu:24.04

ARG TARGET_PLATFORM
ARG RUST_VERSION

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    clang \
    make \
    cmake \
    libssl-dev \
    protobuf-compiler \
    libzstd-dev \
    git \
    tar \
    gzip \
    perl \
    curl \
    ca-certificates \
    software-properties-common \
    python3-pip \
    zip \
    file \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add ${TARGET_PLATFORM}

RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.13-full python3.13-dev && ldconfig

# Install cargo-lambda, ignore venv errors for now
RUN pip3 install --break-system-package cargo-lambda

WORKDIR /build

# Copy the bundle-zip script
COPY scripts/bundle-zip.sh /usr/local/bin/bundle-zip.sh
RUN chmod +x /usr/local/bin/bundle-zip.sh

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY rust-toolchain.toml ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --target ${TARGET_PLATFORM}; \
    rm -rf src

# Copy actual source code
COPY src ./src

ENV PYO3_PYTHON=/usr/bin/python3.13

# Build the Lambda function
RUN touch src/main.rs && \
    cargo lambda build --release --target ${TARGET_PLATFORM}

RUN find /build/target -name 'bootstrap'

# Build the bundled bootstrap, including lib files
RUN /usr/local/bin/bundle-zip.sh \
    /build/target/lambda/rotel-lambda-forwarder/bootstrap \
    /build/target/lambda/rotel-lambda-forwarder/bootstrap.zip

# The zip file will be in target/lambda/rotel-lambda-forwarder/bootstrap.zip
