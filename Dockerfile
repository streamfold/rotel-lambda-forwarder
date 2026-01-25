FROM public.ecr.aws/lambda/python:3.13

ARG TARGET_PLATFORM
ARG RUST_VERSION

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN dnf install -y \
    clang \
    make \
    cmake \
    openssl-devel \
    protobuf-compiler \
    libzstd-devel \
    git \
    tar \
    gzip \
    perl \
    ca-certificates \
    zip \
    file \
    && dnf clean all

# Install Rust
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add ${TARGET_PLATFORM}

RUN pip3 install cargo-lambda

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
    cargo build --release --target ${TARGET_PLATFORM}.2.34; \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build with a target specifically targeting the >=2.34 glibc, required
# for linking libpython
RUN touch src/main.rs && \
    cargo lambda build --release --target ${TARGET_PLATFORM}.2.34

# Build the bundled bootstrap, including lib files
RUN /usr/local/bin/bundle-zip.sh \
    /build/target/lambda/rotel-lambda-forwarder/bootstrap \
    /build/target/lambda/rotel-lambda-forwarder/bootstrap.zip

# The zip file will be in target/lambda/rotel-lambda-forwarder/bootstrap.zip
