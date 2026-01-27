FROM public.ecr.aws/lambda/python:3.13 AS builder

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
    file \
    && dnf clean all

# Install Rust
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add ${TARGET_PLATFORM}

RUN pip3 install cargo-lambda

WORKDIR /build

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY rust-toolchain.toml ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --features pyo3; \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build with pyo3 feature enabled, targeting >=2.34 glibc required for linking libpython
RUN touch src/main.rs && \
    cargo lambda build --release --features pyo3 --target ${TARGET_PLATFORM}.2.34

FROM public.ecr.aws/lambda/python:3.13

# Copy the bootstrap binary to the Lambda expected location
COPY --from=builder /build/target/lambda/rotel-lambda-forwarder/bootstrap ${LAMBDA_TASK_ROOT}/bootstrap

# Ensure the binary is executable
RUN chmod +x ${LAMBDA_TASK_ROOT}/bootstrap

# Set LD_LIBRARY_PATH to include Python libraries
# ENV LD_LIBRARY_PATH=/var/lang/lib:/lib64:/usr/lib64:${LD_LIBRARY_PATH}

# Set the ENTRYPOINT to run the bootstrap binary as a custom runtime
ENTRYPOINT [ "/var/task/bootstrap" ]
