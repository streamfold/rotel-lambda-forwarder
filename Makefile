.PHONY: clippy build-zip build-container deploy-zip deploy-container clean

# Required for deployment
IAM_ROLE ?= "arn:aws:iam::123456789012:role/lambda-execution-role"

# Build configuration
FUNC_NAME ?= "rotel-lambda-forwarder"
ARCH ?= x86_64
RUST_VERSION := $(shell grep '^channel = ' rust-toolchain.toml | sed 's/channel = "\(.*\)"/\1/')
BOOTSTRAP_ZIP := bootstrap.zip

# Docker platform mapping
ifeq ($(ARCH),x86_64)
	TARGET_PLATFORM := x86_64-unknown-linux-gnu
	DOCKER_PLATFORM := linux/amd64
else ifeq ($(ARCH),arm64)
	TARGET_PLATFORM := aarch64-unknown-linux-gnu
	DOCKER_PLATFORM := linux/arm64
else
	$(error Unsupported architecture: $(ARCH). Use x86_64 or arm64)
endif

clippy:
	cargo clippy

# Build 1: Direct cargo lambda build (produces bootstrap.zip)
build-zip:
	@echo "Building $(FUNC_NAME) for $(ARCH) using cargo lambda..."
	@echo "Target: $(TARGET_PLATFORM)"

	cargo lambda build --release -o zip --target $(TARGET_PLATFORM)

	@mkdir -p tmp/
	@cp target/lambda/rotel-lambda-forwarder/bootstrap.zip ./tmp/$(BOOTSTRAP_ZIP)
	@echo "Build complete: ./tmp/$(BOOTSTRAP_ZIP)"

# Build 2: Docker container build with pyo3 feature (produces Docker image)
build-container:
	@echo "Building $(FUNC_NAME) container for $(ARCH) with pyo3 feature..."
	@echo "Rust version: $(RUST_VERSION)"
	@echo "Docker platform: $(DOCKER_PLATFORM)"

	# Build Docker image with pyo3 feature
	docker buildx build \
		--platform $(DOCKER_PLATFORM) \
		--load \
		-t rotel-lambda-forwarder:$(ARCH) \
		--build-arg TARGET_PLATFORM=$(TARGET_PLATFORM) \
		--build-arg RUST_VERSION=$(RUST_VERSION) \
		--progress plain \
		.

	@echo "Container build complete: rotel-lambda-forwarder:$(ARCH)"

# Deploy using zip artifact
deploy-zip: build-zip
	@echo "Deploying $(FUNC_NAME) from zip..."
	IAM_ROLE=$(IAM_ROLE) ./scripts/deploy-lambda.sh ./tmp/$(BOOTSTRAP_ZIP) $(FUNC_NAME)

# Deploy using container image
deploy-container: build-container
	@echo "Container deployment requires pushing to ECR and updating Lambda function"
	@echo "Image: rotel-lambda-forwarder:$(ARCH)"
	@echo "Please use AWS CLI or console to deploy the container image"

# Clean build artifacts
clean:
	rm -rf ./tmp/
	rm -rf target/
	docker rmi rotel-lambda-forwarder:$(ARCH) 2>/dev/null || true
