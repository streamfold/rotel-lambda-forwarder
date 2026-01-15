.PHONY: clippy docker-build deploy clean

# Required for deployment
IAM_ROLE ?= "arn:aws:iam::123456789012:role/lambda-execution-role"

# Build configuration
FUNC_NAME ?= "rotel-lambda-forwarder"
ARCH ?= x86_64
RUST_VERSION := $(shell grep '^channel = ' rust-toolchain.toml | sed 's/channel = "\(.*\)"/\1/')
DOCKER_IMAGE := rotel-lambda-builder:$(ARCH)
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

# Docker-based build (matches pre-release workflow)
docker-build:
	@echo "Building $(FUNC_NAME) for $(ARCH) using Docker..."
	@echo "Rust version: $(RUST_VERSION)"
	@echo "Docker platform: $(DOCKER_PLATFORM)"

	# Build Docker image
	docker buildx build \
		--platform $(DOCKER_PLATFORM) \
		--load \
		-t $(DOCKER_IMAGE) \
		--build-arg TARGET_PLATFORM=$(TARGET_PLATFORM) \
		--build-arg RUST_VERSION=$(RUST_VERSION) \
		--progress plain \
		.

	# Extract bootstrap.zip from container
	@echo "Extracting bootstrap.zip from container..."
	@CONTAINER_ID=$$(docker create $(DOCKER_IMAGE)) && \
	mkdir -p tmp/ &&
	docker cp $$CONTAINER_ID:/build/target/lambda/rotel-lambda-forwarder/bootstrap.zip ./tmp/$(BOOTSTRAP_ZIP) && \
	docker rm $$CONTAINER_ID

	@echo "Build complete: ./tmp/$(BOOTSTRAP_ZIP)"

# Deploy using Docker-built artifact
deploy: docker-build
	@echo "Deploying $(FUNC_NAME)..."
	IAM_ROLE=$(IAM_ROLE) ./scripts/deploy-lambda.sh ./tmp/$(BOOTSTRAP_ZIP) $(FUNC_NAME)

# Clean build artifacts
clean:
	rm -f ./tmp/$(BOOTSTRAP_ZIP)
	rm -rf target/
	docker rmi $(DOCKER_IMAGE) 2>/dev/null || true
