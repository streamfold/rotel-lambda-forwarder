.PHONY: build clippy deploy

IAM_ROLE ?= "arn:aws:iam::123456789012:role/lambda-execution-role"

build:
	cargo lambda build --release

clippy:
	cargo clippy

deploy: build
	cargo lambda deploy --role ${IAM_ROLE} rotel-lambda-forwarder
