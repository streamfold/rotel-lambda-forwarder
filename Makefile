.PHONY: build clippy deploy

IAM_ROLE ?= "arn:aws:iam::123456789012:role/lambda-execution-role"

FUNC_NAME ?= "rotel-lambda-forwarder"

build:
	cargo lambda build --release

clippy:
	cargo clippy

deploy: build
	cargo lambda deploy --role ${IAM_ROLE} --binary-name "rotel-lambda-forwarder" ${FUNC_NAME}
