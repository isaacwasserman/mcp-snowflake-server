.DEFAULT_GOAL := help
.PHONY: help install ruff run

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Uv install dependencies and set up the development environment
	uv sync --group dev --active

ruff: ## Run Ruff on all the code and autofix when possible
	ruff check . --fix
	ruff format .

run: ## Run mcp_snowflake_server locally
	uv --directory . run mcp_snowflake_server

