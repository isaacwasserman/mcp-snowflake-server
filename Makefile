# Makefile for MCP Snowflake Server

.PHONY: help install test coverage clean lint format check

# Default target
help:
	@echo "Available targets:"
	@echo "  install        Install dependencies"
	@echo "  test           Run all tests"
	@echo "  coverage       Run tests with coverage report"
	@echo "  lint           Run linting checks"
	@echo "  format         Format code"
	@echo "  check          Run all checks (lint + test)"
	@echo "  clean          Clean up generated files"

# Install dependencies
install:
	uv sync --dev

# Run all tests
test:
	python -m pytest tests/

# Run tests with coverage
coverage:
	python -m pytest tests/ --cov=src/mcp_snowflake_server --cov-report=term-missing --cov-report=html:htmlcov

# Lint code
lint:
	python -m pyright src/ tests/

# Format code (if you add formatting tools later)
format:
	ruff format --diff

# Run all checks
check: lint test

# Clean up generated files
clean:
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/
	rm -rf __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Development setup
dev-setup: install
	@echo "Development environment setup complete!"
	@echo "Run 'make test' to verify everything works."

# Quick test run for development
dev-test:
	python -m pytest tests/ -x -v --tb=short

# Test with verbose output
test-verbose:
	python -m pytest tests/ -v -s

# Generate coverage report only
coverage-report:
	python -m pytest tests/ --cov=src/mcp_snowflake_server --cov-report=html:htmlcov --cov-report=term
	@echo "Coverage report generated in htmlcov/index.html"
