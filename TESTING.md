# Testing Guide for MCP Snowflake Server

## Overview

This document provides a comprehensive guide to the test suite for the MCP Snowflake Server. The test suite includes unit tests, integration tests, and comprehensive coverage of all major components.

## Quick Start

### Install Dependencies
```bash
uv sync --dev
```

### Run Tests
```bash
# Run all tests
make test

# Run with coverage
make coverage
```

## Test Execution Options

### Basic Commands
```bash
# All tests
pytest

# Specific test file
pytest tests/test_write_detector.py

# Specific test method
pytest tests/test_write_detector.py::TestSQLWriteDetector::test_analyze_select_query

# With coverage
pytest --cov=src/mcp_snowflake_server --cov-report=html
```

## Debugging Tests

### Verbose Output
```bash
pytest -v -s --tb=long
```

### Debug Specific Test
```bash
pytest tests/test_write_detector.py::TestSQLWriteDetector::test_analyze_select_query -v -s
```

### Coverage Debug
```bash
pytest --cov=src/mcp_snowflake_server --cov-report=term-missing -v
```
