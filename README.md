# pg-tam

[![Build Status](https://github.com/robertmu/pg-lakehouse/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakehouse/actions)
[![Rust](https://img.shields.io/badge/rust-1.85.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-14%20%7C%2015%20%7C%2016-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/pg-tam.svg)](https://crates.io/crates/pg-tam)

**A PostgreSQL Table Access Method (TAM) framework in Rust**

`pg-tam` is a PostgreSQL extension framework built with [pgrx](https://github.com/tcdi/pgrx) that simplifies the development of custom Table Access Methods (TAM). The framework provides safe abstractions and boilerplate reduction for integrating deep into PostgreSQL's storage engine.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)

## Overview

pg-tam provides a framework for building PostgreSQL table access methods. The framework consists of three main components:

- **pg-tam**: Core framework library providing access method abstractions and utilities
- **pg-tam-macros**: Procedural macro library for simplifying access method development  
- **pg-iceberg**: Apache Iceberg access method implementation as a reference example

### Key Benefits

pg-tam leverages PostgreSQL's access method interface to provide:

- **Native Integration**: Direct integration with PostgreSQL's storage and query execution engine
- **High Performance**: Optimized data access patterns with minimal overhead
- **Safe Abstractions**: Rust traits that map to PostgreSQL's internal C structures
- **Extensible Architecture**: Simple framework for implementing custom storage engines

## Key Features

**High Performance**
- Native PostgreSQL access method integration
- Support for custom scan strategies
- Helper functions for memory management and transactions

**Developer Experience**  
- Simple trait-based architecture (`TableAccessMethod` trait)
- Procedural macros (`#[pg_table_am]`) for reducing boilerplate code
- Comprehensive error handling and logging utilities

**Reference Implementation**
- See `pg-iceberg` for a real-world usage example
