# adbc

`adbc` (short for **A**synchronous **D**ata**B**ase **C**onnector) is a library and CLI that provides operations between Postgres-based databases.

## Commands

There are several commands available

### diff 

### info

### copy

### run

## Configuration

`adbc` looks for a config file called "adbc.yml" where it expects to find blocks:

- **adbc**: tool information
- **databases**: named set of datasources with connection and schema info
- **workflows**: named set of tasks, each with one or more commands

### adbc

- **version**: version string

### databases

### workflow
