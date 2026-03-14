# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

Gaffer is a persistent job queue implemented in Erlang/OTP.

## Build & Development Commands

```sh
mise run verify             # Run all verification checks
mise run test               # Run all tests (requires Docker for CT)
rebar3 fmt                  # Format code
rebar3 eunit --module=gaffer_tests  # Run a single eunit module
rebar3 ct --suite=gaffer_driver_pgo_SUITE  # Run a single CT suite
elp lint --diagnostic-filter W0023 --apply-fix  # Apply a specific elp fix by code
```

CT tests use `docker compose up -d --wait` / `docker compose down` automatically
via rebar3 hooks — Docker must be running.

## Architecture

Modules:

* `gaffer` - Public user API
* `gaffer_queue` - Functional queue implementation
* `gaffer_queue_runner` - Process managing a queue and its jobs (using `gaffer_queue`)
* `gaffer_driver` - Storage-agnostic driver behaviour
* `gaffer_driver_pgo` - Postgres driver implementation (using `pgo`, test-only dep)
* `gaffer_postgres` - Postgres SQL query builder / deserializer
* `gaffer_worker` - Worker process executing jobs
* `gaffer_sup` - Top-level supervisor

### Separation of Concerns / Hierarchy

The encapsulation layers are as follows:

`gaffer` -> `gaffer_queue_runner` -> `gaffer_queue` -> DriverMod.

* `gaffer_queue` has a functional API that can be unit tested with a functional
  mock driver.

## Coding Conventions

* Prefer exceptions over tagged return values. If the caller cannot meaningfully
  act on the return value at the call site, an exception should be used.

## Changes

After every change
* Format the code (`rebar3 fmt`)
* Verify and test the codebase
