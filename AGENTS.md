# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

Gaffer is a persistent job queue implemented in Erlang/OTP.

## Build & Development Commands

```sh
mise run verify             # Run all linting
mise run test               # Run all tests (requires Docker for CT)
mise run format             # Format all code
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
* `gaffer_driver_ets` - ETS driver implementation (in-memory, for testing/dev)
* `gaffer_driver_pgo` - Postgres driver implementation (using `pgo`, test-only dep)
* `gaffer_postgres` - Postgres SQL query builder / deserializer
* `gaffer_worker` - Worker process executing jobs
* `gaffer_sup` - Top-level supervisor

## Coding Conventions

* Prefer exceptions over tagged return values. If the caller cannot meaningfully
  act on the return value at the call site, an exception should be used.
* Always comment using single comment characters (%)
* Use binary sigils (`~"bin"`) over old-style binaries (`<<"bin">>`)
* Prefer map pattern matching over `maps:find/2`
* Keep JSON payloads with binary keys, don't convert to atoms
* Never chain shell commands with `&&`
* Use `gaffer` and `gaffer_queue` as templates for module layouts (comments,
  sections etc.)
* Exported functions at the top under API heading
    * Sorted under relevant sections if needed
* Private functions below under the Internal heading
    * Here functions should be sorted in the order of appearance in the module

## Changes

After every change

* Format the code: `mise run format`
* Lint: `mise run verify`
* Test: `mise run test`
* Ensure all modified code is covered by tests
