# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

Gaffer is a persistent job queue implemented in Erlang/OTP.

## Build & Development Commands

```sh
mise run verify             # Run all verification checks
mise run test               # Run all tests
rebar3 fmt                  # Format code
elp lint --diagnostic-filter <code> --apply-fix # Apply specific fixes
```

## Architecture

Modules:

* `gaffer` - Public user API
* `gaffer_queue` - Functional queue implementation
* `gaffer_queue_runner` - Process managing a queue and its jobs (using `gaffer_queue`)
* `gaffer_driver` - Storage-agnostic driver behaviour
* `gaffer_worker` - Worker process executing jobs

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
