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

Standard OTP application structure.

Tests use both EUnit (`test/gaffer_tests.erl`) and Common Test (`test/gaffer_SUITE.erl`).

## Code Style & Conventions

- All modules must be prefixed with `gaffer` (verify with Elvis: `rebar3 lint`)
- Formatter: erlfmt with 80-char print width (run `rebar fmt` on any changes)
- Indentation: 4 spaces, no tabs
- Line length: try not to exceed 100 characters
- Compiler flags: `warnings_as_errors` is enabled — all warnings must be resolved
- Dialyzer warns on `unmatched_returns`

## Tooling

- Erlang 28.4 / Rebar3 3.27.0 (managed via `.tool-versions`)
- Plugins: erlfmt, rebar3_lint (elvis), rebar3_hank, rebar3_ex_doc, rebar3_hex
