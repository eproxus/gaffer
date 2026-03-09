# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

Gaffer is a persistent job queue implemented in Erlang/OTP.

## Build & Development Commands

```sh
rebar3 compile              # Compile
rebar3 fmt                  # Format code
rebar3 fmt --check          # Check formatting without modifying
rebar3 lint                 # Run elvis linter
rebar3 hank                 # Dead code detection
rebar3 dialyzer             # Type checking (incremental)
rebar3 xref                 # Cross-reference checking
rebar3 eunit                # Run unit tests (EUnit)
rebar3 ct                   # Run integration tests (Common Test)
rebar3 ci                   # Full CI pipeline (fmt, hank, lint, dialyzer, xref, eunit, ct, ex_doc, cover)
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
