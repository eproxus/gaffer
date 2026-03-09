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

## Coding Conventions

* Prefer exceptions over tagged return values. If the caller cannot meaningfully
  act on the return value at the call site, an exception should be used.

## Changes

After every change, verify and test the codebase. List the number of added or
removed lines for all files with nice graphics.
