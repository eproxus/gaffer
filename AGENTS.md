# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

Gaffer is a reliable job queue implemented in Erlang/OTP.

## Build & Development Commands

```sh
mise run --output=keep-order verify             # Run all linting
mise run --output=keep-order test               # Run all tests (requires Docker for CT)
mise run format                                 # Format all code
mise run docs                                   # Generate documentation
rebar3 eunit --module=gaffer_tests              # Run a single eunit module
rebar3 ct --suite=gaffer_driver_pgo_SUITE       # Run a single CT suite
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
* `gaffer_driver_pgo` - Postgres driver implementation (using `pgo`, optional dep)
* `gaffer_postgres` - Postgres SQL query builder / deserializer
* `gaffer_worker` - Worker process executing jobs
* `gaffer_sup` - Top-level supervisor

## Coding Conventions

* Use `gaffer` and `gaffer_queue` as templates for module layouts (comments,
  sections etc.)
* Prefer exceptions over tagged return values. If the caller cannot meaningfully
  act on the return value at the call site, an exception should be used.
* Always comment using single comment characters (%)
* Use binary sigils (`~"bin"`) over old-style binaries (`<<"bin">>`)
* Prefer map pattern matching over `maps:get` or `maps:find`:

  ```erlang
  % Don't
  Verbose = maps:get(admin, User, undefined),
  case Verbose of
      true -> delete();
      _ -> ok
  end.
  % Do
  case User of
      #{admin := true} -> delete();
      _ -> ok
  end.
  ```

* Always match patterns as early as possible to avoid `badarg` or strange
  errors:

  ```erlang
  % Don't
  switch(Opts) ->
      {List, _Length} = maps:get(items, Opts, undefined)
      handle(List).
  % Do
  switch(#{list := {List, _Length}}) -> handle(List).
  ```

* Keep JSON payloads with binary keys, don't convert to atoms
* Keep exported functions at the top under API heading
    * Sorted under relevant sections if needed
* Keep private functions below under the Internal heading
    * Here functions should be sorted in the order of appearance in the module
* Join small functions to single line clauses:

  ```erlang
  % Don't
  check(ok) ->
      true;
  check({error, _}) ->
      false.
  % Do
  check(ok) -> true;
  check({error, _}) -> false.
  ```

* Prefer short functions with pattern matching over case statements:

  ```erlang
  % Don't
  switch(Opts) ->
      case Opts of
          #{verbose := true} -> print();
          _ -> ok
      end.
  % Do
  switch(#{verbose := true}) -> print();
  switch(_Opts) -> ok.
  ```

## Changes

Before making changes:

* Run tests with `mise run --output=keep-order test` and establish a code
  coverage baseline

When making changes:

* When refactoring, check if multiple lines can be joined and still stay under
  the length limit. When in doubt, prefer longer lines over shorter lines, the
  formatter will split them

After every change

* Format the code: `mise run format`
* Lint: `mise run --output=keep-order verify`
* Docs: `mise run docs`
* Test: `mise run --output=keep-order test`
* Ensure all modified code is covered by tests using `rebar3 uncovered --format
  raw --context 0 --git` to check coverage of changed lines
* For substantial changes or new features, verify that total coverage did not go
  down from the baseline
