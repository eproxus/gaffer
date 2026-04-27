<!-- markdownlint-disable-line MD013 -->
# gaffer [![CI Status][ci-img]][ci] [![Hex.pm Version][hex-img]][hex] [![Docs][docs-img]][docs] [![Minimum Erlang Version][erlang-img]][erlang] [![License][license-img]][license]

[ci]:          https://github.com/eproxus/gaffer/actions/workflows/ci.yml?query=branch%3Amain
[ci-img]:      https://img.shields.io/github/actions/workflow/status/eproxus/gaffer/ci.yml?label=ci
[hex]:         https://hex.pm/packages/gaffer
[hex-img]:     https://img.shields.io/hexpm/v/gaffer
[docs]:        https://hexdocs.pm/gaffer
[docs-img]:    https://img.shields.io/badge/docs-hexdocs-blue
[erlang]:      https://github.com/eproxus/gaffer/blob/main/mise.toml
[erlang-img]:  https://img.shields.io/badge/erlang-28+-blue.svg
[license]:     LICENSE.md
[license-img]: https://img.shields.io/badge/license-MIT-blue.svg

A reliable job queue implemented in Erlang.

## Features

- [x] Priority-based execution
- [x] Per-queue concurrency limits (local and global)
- [x] Pluggable storage drivers (ETS for dev/test, Postgres for production)
- [x] Hooks for queue and job events
- [x] Dead-letter queues (`on_discard`)
- [x] Queue introspection and automatic/manual job pruning
- [x] Delayed job scheduling
- [x] Automatic retries with backoff
- [ ] Drain and flush (graceful shutdown)
- [x] Job execution timeouts
- [ ] Worker shutdown timeouts

## Usage

### Shell

For simple jobs, pass an anonymous function as a worker:

```erlang
1> ok = gaffer:ensure_queue(#{
       name => greetings,
       driver => ets,
       worker => fun(#{payload := #{~"name" := Name}}) ->
           io:format(~"Hello, ~s!~n", [Name]),
           complete
       end
   }).
ok
2> gaffer:insert(greetings, #{~"name" => ~"world"}).
#{id => <<...>>, queue => greetings, state => available, ...}
Hello, world!
```

### Application

#### Define a worker

Implement the `gaffer_worker` behaviour:

```erlang
-module(email_sender).
-behaviour(gaffer_worker).
-export([perform/1]).

perform(#{payload := #{~"to" := To, ~"body" := Body}}) ->
    logger:info(~"Sending email to ~s: ~s", [To, Body]),
    complete.
```

The `perform/1` callback can return:

- `complete` - mark the job as completed
- `{complete, Result}` - complete with a result
- `{fail, Reason}` - fail and retry (up to `max_attempts`)
- `{cancel, Reason}` - cancel the job permanently
- `{schedule, Timestamp}` - reschedule the job for later

Crashes are treated as failures and their reason recorded.

#### Create a queue

```erlang
Driver = gaffer_driver_pgo:start(#{
    pool => my_pool,
    start => #{host => ~"localhost", database => ~"my_app", pool_size => 5}
}),
gaffer:ensure_queue(#{
    name => emails,
    driver => {gaffer_driver_pgo, Driver},
    worker => email_sender
}).
```

#### Insert a job

```erlang
Job = gaffer:insert(emails, #{~"to" => ~"user@example.com", ~"body" => ~"Welcome!"}).
```

## Configuration

Queues are configured via `gaffer:queue_conf()` maps:

- `name` (`atom()`, **required**)

  Queue identifier.

- `worker` (`module() | fun/1`, **required**)

  Worker callback module or function.

- `driver` (`{module(), state()}`)

  Storage driver.

- `max_workers` (`pos_integer()`, default = `1`)

  Max concurrent workers per node.

- `global_max_workers` (`pos_integer() | infinity`, default = `infinity`)

  Max concurrent workers across all nodes.

- `poll_interval` (`pos_integer() | infinity`, default = `1000`).

  Polling interval in ms.

- `max_attempts` (`pos_integer()`, default = `3`).

  Max execution attempts.

- `timeout` (`pos_integer()`, default = `30000`).

  Execution timeout in ms.

- `backoff` (`[non_neg_integer()]`, default = `[1000]`).

  Retry backoff schedule in ms.

- `priority` (`integer()`, default = `0`).

  Default job priority. Can be negative. Jobs with higher values are claimed
  first.

- `shutdown_timeout` (`pos_integer()`, default = `5000`).

  Worker shutdown grace period in ms.

- `on_discard` (`atom()`).

  Dead-letter queue name.

- `hooks` (`[hook()]`, default = `[]`).

  Hook modules or funs called after queue and job events. See [Hooks](#hooks).

- `prune` (`prune_conf()`)

  Pruning configuration. A per-queue pruner periodically deletes jobs in
  terminal states older than the configured max age.

    - `interval` (`pos_integer() | infinity`)

      Prune interval in ms.

    - `max_age` (`#{job_state() | '_' => age()}`)

      Per-state max age in milliseconds. `infinity` means never prune. `'_'` sets
      a default for all states.

      Default: `completed`, `failed`, and `cancelled` jobs are pruned
      immediately, others are kept indefinitely.

## Hooks

Gaffer notifies registered hooks after queue and job events. Each hook
receives an event path (a list of atoms) and a payload map carrying an `actor`
field that identifies which Gaffer process or public API call caused the
event.

Hooks can be registered per queue via the `hooks` configuration option or
globally via the `gaffer` application's `hooks` environment variable.

See [`gaffer_hooks`](https://hexdocs.pm/gaffer/gaffer_hooks.html) for the full
list of events and their payload shapes.

## Changelog

See the [Releases](https://github.com/eproxus/gaffer/releases) page.

## Code of Conduct

Find this project's code of conduct in [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## Contributing

First of all, thank you for contributing with your time and energy.

If you want to request a new feature make sure to [open an issue](https://github.com/eproxus/gaffer/issues/new?template=feature_request.md)
so we can discuss it first.

Bug reports and questions are also welcome, but do check you're using the latest version of the
application - if you found a bug - and/or search the issue database - if you have a question,
since it might have already been answered before.

Contributions will be subject to the MIT License. You will retain the copyright.

For more information check out [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

This project's security policy is made explicit in [SECURITY.md](SECURITY.md).

## Conventions

### Versions

This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### License

This project uses the [MIT License](LICENSE.md).
