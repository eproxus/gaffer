# TO-DO

- [X] Fix the UUID PGO tests to be runnable by ETS driver
- [X] Promote ETS driver to `src`
- [X] Implement all Postgres milestones
- [X] Protocol between `gaffer_postgres` and driver should be to return iolists,
      not binaries. Driver is responsible for calling `iolist_to_binary` if necessary
- [X] Deal with linter issues
    - [X] Fix existing warnings
    - [X] Disable as many special cases as possible
- [X] Refactor all module to have `%---` API and `%---` Internal properly
- [X] Review type placement and move to better owning modules
- [X] Implement a hook system (see below)
- [X] Refactor row_to_job to be `decode_job` (to match `encode_job`)
- [X] We can thread the queue config from the first lookup throughout gaffer_queue
      instead of looking it up all the time
    - Job max_attempts default is hardcoded to 3 in gaffer_queue
    - We're looking up the new queue config in maybe_forward but only care about
      max_attempts. We should care about all config for the new queue?
- [X] Remove 'scheduled' state, it can be represented with 'available' +
      'scheduled_at'
- [X] Move timestamp normalization out of gaffer_queue into the driver layer
      to remove duplication with gaffer_driver_pgo:encode_timestamp/1
- [X] Refactor runner and queue module APIs so the runner doesn't construct job
      internal data (such as errors)
- [X] Improve CI to use `mise` and only one workflow
- [X] Add a feature to introspect queues
    - [X] Count actual items in storage. How to make performant?
- [X] Document public API using `-moduledoc` and `-doc` attributes
- [X] Allow `worker` funs
- [X] Make ETS driver start with Gaffer
    - How to handle the driver arguments? Store centrally somewhere?
- [X] Allow `infinity` as value to `global_max_workers` and set that to default
- [X] `gaffer:list` should be always be per queue
    - [X] `gaffer:list(Queue)`
    - [X] `gaffer:list(Queue, Filters)`
- [X] Store `{complete, Result}` persistently per job
- [X] Refactor the strange dispatch lookup in gaffer_queue_runner
- [X] Implement 'priority' support
- [X] Review gaffer_queue_runner job functions that are only used in tests
    - Should they be exported like this? Are they needed?
- [X] Implement retries and backoff
- [X] Figure out a way to make on_discard atomic for Postgres (without messing with ETS)
    - Implement `job_upsert/1` that takes multiple jobs and atomically inserts/updates
      them
- [X] Define priority (should allow negative and make higher higher)
- [ ] Add a global job pruner process that deletes stale/orphaned jobs
    - Completed jobs that are older than X
    - Jobs that failed and haven't been transferred to a DLQ
    - Jobs belonging to queues that are no longer used (how to detect?)
        - If many nodes upgrade to new config that abandons a queue, how do we
          detect this? There is no cluster query
    - Investigate if we can remove the `idx_gaffer_jobs_state` index?
- [ ] `gaffer_driver_ets` could be reactive by triggering a poll
    - Should it only be reactive, i.e. we don't need poll_interval at all...
- [ ] Starting pools using `gaffer_driver_pgo` crashes
- [ ] Handle timeouts in runner
- [ ] Do not expose the internal driver configuration in the exposed queue config
- [ ] Add a public `migrations/1` function to the PGO driver to use together
      with rollback
- [ ] Make job ID output format configurable (hex, type_id etc.)
- [ ] Make job ID UUID format configurable (`v4` etc.)
- [ ] Implement drain and flush
- [ ] Make queue config defaults an application environment variable
- [ ] Handle raw (e.g. non-UTF) binaries in JSON normalization:
      ```erlang
      jsonify(B) when is_binary(B) ->
          try
              json:encode(B),
              B
          catch
              error:{invalid_byte, _} ->
                  print_term_to_binary(B);
              error:unexpected_end ->
                  print_term_to_binary(B)
          end;
      ```
- [ ] Review and deduplicate tests
    - Use queue info to verify test state?
- [ ] More hooks
    - [ ] Worker created/destroyed
    - [ ] ...
- [ ] Remove any usage of global_max_workers from the ETS driver? Fold it into
      an internal max value at the beginning?
- [ ] Make time output value configurable
    - [ ] Support date tuples is input/output format
- [ ] Implement `egpsql` driver to verify the driver/Postgres APIs
- [ ] Figure out a way to run EXPLAIN ANALYZE on all queries to validate
      performance/indices
- [ ] on_discard should be switchable so success jobs can be put in one queue
      and failed in another. How do dispatch?
    - on_success?
    - What about discarded state? What is it used for? (cleanup)
    - [ ] Handle deleting a queue that is referenced in on_discard
- [ ] Add support for LISTEN/NOTIFY
- [ ] Performance tests
    - [ ] Test queries with EXPLAIN ANALYZE and large datasets
    - [ ] Performance test Erlang code with erlperf
- [ ] Add property based tests
- [ ] Document driver quirks
    - Postgres
        - JSON fields return binary keys
        - More fine-grained timestamps are truncated to microseconds
            precision (but returned as native)
    - ETS
        - Preserves original terms including atom keys
- [ ] Write a user guide
- [ ] Avoid querying all persistent terms when stopping Gaffer
    - ETS table *plus* persistent_term ("best of both worlds")?
