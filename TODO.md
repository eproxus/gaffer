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
- [ ] Refactor runner and queue module APIs so the runner doesn't construct job
      internal data (such as errors)
- [ ] Handle backoff and timeout in runner
- [ ] on_discard should be switchable so success jobs can be put in one queue
      and failed in another. How do dispatch?
    - What about discarded state? What is it used for? (cleanup)
    - [ ] Handle deleting a queue that is referenced in on_discard
- [ ] Figure out a way to make on_discard atomic for Postgres (without messing with ETS)
- [ ] Add feature to introspect queues
    - [ ] Count actual items in storage. How to make performant?
- [ ] Implement drain and flush
- [ ] Implement 'priority' support
- [ ] Improve CI to use `mise` and only one workflow
- [ ] Add support for LISTEN/NOTIFY
- [ ] Improve tests
    - [ ] Verify life cycle / runner more carefully
        - Figure out a way to timestep the world? Are we already doing this?
            - Manually call poll somehow?
    - [ ] Explore scenarios with shared queue config
        - Nodes booting up with existing queues, differing configs etc.
        - What about a rolling app deployment that updates a queue config?
        - What about config updates to persistent storage? Should other runners
          pick them up?
    - [ ] Add property based tests
- [ ] Document public API using `-moduledoc` and `-doc` attributes
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
