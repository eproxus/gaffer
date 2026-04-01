#!/usr/bin/env escript
% Export cover data to annotated text files alongside existing HTML
% reports. Each subdirectory gets data from its matching .coverdata file,
% while aggregate/ imports all of them.

main(_) ->
    CoverDir = "_build/test/cover",
    case filelib:wildcard(filename:join(CoverDir, "*.coverdata")) of
        [] -> halt(1);
        All ->
            {ok, Null} = file:open("/dev/null", [write]),
            Dir = fun(Name) -> filename:join(CoverDir, Name) end,
            % Per-framework dirs derived from coverdata filenames.
            [export_dir(Dir(filename:basename(F, ".coverdata")), [F], Null) || F <- All],
            % Aggregate dir imports all coverdata files.
            export_dir(Dir("aggregate"), All, Null),
            file:close(Null)
    end.

export_dir(Dir, Files, Null) ->
    import_coverdata(Files, Null),
    Mods = lists:sort(cover:imported_modules()),
    [{ok, _} = cover:analyse_to_file(M, filename:join(Dir, atom_to_list(M) ++ ".txt")) || M <- Mods],
    io:format("  module coverage exported to ~s/*.txt~n", [Dir]).

import_coverdata(Files, Null) ->
    cover:stop(), cover:start(),
    % cover:import/1 and cover:analyse_to_file/2 unconditionally print
    % "Analysis includes data from imported files" to the group leader.
    % Redirect the cover server's group leader to /dev/null to suppress it.
    group_leader(Null, whereis(cover_server)),
    [cover:import(F) || F <- Files].
