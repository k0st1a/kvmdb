-module(kvmdb_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    lager:info("Start", []),
    kvmdb_sup:start_link().

stop(_State) ->
    lager:info("Stop", []),
    ok.
