-module(kvmdb_debug_api).

-export([
    start/0,
    stop/0,
    restart/0,
    lager/0,
    nolager/0,
    debug/0,
    nodebug/0
]).

-spec start() -> {ok, Started :: [atom()]} | {error, Reason :: term()}.
start() ->
    lager:info("Start", []),
    Result = application:ensure_all_started(kvmdb),
    lager:info("Start, Result: ~p", [Result]),
    Result.

-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    lager:info("Stop", []),
    Result = application:stop(kvmdb),
    lager:info("Stop, Result: ~p", [Result]),
    Result.

-spec restart() -> {ok, Started :: [atom()]} | {error, Reason :: term()}.
restart() ->
    lager:info("Restart", []),
    stop(),
    start().

-spec debug() -> {ok, Started :: [atom()]} | {error, Reason :: term()}.
debug() ->
    lager:info("Debug", []),
    lager(),
    async().

-spec nodebug() -> {ok, Started :: [atom()]} | {error, Reason :: term()}.
nodebug() ->
    lager:info("Nodebug", []),
    nolager(),
    noasync().

-spec lager() -> ok.
lager() ->
    lager:info("Lager", []),
    Result = application:ensure_all_started(lager),
    lager:info("Lager, Result: ~p", [Result]),
    ok.

-spec async() -> ok.
async() ->
    lager:info("Async", []),
    Result = application:ensure_all_started(async),
    lager:info("Async, Result: ~p", [Result]),
    ok.

-spec nolager() -> ok.
nolager() ->
    lager:info("Nolager", []),
    Result = application:stop(lager),
    lager:info("Nolager, Result: ~p", [Result]),
    ok.

-spec noasync() -> ok.
noasync() ->
    lager:info("Nosync", []),
    Result = application:stop(async),
    lager:info("Nosync, Result: ~p", [Result]),
    ok.
