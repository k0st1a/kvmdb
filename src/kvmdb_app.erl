-module(kvmdb_app).

-behaviour(application).

-export([
    start/2,
    prep_stop/1,
    stop/1
]).


start(_Type, _Args) ->
    lager:info("Start", []),
	Dispatch =
        cowboy_router:compile([
            {'_', [
                {"/kvmdb_api/v1/keys/:key", kvmdb_cowboy_keys_handler, []},
                {"/kvmdb_api/v1/streams", kvmdb_cowboy_stream_handler, []}
            ]}
        ]),
    TransportOptions = [
        {port, application:get_env(kvmdb, port, 8181)}
    ],
    ProtocolOptions = #{
        env => #{dispatch => Dispatch},
	    stream_handlers => [cowboy_compress_h, cowboy_stream_h]
    },
    case cowboy:start_clear(http, TransportOptions, ProtocolOptions) of
        {ok, _} ->
            kvmdb_sup:start_link();
        {error, _} = Error ->
            Error
    end.

prep_stop(State) ->
    lager:info("Prep stop", []),
    ok = ranch:suspend_listener(http),
    ok = ranch:wait_for_connections(http, '==', 0),
    ok = ranch:stop_listener(http),
    State.

stop(_State) ->
    lager:info("Stop", []),
    ok.
