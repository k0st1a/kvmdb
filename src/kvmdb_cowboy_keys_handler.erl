-module(kvmdb_cowboy_keys_handler).

-export([init/2]).
-export([service_available/2]).
-export([known_methods/2]).
-export([allowed_methods/2]).
-export([resource_exists/2]).
-export([delete_resource/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).
-export([from_json/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

service_available(Req, State) ->
    IsAvailable = kvmdb_manager:is_started(),
    {IsAvailable, Req, State}.

known_methods(Req, State) ->
    {[
        <<"PUT">>,
        <<"DELETE">>,
        <<"OPTIONS">>
    ], Req, State}.

allowed_methods(Req, State) ->
    {[
        <<"PUT">>,
        <<"DELETE">>,
        <<"OPTIONS">>
    ], Req, State}.

resource_exists(Req, State) ->
    Key = cowboy_req:binding(key, Req),
    IsExists = kvmdb_manager:is_exists(Key),
    {IsExists, Req, State}.

delete_resource(Req, State) ->
    Key = cowboy_req:binding(key, Req),
    kvmdb_manager:delete(Key),
    {true, Req, State}.

content_types_provided(Req, State) ->
	{[
		{<<"application/json">>, to_json}
	], Req, State}.

content_types_accepted(Req, State) ->
	{[
		{<<"application/json">>, from_json}
	], Req, State}.

from_json(Req, State) ->
    {ok, Body, Req2} = cowboy_req:read_body(Req),
    case jsx:decode(Body) of
        #{<<"value">> := Value} ->
            Key = cowboy_req:binding(key, Req),
            make_insert(Req2, {Key, Value});
        _ ->
            {false, Req2, State}
    end.

make_insert(Req, KV) ->
    case kvmdb_manager:insert(KV) of
        {ok, updated} ->
            Req2 = cowboy_req:set_resp_body(<<>>, Req),
            {true, Req2, undefined};
        {ok, inserted} ->
            {true, Req, undefined};
        {error, _} ->
            {false, Req, undefined}
    end.
