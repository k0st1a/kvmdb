-module(kvmdb_cowboy_stream_handler).

-export([init/2]).
-export([service_available/2]).
-export([known_methods/2]).
-export([allowed_methods/2]).
-export([content_types_provided/2]).
-export([to_json/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

service_available(Req, State) ->
    IsAvailable = kvmdb_manager:is_started(),
    {IsAvailable, Req, State}.

known_methods(Req, State) ->
    {[
        <<"GET">>,
        <<"OPTIONS">>
    ], Req, State}.

allowed_methods(Req, State) ->
    {[
        <<"GET">>,
        <<"OPTIONS">>
    ], Req, State}.

content_types_provided(Req, State) ->
	{[
		{<<"application/json">>, to_json}
	], Req, State}.

to_json(Req, State) ->
    Req2 = cowboy_req:stream_reply(200, Req),
    start_stream(Req2, undefined),
    {<<>>, Req2, State}.

start_stream(Req, From) ->
    case kvmdb_manager:start_get(From) of
        {{Key, Value}, Continuation, To} ->
            Data = jsx:encode(#{Key => Value}),
	        cowboy_req:stream_body(<<"[", Data/binary>>, nofin, Req),
            continue_stream(Req, Continuation, To);
        'end_of_get' ->
            timer:sleep(5000),
            start_stream(Req, From)
	       %cowboy_req:stream_body(<<"[]">>, fin, Req)
    end.

continue_stream(Req, Continuation, To) ->
    case kvmdb_manager:continue_get(Continuation) of
        {{Key, Value}, Continuation2} ->
            Data = jsx:encode(#{Key => Value}),
	        cowboy_req:stream_body(<<",", Data/binary>>, nofin, Req),
            continue_stream(Req, Continuation2, To);
        'end_of_get' ->
	        cowboy_req:stream_body(<<"]">>, nofin, Req),
            timer:sleep(5000),
            start_stream(Req, To)
	       %cowboy_req:stream_body(<<"]">>, fin, Req)
    end.
