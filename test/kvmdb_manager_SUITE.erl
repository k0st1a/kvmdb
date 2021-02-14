-module(kvmdb_manager_SUITE).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    start_get/1,
    insert_get/1,
    insert_get_update_get/1,
    insert_get_insert_get/1,
    insert_max_key_count_reached/1,
    insert_max_value_size_exceeded/1,
    delete_not_found/1,
    insert_delete_get/1
]).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() -> [
    start_get,
    insert_get,
    insert_get_update_get,
    insert_get_insert_get,
    insert_max_key_count_reached,
    insert_max_value_size_exceeded,
    delete_not_found,
    insert_delete_get
].

suite() ->
    [{timetrap, {seconds, 5}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    kvmdb_debug_api:lager(),
    kvmdb_debug_api:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    kvmdb_debug_api:stop(),
    kvmdb_debug_api:nolager(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================
start_get(_Config) ->
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:start_get()
    ).

insert_get(_Config) ->
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key">>, <<"value">>})
    ),
    {KV2, Continuation, _} = kvmdb_manager:start_get(),
    ?assertEqual(
        {<<"key">>, <<"value">>},
        KV2
    ),
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:continue_get(Continuation)
    ).

insert_get_update_get(_Config) ->
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key">>, <<"value">>})
    ),
    {KV2, Continuation, To} = kvmdb_manager:start_get(),
    ?assertEqual(
        {<<"key">>, <<"value">>},
        KV2
    ),
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:continue_get(Continuation)
    ),
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key">>, <<"value2">>})
    ),
    {KV3, Continuation2, _} = kvmdb_manager:start_get(To),
    ?assertEqual(
        {<<"key">>, <<"value2">>},
        KV3
    ),
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:continue_get(Continuation2)
    ).

insert_get_insert_get(_Config) ->
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key">>, <<"value">>})
    ),
    {KV2, Continuation, To} = kvmdb_manager:start_get(),
    ?assertEqual(
        {<<"key">>, <<"value">>},
        KV2
    ),
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:continue_get(Continuation)
    ),
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key2">>, <<"value2">>})
    ),
    {KV3, Continuation2, _} = kvmdb_manager:start_get(To),
    ?assertEqual(
        {<<"key2">>, <<"value2">>},
        KV3
    ),
    ?assertEqual(
        'end_of_get',
        kvmdb_manager:continue_get(Continuation2)
    ).

insert_max_key_count_reached(_Config) ->
    ok = kvmdb_manager:insert({<<"key">>, <<"value">>}),
    ok = kvmdb_manager:insert({<<"key2">>, <<"value2">>}),
    ok = kvmdb_manager:insert({<<"key3">>, <<"value3">>}),
    ok = kvmdb_manager:insert({<<"key4">>, <<"value4">>}),
    ?assertEqual(
        {error, max_key_count_reached},
        kvmdb_manager:insert({<<"key5">>, <<"value5">>})
    ).

insert_max_value_size_exceeded(_Config) ->
    ?assertEqual(
        {error, max_value_size_exceeded},
        kvmdb_manager:insert({<<"key5">>, <<"value101">>})
    ).

delete_not_found(_Config) ->
    ?assertEqual(
        {error, not_found},
        kvmdb_manager:delete(<<"key">>)
    ).

insert_delete_get(_Config) ->
    ?assertEqual(
        ok,
        kvmdb_manager:insert({<<"key">>, <<"value">>})
    ),
    ?assertEqual(
        ok,
        kvmdb_manager:delete(<<"key">>)
    ),
    ?assertEqual(
       'end_of_get',
       kvmdb_manager:start_get()
    ).
