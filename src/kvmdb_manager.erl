-module(kvmdb_manager).

-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

-export([
    %% API
    start_link/1,
    insert/1,
    delete/1,
    start_get/0,
    start_get/1,
    continue_get/1,
    stop_get/0,
    %% gen_server callbacks
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(TABLE_NAME, kvmdb).

-record(state, {
    max_key_count :: non_neg_integer(),
    max_value_size :: non_neg_integer()
}).
-type state() :: #state{}.

-record(kvr, { %% key value record
    key :: binary(),
    value :: binary(),
    timestamp :: erlang:timestamp()
}).

-record(insert, {
    key :: binary(),
    value :: binary()
}).
-type insert() :: #insert{}.

-record(delete, {
    key :: binary()
}).
-type delete() :: #delete{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%--------------------------------------------------------------------
-spec insert({Key :: binary(), Value :: binary()}) ->
    ok
    | {error, max_key_count_reached}
    | {error, max_value_size_exceeded}.
insert({Key, Value}) ->
    gen_server:call(?MODULE, #insert{key = Key, value = Value}).

%%--------------------------------------------------------------------
-spec delete(Key :: binary()) ->
    ok
    | {error, not_found}.
delete(Key) ->
    gen_server:call(?MODULE, #delete{key = Key}).

%%--------------------------------------------------------------------
-type kv() :: {Key :: binary(), Value :: binary()}.
-type start_get_result() :: {KV :: kv(), Continuation :: term(), To :: erlang:timestamp()} | 'end_of_get'.

-spec start_get() -> start_get_result().
start_get() ->
    start_get(undefined, erlang:timestamp()).

%%--------------------------------------------------------------------
-spec start_get(From :: erlang:timestamp()) -> start_get_result().
start_get(From) ->
    start_get(From, erlang:timestamp()).

%%--------------------------------------------------------------------
-spec start_get(From :: erlang:timestamp(), To :: erlang:timestamp()) -> start_get_result().
start_get(From, To) ->
    lager:debug("From:~1000p, To:~1000p", [From, To]),
    MatchSpec = make_match_spec(From, To),
    lager:debug("MatchSpec:~n~p", [MatchSpec]),
    ets:safe_fixtable(?TABLE_NAME, true),
    case ets:select(?TABLE_NAME, MatchSpec, 1) of
        {[Match], Continuation} ->
            {Match, Continuation, To};
        '$end_of_table' ->
            stop_get(),
            'end_of_get'
    end.

-spec make_match_spec(From :: erlang:timestamp(), To :: erlang:timestamp()) -> term().
make_match_spec(undefined, To) ->
    ets:fun2ms(
        fun (#kvr{key = Key, value = Value, timestamp = Timestamp}) when Timestamp < To ->
                {Key, Value}
        end
    );
make_match_spec(From, To) ->
    ets:fun2ms(
        fun (#kvr{key = Key, value = Value, timestamp = Timestamp}) when (Timestamp >= From) andalso
                                                                         (Timestamp < To) ->
            {Key, Value}
        end
    ).

%%--------------------------------------------------------------------
-spec continue_get(Continuation :: term()) -> {KV :: kv(), Continuation :: term()} | 'end_of_get'.
continue_get(Continue) ->
    case ets:select(Continue) of
        {[Match], Continuation} ->
            {Match, Continuation};
        '$end_of_table' ->
            stop_get(),
            'end_of_get'
    end.

%%--------------------------------------------------------------------
-spec stop_get() -> ok.
stop_get() ->
    ets:safe_fixtable(?TABLE_NAME, false),
    ok.
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
    lager:info("Init, Args:~1000000p", [Args]),
    MaxKeyCount = proplists:get_value(max_key_count, Args),
    MaxValueSize = proplists:get_value(max_value_size, Args),
    ets:new(
        ?TABLE_NAME,
        [
            set,
            protected,
            named_table,
            {keypos, #kvr.key},
		    {read_concurrency, true}
        ]
    ),
    {ok, #state{max_key_count = MaxKeyCount, max_value_size = MaxValueSize}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(#insert{} = Req, _From, #state{} = State) ->
    lager:debug("insert, From: ~100p, Req:~p", [_From, Req]),
    case handle_insert(Req, State) of
        ok ->
            lager:debug("Ok", []),
            {reply, ok, State};
        {error, Cause} ->
            lager:debug("Error of insert, Cause:~1000p", [Cause]),
            {reply, {error, Cause}, State}
    end;
handle_call(#delete{} = Req, _From, #state{} = State) ->
    lager:debug("delete, From: ~100p, Req:~p", [_From, Req]),
    case handle_delete(Req) of
        ok ->
            lager:debug("Deleted", []),
            {reply, ok, State};
        {error, Cause} ->
            lager:debug("Error of delete, Cause:~1000p", [Cause]),
            {reply, {error, Cause}, State}
    end;
handle_call(_Msg, _From, State) ->
    lager:debug("Unknown handle_call, From: ~100p, Msg:~p", [_From, _Msg]),
    {reply, {error, unknown_msg}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    lager:debug("Unknown handle_cast, Msg:~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
%% unknown
handle_info(_Info, State) ->
    lager:debug("Skip handle_info, Info:~p", [_Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    lager:info("Terminate, Reason: ~p", [_Reason]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec handle_insert(Req :: insert(), State :: state()) -> ok
                                                        | {error, max_key_count_reached}
                                                        | {error, max_value_size_exceeded}.
handle_insert(#insert{value = Value}, #state{max_value_size = MaxValueSize}) when bit_size(Value) > MaxValueSize ->
    lager:debug("Max value size exceeded, MaxValueSize:~1000p", [MaxValueSize]),
    {error, max_value_size_exceeded};
handle_insert(#insert{key = Key, value = Value} = Req, State) ->
    lager:debug("Handle insert, Key:~1000p", [Key]),
    case ets:member(?TABLE_NAME, Key) of
        true ->
            lager:debug("key(~1000p) exists => update", [Key]),
            ets:update_element(
                ?TABLE_NAME,
                Key,
                [
                    {#kvr.value, Value},
                    {#kvr.timestamp, erlang:timestamp()}
                ]
            ),
            ok;
        _ ->
            lager:debug("key(~1000p) not exists => insert", [Key]),
            try_insert(Req, State)
    end.

-spec try_insert(Req :: insert(), State :: state()) -> ok
                                                     | {error, max_key_count_reached}.
try_insert(#insert{key = Key, value = Value} = Req, #state{max_key_count = MaxKeyCount}) ->
    KeyCount = ets:info(?TABLE_NAME, size),
    lager:debug("Try insert, KeyCount:~1000p, MaxKeyCount:~1000p", [KeyCount, MaxKeyCount]),
    case (KeyCount + 1) > MaxKeyCount of
        true ->
            lager:debug("Max key count reached", []),
            {error, max_key_count_reached};
        _ ->
            lager:debug("Insert, Key:~1000p, Value:~1000p", [Key, Value]),
            ets:insert(
                ?TABLE_NAME,
                #kvr{
                    key = Req#insert.key,
                    value = Req#insert.value,
                    timestamp = erlang:timestamp()
                }
            ),
            ok
    end.

-spec handle_delete(Req :: delete()) -> ok
                                      | {error, not_found}.
handle_delete(#delete{key = Key}) ->
    lager:debug("Handle delete, Key:~1000p", [Key]),
    case ets:member(?TABLE_NAME, Key) of
        true ->
            lager:debug("key(~1000p) exists => delete", [Key]),
            ets:delete(?TABLE_NAME, Key),
            ok;
        _ ->
            lager:debug("key(~1000p) not exists => error", [Key]),
            {error, not_found}
    end.
