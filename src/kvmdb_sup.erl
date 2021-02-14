-module(kvmdb_sup).

-behaviour(supervisor).

-export([
    %% API
    start_link/0,
    %% Supervisor callbacks
    init/1
]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one
    },
    Args = [
        {max_key_count, application:get_env(kvmdb, max_key_count, 1000)},
        {max_value_size, application:get_env(kvmdb, max_value_size, 1024)} %% 1Kb
    ],
    ChildSpecs = [
        #{
            id => kvmdb_manager,
            start => {kvmdb_manager, start_link, [Args]}
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
