%% =============================================================================
%%  telemetry_metrics.erl -
%%
%%  Copyright (c) 2023 Leapsight Technologies Limited. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(telemetry_metrics).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-define(PT_METADATA, {?MODULE, metadata}).
-define(PT_HISTOGRAM_BUCKETS, {?MODULE, histogram_buckets}).

-define(DEFAULT_BUCKETS,
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
).

-define(IS_HANDLER_ID(Id),
    is_atom(Id) orelse is_list(Id) orelse is_binary(Id)
).
-define(IS_TIME_UNIT(X),
    (
        X == second orelse
        X == millisecond orelse
        X == microsecond orelse
        X == nanosecond orelse
        X == native
    )
).

-define(IS_BYTE_UNIT(X),
    (
        X == byte orelse
        X == kilobyte orelse
        X == megabyte
    )
).

-define(CONFIG_SPEC, #{
    metadata => #{
        datatype => map
    },
    histogram_buckets => #{
        datatype => map,
        validator => {map, {term, {list, number}}}
    },
    handlers => #{
        datatype => list,
        validator => {list, fun
            Validate({HandlerId, Config}) when is_atom(HandlerId) ->
                Validate({atom_to_list(HandlerId), Config});

            Validate({HandlerId, Config}) when is_binary(HandlerId) ->
                Validate({binary_to_list(HandlerId), Config});

            Validate({HandlerId, Config}) when is_list(HandlerId) ->
                try
                    Valid = maps_utils:validate(Config, ?HANDLER_SPEC),
                    {ok, {HandlerId, Valid}}
                catch
                    error:Reason ->
                        {error, Reason}
                end;

            Validate(_) ->
                false
        end}
    }

}).


-define(HANDLER_SPEC, #{
    event_metrics => #{
        required => true,
        default => [],
        validator => {list, fun
            ({EventName, Metrics0})
            when is_list(EventName), is_list(Metrics0) ->
                try
                    Metrics = lists:foldl(
                        fun(Metric0, Acc) ->
                            [maps_utils:validate(Metric0, ?METRIC_SPEC) | Acc]
                        end,
                        [],
                        Metrics0
                    ),
                    {ok, {EventName, Metrics}}

                catch
                    error:Reason ->
                        {error, Reason}
                end;

            (_) ->
                false
        end}
    },
    config => #{
        validator => ?HANDLER_CONFIG_SPEC
    }
}).

-define(HANDLER_CONFIG_SPEC, #{
    type => #{
        required => true,
        datatype => {in, [callback, worker_pool]},
        default => callback
    },
    pool_size => #{
        datatype => pos_integer
    },
    pool_type => #{
        datatype => {in, [round_robin, random, hash]}
    }

}).

-define(METRIC_SPEC, #{
    type => #{
        required => true,
        datatype => {in, [counter, gauge, histogram]}
    },
    name => #{
        required => true,
        datatype => [atom, binary]
    },
    measurement =>#{
        required => true,
        datatype =>  [atom, {function, 1}, {function, 2}]
    },
    buckets => #{
        datatype =>  {list, [number]}
    },
    metadata => #{
        datatype => [{list, [atom]}, map, {function, 1}]
    },
    description => #{
        datatype => [binary, list]
    }
}).

-record(state, {
    handlers = #{}      ::  #{handler_id() => module()}
}).

-type config()          ::  #{handlers => [{handler_id(), handler_config()}]}.
-type handler_id()      ::  term().
-type event_metrics()   ::  [
                                {
                                    EventName :: telemetry:event_name(),
                                    Metrics :: [metric()]
                                }
                            ].
-type handler_config()  ::  #{
                                type => handler_type(),
                                event_metrics =>
                                    [{telemetry:event_name(), [metric()]}],
                                %% Valid if type == worker_pool
                                pool_size => pos_integer(),
                                ppol_type => pool_type()
                            }.
-type handler_type()    ::  callback | worker_pool.
-type pool_type()       ::  round_robin | random | hash.
-type metric()          ::  #{
                                type := counter | gauge | histogram,
                                name := atom() | binary(),
                                measurement := measurement(),
                                convert_unit => unit_convertion(),
                                buckets => [number()],
                                metadata => metadata(),
                                description => binary()
                            }.
-type unit_convertion() ::  unit()
                            | {unit(), unit()}
                            | fun((term()) -> term()).

-type unit()            ::  erlang:time_unit()
                            | byte | kilobyte | megabyte.

-type metadata()        ::  [term()]
                            | telemetry:metadata()
                            | fun(
                                (telemetry:metadata()) -> telemetry:metadata()
                              ).
-type measurement()     ::  atom()
                            | fun((telemetry:measurement()) -> number())
                            | fun(
                            (telemetry:measurement(), telemetry:metadata()) ->                          number()
                            ).

-export_type([config/0]).
-export_type([metric/0]).


%% API
-export([start_link/0]).
-export([attach/4]).
-export([attach_many/3]).
-export([detach/1]).
-export([gen_handler/2]).

%% GEN_SERVER API
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-ifdef(TEST).
-export([maybe_convert_unit/2]).
-export([convert_unit/2]).
-export([parts_per_megabyte/1]).
-export([measurement/3]).
-export([metadata/2]).
-export([metric/3]).
-export([metric_common/3]).
-else.
-compile([
    {nowarn_unused_function, [
        maybe_convert_unit/2,
        convert_unit/2,
        parts_per_megabyte/1,
        measurement/3,
        metadata/2
    ]}
]).
-endif.

-compile({parse_transform, parse_trans_codegen}).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec attach(
    HandlerId :: atom() | list() | binary(),
    EventName :: telemetry:event_name(),
    Metrics :: [metric()],
    Config :: any()) ->
    ok | {error, already_exists}.

attach(HandlerId, EventName, Metrics, Config)
when is_list(EventName), is_list(Metrics) ->
    attach_many(HandlerId, [{EventName, Metrics}], Config).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec attach_many(
    HandlerId :: atom() | list() | binary(),
    EventMetrics :: event_metrics(),
    Config :: handler_config()) -> ok | {error, already_exists}.

attach_many(HandlerId, Entries, Config)
when ?IS_HANDLER_ID(HandlerId) andalso is_list(Entries) ->
    gen_server:call(?MODULE, {attach_many, HandlerId, Entries, Config}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec detach(HandlerId :: term()) -> ok | {error, not_found}.

detach(HandlerId) ->
    %% TODO remote state / unload/destroy module
    %% telemetry:detach(EventName).
    gen_server:call(?MODULE, {detach, HandlerId}).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init(_) ->
    dbg:tracer(), dbg:p(all,c), dbg:tpl(maps_utils, '_', x),

    Config0 = application:get_all_env(?MODULE),
    try
        Config = validate_config(Config0, #{}),
        Cmd = {init_handlers, Config},
        {ok, #state{}, {continue, Cmd}}
    catch
        error:Reason ->
            {stop, Reason}
    end.


handle_continue({init_handlers, Config}, State0) ->
    State = lists:foldl(
        fun({HandlerId, Mod}, Acc) ->
            case Mod:type() of
                callback ->
                    ok = attach_many(HandlerId, Mod),
                    Acc#state{
                        handlers = maps:put(HandlerId, Mod, Acc#state.handlers)
                    };
                worker_pool ->
                    %% TODO
                    error({worker_pool, not_implemented})
            end
        end,
        State0,
        gen_handlers(Config)
    ),

    {noreply, State}.


handle_call({attach_many, HandlerId, EventMetrics, HConfig}, _From, State) ->
    Existing = State#state.handlers,

    Config0 = #{
        handlers => [
            {HandlerId, #{
                event_metrics => EventMetrics,
                config => HConfig
            }}
        ]
    },

    try
        Config = validate_config(Config0, Existing),
        L = attach_many(Config),
        Handlers = maps:merge(Existing, maps:from_list(L)),
        {reply, ok, State#state{handlers = Handlers}}

        catch
            error:Reason ->
                {reply, {error, Reason}, State}
    end;

handle_call({detach, HandlerId}, _From, State0) ->
    case maps:take(HandlerId, State0#state.handlers) of
        {Mod, Handlers} ->
            _ = code:purge(Mod),
            telemetry:detach(HandlerId),
            State = State0#state{handlers = Handlers},
            {reply, ok, State};
        error ->
            {reply, ok, State0}
    end;

handle_call(Event, From, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled call",
        event => Event,
        from => From
    }),
    {reply, {error, not_implemented}, State}.


handle_cast(Event, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled cast",
        event => Event
    }),
    {noreply, State}.


handle_info(Event, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled info",
        reason => Event
    }),
    {noreply, State}.


terminate(_Reason, _State) ->
    ?LOG_DEBUG("Terminating."),
    telemetry:detach(?MODULE).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
validate_config(Config, Existing) when is_list(Config) ->
    validate_config(maps:from_list(Config), Existing);

validate_config(#{handlers := _} = Config0, Existing) when is_map(Config0) ->
    Config = maps_utils:validate(Config0, ?CONFIG_SPEC),
    Ids = [Id || {Id, _} <- maps:get(handlers, Config, [])],
    lists:foreach(
        fun(Id) ->
            maps:is_key(Id, Existing) andalso throw({already_exists, Id}),
            ok
        end,
        Ids
    ),
    Config;

validate_config(_, _) ->
    maps:new().


%% @private
attach_many(Config) ->
    lists:foldl(
        fun({HandlerId, Mod}, Acc) ->
            case Mod:type() of
                callback ->
                    ok = attach_many(HandlerId, Mod),
                    [{HandlerId, Mod} | Acc];
                worker_pool ->
                    %% TODO
                    error({worker_pool, not_implemented})
            end
        end,
        [],
        gen_handlers(Config)
    ).


%% @private
attach_many(HandlerId, Mod) ->
    telemetry:attach_many(
        HandlerId,
        Mod:event_names(),
        fun Mod:handle_event/4,
        undefined
    ).



%% =============================================================================
%% PRIVATE: CODEGEN
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_convert_unit(Value, #{convert_unit := UnitOrUnits}) ->
    convert_unit(Value, UnitOrUnits);

maybe_convert_unit(Value, _) ->
    Value.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
convert_unit(Value, Unit) when ?IS_BYTE_UNIT(Unit) ->
    convert_unit(Value, {native, Unit});

convert_unit(Value, Unit) when ?IS_BYTE_UNIT(Unit) ->
    convert_unit(Value, {byte, Unit});

convert_unit(Value, {From, To}) when From == To ->
    Value;

convert_unit(Value, {From, To}) when ?IS_TIME_UNIT(From), ?IS_TIME_UNIT(To) ->
    %% erlang:convert_time_unit/3 always return an integer, so we do the math
    %% ourselves avoiding dividing by zero, returning a float.
    case erlang:convert_time_unit(1, From, To) of
        0 ->
            Value / erlang:convert_time_unit(1, To, From);
        R ->
            Value * R
    end;

convert_unit(Value, {From, To}) when ?IS_BYTE_UNIT(From), ?IS_BYTE_UNIT(To) ->
    Value * parts_per_megabyte(To) / parts_per_megabyte(From);

convert_unit(Value, Fun) when is_function(Fun, 1) ->
    Fun(Value).


%% @private
parts_per_megabyte(megabyte) -> 1;
parts_per_megabyte(kilobyte) -> 1000;
parts_per_megabyte(byte) -> 1000000.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
measurement(Measurements, _, #{measurement := Fun} = Config)
when is_function(Fun, 1) ->
    maybe_convert_unit(Fun(Measurements), Config);

measurement(Measurements, Metadata, #{measurement := Fun} = Config)
when is_function(Fun, 1) ->
    maybe_convert_unit(Fun(Measurements, Metadata), Config);

measurement(Measurements, _, #{measurement := Key} = Config) ->
    case maps:find(Key, Measurements) of
        {ok, Value} ->
            maybe_convert_unit(Value, Config);
        error ->
            throw({no_measurement, Key})
    end.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
metadata(Metadata, #{metadata := Keys}) when is_list(Keys)->
    maps:with(Keys, Metadata);

metadata(Metadata, #{metadata := ToMerge}) when is_map(ToMerge) ->
    maps:merge(Metadata, ToMerge);

metadata(Metadata, #{metadata := Fun}) when is_function(Fun, 1) ->
    Fun(Metadata).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
gen_handlers(#{handlers := Handlers}) ->
    try
        lists:foldl(
            fun({HandlerId, HConfig}, Acc) ->
                case gen_handler(HandlerId, HConfig) of
                    {ok, HandlerName} ->
                        [{HandlerId, HandlerName}|Acc];

                    {error, Reason} ->
                        %% Purge all handlers loaded so far
                        [code:purge(Mod) || {_, Mod} <- Acc],
                        %% Stop fold
                        throw({break, Reason})
                end
            end,
            [],
            Handlers
        )

    catch
        throw:{break, Reason} ->
            error(Reason)
    end;

gen_handlers(_) ->
    [].


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
gen_handler(HandlerId, HandlerConfig) when is_map(HandlerConfig) ->
    HandlerName = handler_name(HandlerId),
    Type = maps:get(type, HandlerConfig, callback),
    PoolType = maps:get(pool_type, HandlerConfig, undefined),
    PoolSize = maps:get(pool_size, HandlerConfig, 0),
    EventMetrics = maps:get(event_metrics, HandlerConfig, []),
    Buckets = maps:get(histogram_buckets, HandlerConfig, #{}),
    EventNames = [EventName || {EventName, _} <- EventMetrics],

    %% Purge first
    _ = code:purge(HandlerName),

    Forms = codegen:gen_module(
        {'$var', HandlerName},
        [
            %% API
            {event_names, 0},
            {handle_event, 4},
            {histogram_buckets, 0},
            {get_histogram_buckets, 1},
            {pool_size, 0},
            {pool_type, 0},
            {type, 0},
            %% GEN_SERVER CALLBACKS
            {init, 1},
            {handle_call, 3},
            {handle_cast, 2},
            {handle_info, 2},
            {code_change, 3},
            {terminate, 2},
            %% PRIVATE
            {do_handle_event, 4}
        ],
        [
            %% API
            {type,
                fun() -> {'$var', Type} end
            },
            {pool_type,
                fun() -> {'$var', PoolType} end
            },
            {pool_size,
                fun() -> {'$var', PoolSize} end
            },
            {event_names,
                fun() -> {'$var', EventNames} end
            },
            {histogram_buckets,
                fun() -> {'$var', Buckets} end
            },
            {get_histogram_buckets,
                fun(Name) ->
                   maps:get(Name, histogram_buckets(), ?DEFAULT_BUCKETS)
                end
            },
            {handle_event,
                fun(EventName, Measurements, Metadata, Config) ->
                    case {'$var', Type} of
                        callback ->
                            %% We synchronously call the callback
                            do_handle_event(
                                EventName, Measurements, Metadata, Config
                            );

                        worker_pool ->
                            %% TODO check pool type and select worker using
                            %% gproc_pool api
                            gen_server:cast(
                                {'$var', HandlerName},
                                {
                                    handle_event,
                                    EventName,
                                    Measurements,
                                    Metadata,
                                    Config
                                }
                            )
                    end
                end
            },
            %% GEN_SERVER CALLBACKS
            {init,
                fun(_) -> {ok, undefined} end
            },
            {handle_call,
                fun(_, _From, State) ->
                    {reply, {error, unknown_call}, State}
                end
            },
            {handle_cast,
                fun
                    ({handle_event, Name, Measures, Meta, Config}, State) ->
                        _ = catch ({'$var', HandlerName}):do_handle_event(
                            Name, Measures, Meta, Config
                        ),
                        {noreply, State};
                    (_, State) ->
                        {noreply, State}
                end
            },
            {handle_info,
                fun(_, State) -> {noreply, State} end
            },
            {code_change,
                fun(_, State, _) -> {ok, State} end
            },
            {terminate,
                fun(_, _) -> ok end
            },
            {do_handle_event,
                [
                    fun({'$var', EventName}, Measurements, Metadata, _Config) ->
                        try
                            _ = [
                                metric(Measurements, Metadata, Metric)
                                || Metric <- {'$var', Metrics}
                            ],
                            ok
                        catch
                            Class:Reason:Stacktrace ->
                                ?LOG_ERROR(#{
                                    description =>
                                        "Error while generating metric",
                                    class => Class,
                                    reason => Reason,
                                    stacktrace => Stacktrace
                                }),
                                ok
                        end
                    end
                    || {EventName, Metrics} <- EventMetrics
                ]
            },
            %% PRIVATE
            {metric,
                fun
                    (Measurements, Metadata, #{type := counter} = Config) ->
                        metrics:counter(#{
                            %% description => maps:get(description, Config, ""),
                            name => maps:get(name, Config),
                            delta => measurement(
                                Measurements, Metadata, Config
                            ),
                            label => metadata(Metadata, Config)
                        });

                    (Measurements, Metadata, #{type := gauge} = Config) ->
                        metrics:gauge(#{
                            %% description => maps:get(description, Config, ""),
                            name => maps:get(name, Config),
                            delta => measurement(
                                Measurements, Metadata, Config
                            ),
                            label => metadata(Metadata, Config)
                        });

                    (Measurements, Metadata, #{type := histogram} = Config) ->
                        Buckets =
                            case maps:find(buckets, Config) of
                                {ok, Name} when is_atom(Name) ->
                                    get_histogram_buckets(Name);

                                {ok, Val} when is_list(Val) ->
                                    Val;

                                error ->
                                    ?DEFAULT_BUCKETS
                            end,

                        metrics:histogram(#{
                            %% description => maps:get(description, Config, ""),
                            name => maps:get(name, Config),
                            buckets => Buckets,
                            value => measurement(
                                Measurements, Metadata, Config
                            ),
                            label => metadata(Metadata, Config)
                        })
                end
            },
            {maybe_convert_unit, fun maybe_convert_unit/2},
            {convert_unit, fun convert_unit/2},
            {parts_per_megabyte, fun parts_per_megabyte/1},
            {measurement, fun measurement/3},
            {metadata, fun metadata/2}
        ]
    ),

    case compile:forms(Forms) of
        {ok, HandlerName, Bin} ->
            {module, _} = code:load_binary(HandlerName, "nofile", Bin),
            ?LOG_INFO(#{
                description => "Telemetry Metrics handler generated and loaded",
                type => Type,
                handler_id => HandlerId,
                module => HandlerName
            }),
            {ok, HandlerName};

        {error, Errors, Warnings} ->
            {error, {codegen, #{errors => Errors, warnings => Warnings}}}
    end.

%% @private
handler_name(Term) when is_atom(Term) ->
    handler_name(atom_to_list(Term));

handler_name(Term) when is_binary(Term) ->
    handler_name(binary_to_list(Term));

handler_name(Term) when is_list(Term) ->
    list_to_atom("telemetry_metrics_handler_" ++ Term).




