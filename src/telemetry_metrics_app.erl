%%%-------------------------------------------------------------------
%% @doc telemetry_metrics public API
%% @end
%%%-------------------------------------------------------------------

-module(telemetry_metrics_app).

-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_StartType, _StartArgs) ->
    telemetry_metrics_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
