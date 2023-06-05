# telemetry_metrics
An Erlang application that subscribes to `telemetry` events and generates metrics using `shortishly/metrics` based on a declarative definition.

## Usage
The APIS allows you to define a dynamically generated module as a handler for a set of Telemetry Events with corrsponding metrics.

This is done declaratively via `sys.config` or by calling `telemetry_metrics` functions.

For example:
```erlang
```