%% @doc Test helper for creating and processing schemas
-module(test_schema_helper).
-export([schema/2, table/1, field/2, field/3, field_type/3, vector_type/1]).

-include("flatbuferl_records.hrl").

%% Create a processed schema from raw definitions
%% Usage: schema(#{name => table([field(a, int)])}, #{root_type => name})
-spec schema(map(), map()) -> {map(), map()}.
schema(Defs, Opts) ->
    flatbuferl_schema:process({Defs, Opts}).

%% Create raw table definition (tuple format for schema processing)
%% Usage: table([field(a, int), field(b, string)])
-spec table([{atom(), atom() | tuple()} | {atom(), atom() | tuple(), map()}]) -> {table, list()}.
table(Fields) ->
    {table, Fields}.

%% Create field tuple (for schema processing)
%% Usage: field(name, int) or field(name, {int, 0}) for default
-spec field(atom(), atom() | tuple()) -> {atom(), atom() | tuple()}.
field(Name, Type) ->
    {Name, Type}.

%% Usage: field(name, int, #{id => 1, deprecated => true})
-spec field(atom(), atom() | tuple(), map()) -> {atom(), atom() | tuple(), map()}.
field(Name, Type, Attrs) ->
    {Name, Type, Attrs}.

%% Get the resolved type for a field from a processed schema
%% Usage: field_type(Schema, test, a) -> int32
-spec field_type({map(), map()}, atom(), atom()) -> term().
field_type({Defs, _Opts}, TableName, FieldName) ->
    #table_def{field_map = FieldMap} = maps:get(TableName, Defs),
    #field_def{resolved_type = Type} = maps:get(FieldName, FieldMap),
    Type.

%% Create a vector_def for low-level API use
%% Usage: vector_type(int32) -> #vector_def{element_type = int32, ...}
-spec vector_type(atom()) -> #vector_def{}.
vector_type(ElemType) ->
    #vector_def{
        element_type = ElemType,
        is_primitive = is_primitive(ElemType),
        element_size = element_size(ElemType)
    }.

is_primitive(bool) -> true;
is_primitive(int8) -> true;
is_primitive(uint8) -> true;
is_primitive(int16) -> true;
is_primitive(uint16) -> true;
is_primitive(int32) -> true;
is_primitive(uint32) -> true;
is_primitive(int64) -> true;
is_primitive(uint64) -> true;
is_primitive(float32) -> true;
is_primitive(float64) -> true;
is_primitive(_) -> false.

element_size(bool) -> 1;
element_size(int8) -> 1;
element_size(uint8) -> 1;
element_size(int16) -> 2;
element_size(uint16) -> 2;
element_size(int32) -> 4;
element_size(uint32) -> 4;
element_size(int64) -> 8;
element_size(uint64) -> 8;
element_size(float32) -> 4;
element_size(float64) -> 8;
element_size(string) -> 4;
element_size(_) -> 4.
