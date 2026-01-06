-module(schema_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Basic Parsing Tests
%% =============================================================================

simple_table_test() ->
    {ok, {Defs, _Opts}} = schema:parse("table Monster { name: string; hp: int; }"),
    ?assertMatch(#{'Monster' := {table, _}}, Defs),
    {table, Fields} = maps:get('Monster', Defs),
    ?assertEqual(2, length(Fields)).

table_with_defaults_test() ->
    {ok, {Defs, _Opts}} = schema:parse("table Monster { hp: int = 100; }"),
    {table, [{hp, {int, 100}, #{id := 0}}]} = maps:get('Monster', Defs).

enum_test() ->
    {ok, {Defs, _Opts}} = schema:parse("enum Color : byte { Red, Green, Blue }"),
    ?assertEqual({{enum, byte}, ['Red', 'Green', 'Blue']}, maps:get('Color', Defs)).

union_test() ->
    {ok, {Defs, _Opts}} = schema:parse("union Animal { Dog, Cat }"),
    ?assertEqual({union, ['Dog', 'Cat']}, maps:get('Animal', Defs)).

vector_field_test() ->
    {ok, {Defs, _Opts}} = schema:parse("table Inventory { items: [string]; }"),
    {table, [{items, {vector, string}, #{id := 0}}]} = maps:get('Inventory', Defs).

%% =============================================================================
%% Options Tests
%% =============================================================================

namespace_test() ->
    {ok, {_Defs, Opts}} = schema:parse("namespace MyGame; table X { }"),
    ?assertEqual('MyGame', maps:get(namespace, Opts)).

root_type_test() ->
    {ok, {_Defs, Opts}} = schema:parse("table X { } root_type X;"),
    ?assertEqual('X', maps:get(root_type, Opts)).

file_identifier_test() ->
    {ok, {_Defs, Opts}} = schema:parse("file_identifier \"TEST\"; table X { }"),
    ?assertEqual(<<"TEST">>, maps:get(file_identifier, Opts)).

%% =============================================================================
%% Field ID Assignment Tests
%% =============================================================================

sequential_ids_test() ->
    {ok, {Defs, _}} = schema:parse("table T { a: int; b: int; c: int; }"),
    {table, Fields} = maps:get('T', Defs),
    ?assertEqual(
        [
            {a, int, #{id => 0}},
            {b, int, #{id => 1}},
            {c, int, #{id => 2}}
        ],
        Fields
    ).

explicit_ids_test() ->
    {ok, {Defs, _}} = schema:parse("table T { a: int (id: 2); b: int (id: 0); c: int (id: 1); }"),
    {table, Fields} = maps:get('T', Defs),
    %% Fields keep original order, IDs as specified
    [{a, int, #{id := 2}}, {b, int, #{id := 0}}, {c, int, #{id := 1}}] = Fields.

mixed_ids_test() ->
    {ok, {Defs, _}} = schema:parse("table T { a: int (id: 0); b: int (id: 2); c: int; d: int; }"),
    {table, Fields} = maps:get('T', Defs),
    %% c and d should fill gaps and continue after explicit IDs
    [
        {a, int, #{id := 0}},
        {b, int, #{id := 2}},
        %% fills gap
        {c, int, #{id := 1}},
        %% continues after 2
        {d, int, #{id := 3}}
    ] =
        Fields.

%% =============================================================================
%% Attribute Tests
%% =============================================================================

deprecated_attr_test() ->
    {ok, {Defs, _}} = schema:parse("table T { old: int (deprecated); new: int; }"),
    {table, Fields} = maps:get('T', Defs),
    [
        {old, int, #{deprecated := true, id := _}},
        {new, int, #{id := _}}
    ] = Fields.

multiple_attrs_test() ->
    {ok, {Defs, _}} = schema:parse("table T { f: int (id: 5, deprecated); }"),
    {table, [{f, int, #{id := 5, deprecated := true}}]} = maps:get('T', Defs).

%% =============================================================================
%% Complex Schema File Tests
%% =============================================================================

complex_schema_file_test() ->
    {ok, {Defs, Opts}} = schema:parse_file("test/complex_schemas/game_state.fbs"),
    %% Check we got all 25 types
    ?assertEqual(25, maps:size(Defs)),
    %% Check options
    ?assertEqual(<<"gmst">>, maps:get(file_identifier, Opts)),
    ?assertEqual('GameStateRoot', maps:get(root_type, Opts)),
    ?assertEqual('DogeFB.GameState', maps:get(namespace, Opts)).
