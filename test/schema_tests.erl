-module(schema_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flatbuferl_records.hrl").

%% =============================================================================
%% Basic Parsing Tests
%% =============================================================================

simple_table_test() ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema("table Monster { name: string; hp: int; }"),
    ?assertMatch(#{'Monster' := #table_def{}}, Defs),
    #table_def{all_fields = AllFields} = maps:get('Monster', Defs),
    ?assertEqual(2, length(AllFields)).

table_with_defaults_test() ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema("table Monster { hp: int = 100; }"),
    #table_def{scalars = [#field_def{name = hp, type = int, default = 100, id = 0}]} = maps:get(
        'Monster', Defs
    ).

enum_test() ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema("enum Color : byte { Red, Green, Blue }"),
    {{enum, byte}, ['Red', 'Green', 'Blue'], IndexMap} = maps:get('Color', Defs),
    ?assertEqual(#{'Red' => 0, 'Green' => 1, 'Blue' => 2}, IndexMap).

enum_default_test() ->
    %% Enum field with default value - default should be atom, not binary
    Schema =
        "enum Color : ubyte { Red, Green, Blue }\n"
        "table Pixel { color: Color = Blue; }\n"
        "root_type Pixel;\n",
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema(Schema),
    #table_def{scalars = Scalars} = maps:get('Pixel', Defs),
    %% Default should be atom 'Blue', not binary <<"Blue">>
    ?assertMatch([#field_def{name = color, type = 'Color', default = 'Blue', id = 0}], Scalars).

enum_default_roundtrip_test() ->
    %% Full encode/decode roundtrip with enum default
    Schema =
        "enum Color : ubyte { Red, Green, Blue }\n"
        "table Pixel { x: int; color: Color = Blue; }\n"
        "root_type Pixel;\n",
    {ok, S} = flatbuferl:parse_schema(Schema),
    %% Encode with explicit enum value
    Data = #{x => 10, color => 'Green'},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, S)),
    Ctx = flatbuferl:new(Buffer, S),
    ?assertEqual('Green', flatbuferl:get(Ctx, [color])),
    %% Encode without color (should use default)
    Data2 = #{x => 20},
    Buffer2 = iolist_to_binary(flatbuferl:from_map(Data2, S)),
    Ctx2 = flatbuferl:new(Buffer2, S),
    ?assertEqual('Blue', flatbuferl:get(Ctx2, [color])).

union_test() ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema("union Animal { Dog, Cat }"),
    {union, ['Dog', 'Cat'], IndexMap} = maps:get('Animal', Defs),
    ?assertEqual(#{'Dog' => 1, 'Cat' => 2}, IndexMap).

vector_field_test() ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema("table Inventory { items: [string]; }"),
    #table_def{refs = [#field_def{name = items, type = {vector, string}, id = 0}]} = maps:get(
        'Inventory', Defs
    ).

%% =============================================================================
%% Options Tests
%% =============================================================================

namespace_test() ->
    {ok, {_Defs, Opts}} = flatbuferl:parse_schema("namespace MyGame; table X { }"),
    ?assertEqual('MyGame', maps:get(namespace, Opts)).

root_type_test() ->
    {ok, {_Defs, Opts}} = flatbuferl:parse_schema("table X { } root_type X;"),
    ?assertEqual('X', maps:get(root_type, Opts)).

file_identifier_test() ->
    {ok, {_Defs, Opts}} = flatbuferl:parse_schema("file_identifier \"TEST\"; table X { }"),
    ?assertEqual(<<"TEST">>, maps:get(file_identifier, Opts)).

%% =============================================================================
%% Field ID Assignment Tests
%% =============================================================================

sequential_ids_test() ->
    {ok, {Defs, _}} = flatbuferl:parse_schema("table T { a: int; b: int; c: int; }"),
    #table_def{scalars = Scalars} = maps:get('T', Defs),
    %% Pre-sorted by layout order (size desc, id desc). Same size = higher ID first
    [#field_def{name = c, id = 2}, #field_def{name = b, id = 1}, #field_def{name = a, id = 0}] =
        Scalars.

explicit_ids_test() ->
    {ok, {Defs, _}} = flatbuferl:parse_schema(
        "table T { a: int (id: 2); b: int (id: 0); c: int (id: 1); }"
    ),
    #table_def{scalars = Scalars} = maps:get('T', Defs),
    %% Pre-sorted by layout order (size desc, id desc)
    [#field_def{name = a, id = 2}, #field_def{name = c, id = 1}, #field_def{name = b, id = 0}] =
        Scalars.

mixed_ids_test() ->
    {ok, {Defs, _}} = flatbuferl:parse_schema(
        "table T { a: int (id: 0); b: int (id: 2); c: int; d: int; }"
    ),
    #table_def{scalars = Scalars} = maps:get('T', Defs),
    %% Pre-sorted by layout order (size desc, id desc)
    [
        #field_def{name = d, id = 3},
        #field_def{name = b, id = 2},
        #field_def{name = c, id = 1},
        #field_def{name = a, id = 0}
    ] = Scalars.

%% =============================================================================
%% Attribute Tests
%% =============================================================================

deprecated_attr_test() ->
    {ok, {Defs, _}} = flatbuferl:parse_schema("table T { old: int (deprecated); new: int; }"),
    #table_def{scalars = Scalars} = maps:get('T', Defs),
    %% Pre-sorted by layout_key (size desc, id desc). Same size = higher ID first
    [#field_def{name = new, deprecated = false}, #field_def{name = old, deprecated = true}] =
        Scalars.

multiple_attrs_test() ->
    {ok, {Defs, _}} = flatbuferl:parse_schema("table T { f: int (id: 5, deprecated); }"),
    #table_def{scalars = [#field_def{name = f, id = 5, deprecated = true}]} = maps:get('T', Defs).

%% =============================================================================
%% Complex Schema File Tests
%% =============================================================================

complex_schema_file_test() ->
    {ok, {Defs, Opts}} = flatbuferl:parse_schema_file("test/complex_schemas/game_state.fbs"),
    %% Check we got all 25 types
    ?assertEqual(25, maps:size(Defs)),
    %% Check options
    ?assertEqual(<<"gmst">>, maps:get(file_identifier, Opts)),
    ?assertEqual('GameStateRoot', maps:get(root_type, Opts)),
    ?assertEqual('DogeFB.GameState', maps:get(namespace, Opts)).
