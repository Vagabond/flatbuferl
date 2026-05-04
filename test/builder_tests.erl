-module(builder_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flatbuferl_records.hrl").

-import(test_schema_helper, [schema/2, table/1, field/2, field/3, field_type/3]).

%% =============================================================================
%% Simple Scalar Tests
%% =============================================================================

simple_int_test() ->
    Schema = schema(#{test => table([field(a, int)])}, #{
        root_type => test, file_identifier => <<"TEST">>
    }),
    Map = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    ?assertEqual(<<"TEST">>, flatbuferl_reader:get_file_id(Buffer)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, 42}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, a), Buffer)
    ).

two_ints_test() ->
    Schema = schema(#{test => table([field(a, int), field(b, int, #{id => 1})])}, #{
        root_type => test, file_identifier => <<"TEST">>
    }),
    Map = #{a => 10, b => 20},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, 10}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, a), Buffer)
    ),
    ?assertEqual(
        {ok, 20}, flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, b), Buffer)
    ).

skip_default_value_test() ->
    Schema = schema(#{test => table([field(a, {int, 100}), field(b, int, #{id => 1})])}, #{
        root_type => test
    }),
    %% a has default value, should be skipped
    Map = #{a => 100, b => 20},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    %% a not written
    ?assertEqual(
        missing, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, a), Buffer)
    ),
    ?assertEqual(
        {ok, 20}, flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, b), Buffer)
    ).

non_contiguous_field_ids_test() ->
    %% Field IDs 0 and 2 (gap at 1)
    Schema = schema(#{test => table([field(a, int), field(c, int, #{id => 2})])}, #{
        root_type => test
    }),
    Map = #{a => 10, c => 30},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, 10}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, a), Buffer)
    ),
    %% No field 1 - use int32 as there's no field to look up
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 1, int32, Buffer)),
    ?assertEqual(
        {ok, 30}, flatbuferl_reader:get_field(Root, 2, field_type(Schema, test, c), Buffer)
    ).

%% =============================================================================
%% Different Scalar Types
%% =============================================================================

bool_test() ->
    Schema = schema(#{test => table([field(flag, bool)])}, #{root_type => test}),
    Map = #{flag => true},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, true}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, flag), Buffer)
    ).

byte_test() ->
    Schema = schema(#{test => table([field(val, byte)])}, #{root_type => test}),
    Map = #{val => -42},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, -42}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, val), Buffer)
    ).

short_test() ->
    Schema = schema(#{test => table([field(val, short)])}, #{root_type => test}),
    Map = #{val => -1000},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, -1000}, flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, val), Buffer)
    ).

long_test() ->
    Schema = schema(#{test => table([field(val, long)])}, #{root_type => test}),
    Map = #{val => 9000000000000},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, 9000000000000},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, val), Buffer)
    ).

float_test() ->
    Schema = schema(#{test => table([field(val, float)])}, #{root_type => test}),
    Map = #{val => 3.14},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, V} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, val), Buffer),
    ?assert(abs(V - 3.14) < 0.001).

double_test() ->
    Schema = schema(#{test => table([field(val, double)])}, #{root_type => test}),
    Map = #{val => 2.718281828},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, V} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, val), Buffer),
    ?assert(abs(V - 2.718281828) < 0.0000001).

%% =============================================================================
%% String Tests
%% =============================================================================

simple_string_test() ->
    Schema = schema(#{test => table([field(name, string)])}, #{
        root_type => test, file_identifier => <<"TEST">>
    }),
    Map = #{name => <<"hello">>},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    ?assertEqual(<<"TEST">>, flatbuferl_reader:get_file_id(Buffer)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, <<"hello">>},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, name), Buffer)
    ).

string_and_int_test() ->
    Schema = schema(#{test => table([field(name, string), field(val, int, #{id => 1})])}, #{
        root_type => test
    }),
    Map = #{name => <<"world">>, val => 42},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, <<"world">>},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, name), Buffer)
    ),
    ?assertEqual(
        {ok, 42}, flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, val), Buffer)
    ).

%% =============================================================================
%% Vector Tests
%% =============================================================================

int_vector_test() ->
    Schema = schema(#{test => table([field(nums, {vector, int})])}, #{root_type => test}),
    Map = #{nums => [1, 2, 3]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, [1, 2, 3]},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, nums), Buffer)
    ).

string_vector_test() ->
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{root_type => test}),
    Map = #{items => [<<"a">>, <<"bb">>, <<"ccc">>]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, [<<"a">>, <<"bb">>, <<"ccc">>]},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, items), Buffer)
    ).

mixed_with_vector_test() ->
    Schema = schema(
        #{test => table([field(name, string), field(scores, {vector, int}, #{id => 1})])}, #{
            root_type => test
        }
    ),
    Map = #{name => <<"test">>, scores => [10, 20, 30]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, <<"test">>},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, name), Buffer)
    ),
    ?assertEqual(
        {ok, [10, 20, 30]},
        flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, scores), Buffer)
    ).

%% =============================================================================
%% Nested Table Tests
%% =============================================================================

nested_table_test() ->
    Schema = schema(
        #{
            'Inner' => table([field(value, int)]),
            'Outer' => table([field(name, string), field(inner, 'Inner', #{id => 1})])
        },
        #{root_type => 'Outer'}
    ),
    Map = #{name => <<"outer">>, inner => #{value => 42}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, <<"outer">>},
        flatbuferl_reader:get_field(Root, 0, field_type(Schema, 'Outer', name), Buffer)
    ),
    {ok, InnerRef} = flatbuferl_reader:get_field(
        Root, 1, field_type(Schema, 'Outer', inner), Buffer
    ),
    ?assertEqual(
        {ok, 42},
        flatbuferl_reader:get_field(InnerRef, 0, field_type(Schema, 'Inner', value), Buffer)
    ).

nested_with_string_test() ->
    Schema = schema(
        #{
            'Child' => table([field(name, string), field(age, int, #{id => 1})]),
            'Parent' => table([field(child, 'Child')])
        },
        #{root_type => 'Parent'}
    ),
    Map = #{child => #{name => <<"Alice">>, age => 30}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, ChildRef} = flatbuferl_reader:get_field(
        Root, 0, field_type(Schema, 'Parent', child), Buffer
    ),
    ?assertEqual(
        {ok, <<"Alice">>},
        flatbuferl_reader:get_field(ChildRef, 0, field_type(Schema, 'Child', name), Buffer)
    ),
    ?assertEqual(
        {ok, 30}, flatbuferl_reader:get_field(ChildRef, 1, field_type(Schema, 'Child', age), Buffer)
    ).

%% =============================================================================
%% Struct Tests
%% =============================================================================

simple_struct_test() ->
    %% Struct Vec2 with two floats (8 bytes inline)
    Schema = schema(
        #{
            'Vec2' => {struct, [{x, float}, {y, float}]},
            test => table([field(pos, 'Vec2')])
        },
        #{root_type => test}
    ),
    Map = #{pos => #{x => 1.0, y => 2.0}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, pos), Buffer),
    ?assertEqual(1.0, maps:get(x, Struct)),
    ?assertEqual(2.0, maps:get(y, Struct)).

nested_struct_test() ->
    %% Struct containing another struct (Rect = two Vec2s = 16 bytes)
    Schema = schema(
        #{
            'Vec2' => {struct, [{x, float}, {y, float}]},
            'Rect' => {struct, [{min, 'Vec2'}, {max, 'Vec2'}]},
            test => table([field(bounds, 'Rect'), field(label, string, #{id => 1})])
        },
        #{root_type => test}
    ),
    Map = #{
        bounds => #{min => #{x => 1.0, y => 2.0}, max => #{x => 3.0, y => 4.0}},
        label => <<"test">>
    },
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, bounds), Buffer),
    Min = maps:get(min, Struct),
    Max = maps:get(max, Struct),
    ?assertEqual(1.0, maps:get(x, Min)),
    ?assertEqual(2.0, maps:get(y, Min)),
    ?assertEqual(3.0, maps:get(x, Max)),
    ?assertEqual(4.0, maps:get(y, Max)),
    ?assertEqual(
        {ok, <<"test">>},
        flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, label), Buffer)
    ).

struct_with_int_and_float_test() ->
    %% Struct with mixed types to test alignment (12 bytes with alignment)
    Schema = schema(
        #{
            'Mixed' => {struct, [{a, byte}, {b, float}, {c, short}]},
            test => table([field(data, 'Mixed')])
        },
        #{root_type => test}
    ),
    Map = #{data => #{a => 10, b => 3.14, c => 1000}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, data), Buffer),
    ?assertEqual(10, maps:get(a, Struct)),
    {ok, BVal} = maps:find(b, Struct),
    ?assert(abs(BVal - 3.14) < 0.001),
    ?assertEqual(1000, maps:get(c, Struct)).

struct_and_scalar_test() ->
    %% Table with both a struct and a regular scalar
    Schema = schema(
        #{
            'Vec2' => {struct, [{x, float}, {y, float}]},
            test => table([field(pos, 'Vec2'), field(name, string, #{id => 1})])
        },
        #{root_type => test}
    ),
    Map = #{pos => #{x => 5.0, y => 10.0}, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, pos), Buffer),
    ?assertEqual(5.0, maps:get(x, Struct)),
    ?assertEqual(10.0, maps:get(y, Struct)),
    ?assertEqual(
        {ok, <<"test">>},
        flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, name), Buffer)
    ).

%% =============================================================================
%% Union Tests
%% =============================================================================

simple_union_test() ->
    %% Parse the union schema
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_field.fbs"),

    %% Build a buffer with hello variant (flatc-compatible format)
    Map = #{
        data_type => hello,
        data => #{salute => <<"hi there">>},
        additions_value => 42
    },
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    %% Verify file identifier
    ?assertEqual(<<"cmnd">>, flatbuferl_reader:get_file_id(Buffer)),

    %% Decode and verify
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(additions_value, Result)),
    ?assertEqual(hello, maps:get(data_type, Result)),
    ?assertEqual(#{salute => <<"hi there">>}, maps:get(data, Result)).

union_bye_variant_test() ->
    %% Test the 'bye' variant of the union
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_field.fbs"),

    Map = #{data_type => bye, data => #{greeting => 123}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(bye, maps:get(data_type, Result)),
    ?assertEqual(#{greeting => 123}, maps:get(data, Result)).

%% =============================================================================
%% JSON Roundtrip Tests
%% =============================================================================

json_roundtrip_simple_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_monster.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Original = flatbuferl:to_map(Ctx),

    %% Encode to JSON (atom keys) -> decode (binary keys) -> from_map -> to_map
    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = iolist_to_binary(flatbuferl:from_map(Decoded, Schema)),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    Result = flatbuferl:to_map(NewCtx),

    ?assertEqual(maps:get(name, Original), maps:get(name, Result)),
    ?assertEqual(maps:get(hp, Original), maps:get(hp, Result)),
    ?assertEqual(maps:get(mana, Original), maps:get(mana, Result)).

json_roundtrip_nested_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_nested.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_nested.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Original = flatbuferl:to_map(Ctx),

    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = iolist_to_binary(flatbuferl:from_map(Decoded, Schema)),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    Result = flatbuferl:to_map(NewCtx),

    ?assertEqual(maps:get(name, Original), maps:get(name, Result)),
    ?assertEqual(maps:get(hp, Original), maps:get(hp, Result)),
    OrigPos = maps:get(pos, Original),
    ResultPos = maps:get(pos, Result),
    ?assertEqual(maps:get(x, OrigPos), maps:get(x, ResultPos)),
    ?assertEqual(maps:get(y, OrigPos), maps:get(y, ResultPos)),
    ?assertEqual(maps:get(z, OrigPos), maps:get(z, ResultPos)).

json_roundtrip_vectors_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_vector.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Original = flatbuferl:to_map(Ctx),

    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = iolist_to_binary(flatbuferl:from_map(Decoded, Schema)),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    Result = flatbuferl:to_map(NewCtx),

    ?assertEqual(maps:get(counts, Original), maps:get(counts, Result)),
    ?assertEqual(maps:get(items, Original), maps:get(items, Result)).

%% =============================================================================
%% Flatc Roundtrip Tests (validates output against official implementation)
%% =============================================================================

flatc_roundtrip_monster_test() ->
    %% Read original, modify, write new buffer, verify flatc can decode it
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_monster.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Map = flatbuferl:to_map(Ctx),

    %% Modify the map
    Modified = Map#{name => <<"Troll">>, hp => 200, mana => 75},

    %% Build new buffer
    NewBuffer = flatbuferl:from_map(Modified, Schema),
    TmpBin = "/tmp/flatbuferl_test_monster.bin",
    TmpJson = "/tmp/flatbuferl_test_monster.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode our buffer (--strict-json for proper JSON)
    Cmd = io_lib:format(
        "flatc --json --strict-json -o /tmp test/vectors/test_monster.fbs -- ~s 2>&1", [TmpBin]
    ),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify values
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assertEqual(<<"Troll">>, maps:get(<<"name">>, Decoded)),
    ?assertEqual(200, maps:get(<<"hp">>, Decoded)),
    ?assertEqual(75, maps:get(<<"mana">>, Decoded)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_roundtrip_nested_test() ->
    %% Test nested table roundtrip with flatc
    {ok, Buffer} = file:read_file("test/vectors/test_nested.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_nested.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Map = flatbuferl:to_map(Ctx),

    %% Modify nested values
    Modified = Map#{
        name => <<"Enemy">>,
        hp => 50,
        pos => #{x => 10.0, y => 20.0, z => 30.0}
    },

    %% Build new buffer
    NewBuffer = flatbuferl:from_map(Modified, Schema),
    TmpBin = "/tmp/flatbuferl_test_nested.bin",
    TmpJson = "/tmp/flatbuferl_test_nested.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode
    Cmd = io_lib:format(
        "flatc --json --strict-json -o /tmp test/vectors/test_nested.fbs -- ~s 2>&1", [TmpBin]
    ),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify values
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assertEqual(<<"Enemy">>, maps:get(<<"name">>, Decoded)),
    ?assertEqual(50, maps:get(<<"hp">>, Decoded)),
    Pos = maps:get(<<"pos">>, Decoded),
    ?assertEqual(10.0, maps:get(<<"x">>, Pos)),
    ?assertEqual(20.0, maps:get(<<"y">>, Pos)),
    ?assertEqual(30.0, maps:get(<<"z">>, Pos)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_roundtrip_vectors_test() ->
    %% Test vector roundtrip with flatc
    {ok, Buffer} = file:read_file("test/vectors/test_vector.bin"),
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    Map = flatbuferl:to_map(Ctx),

    %% Modify vectors
    Modified = Map#{
        counts => [10, 20, 30],
        items => [<<"axe">>, <<"bow">>]
    },

    %% Build new buffer
    NewBuffer = flatbuferl:from_map(Modified, Schema),
    TmpBin = "/tmp/flatbuferl_test_vector.bin",
    TmpJson = "/tmp/flatbuferl_test_vector.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode
    Cmd = io_lib:format(
        "flatc --json --strict-json -o /tmp test/vectors/test_vector.fbs -- ~s 2>&1", [TmpBin]
    ),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify values
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assertEqual([10, 20, 30], maps:get(<<"counts">>, Decoded)),
    ?assertEqual([<<"axe">>, <<"bow">>], maps:get(<<"items">>, Decoded)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpJson).

%% =============================================================================
%% String Deduplication Tests
%% =============================================================================

string_dedup_vector_test() ->
    %% Vector with duplicate strings should be smaller than with unique strings
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{root_type => test}),

    %% 3 duplicate strings
    MapDup = #{items => [<<"same">>, <<"same">>, <<"same">>]},
    BufferDup = iolist_to_binary(flatbuferl_builder:from_map(MapDup, Schema)),

    %% 3 unique strings of same length
    MapUniq = #{items => [<<"aaaa">>, <<"bbbb">>, <<"cccc">>]},
    BufferUniq = iolist_to_binary(flatbuferl_builder:from_map(MapUniq, Schema)),

    %% Duplicate buffer should be smaller (dedup saves 2 string copies)
    ?assert(byte_size(BufferDup) < byte_size(BufferUniq)),

    %% Verify decoding works correctly
    Root = flatbuferl_reader:get_root(BufferDup),
    {ok, Items} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, items), BufferDup),
    ?assertEqual([<<"same">>, <<"same">>, <<"same">>], Items).

string_dedup_preserves_order_test() ->
    %% Test that dedup preserves order with mixed duplicates
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{root_type => test}),
    Map = #{items => [<<"a">>, <<"b">>, <<"a">>, <<"c">>, <<"b">>, <<"a">>]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Items} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, items), Buffer),
    ?assertEqual([<<"a">>, <<"b">>, <<"a">>, <<"c">>, <<"b">>, <<"a">>], Items).

string_dedup_empty_vector_test() ->
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{root_type => test}),
    Map = #{items => []},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Items} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, items), Buffer),
    ?assertEqual([], Items).

string_dedup_single_test() ->
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{root_type => test}),
    Map = #{items => [<<"only">>]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Items} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, items), Buffer),
    ?assertEqual([<<"only">>], Items).

string_dedup_flatc_compat_test() ->
    %% Test that deduplicated buffers are valid per flatc
    Schema = schema(#{test => table([field(items, {vector, string})])}, #{
        root_type => test, file_identifier => <<"TEST">>
    }),
    Map = #{items => [<<"hello">>, <<"world">>, <<"hello">>]},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),

    TmpBin = "/tmp/flatbuferl_dedup_test.bin",
    TmpSchema = "/tmp/flatbuferl_dedup_test.fbs",
    TmpJson = "/tmp/flatbuferl_dedup_test.json",

    %% Write schema
    SchemaStr = "file_identifier \"TEST\";\ntable test { items: [string]; }\nroot_type test;\n",
    ok = file:write_file(TmpSchema, SchemaStr),
    ok = file:write_file(TmpBin, Buffer),

    %% Use flatc to decode
    Cmd = io_lib:format("flatc --json --strict-json -o /tmp ~s -- ~s 2>&1", [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify values
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assertEqual([<<"hello">>, <<"world">>, <<"hello">>], maps:get(<<"items">>, Decoded)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpSchema),
    file:delete(TmpJson).

%% =============================================================================
%% Fixed Array Tests
%% =============================================================================

binary_as_uint8_array_test() ->
    %% Test that a binary can be used directly for [uint8:N] arrays
    %% uint8 returns list (use ubyte for binary)
    Schema = schema(
        #{
            'Hash' => {struct, [{data, {array, uint8, 32}}]},
            test => table([field(hash, 'Hash')])
        },
        #{root_type => test}
    ),
    Bin = crypto:strong_rand_bytes(32),
    Map = #{hash => #{data => Bin}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, hash), Buffer),
    ?assertEqual(binary_to_list(Bin), maps:get(data, Struct)).

binary_as_int8_array_test() ->
    %% Test that a binary can be used directly for [int8:N] arrays
    %% int8 returns list (use byte for binary)
    Schema = schema(
        #{
            'Data' => {struct, [{bytes, {array, int8, 16}}]},
            test => table([field(data, 'Data')])
        },
        #{root_type => test}
    ),
    Bin = crypto:strong_rand_bytes(16),
    Map = #{data => #{bytes => Bin}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, data), Buffer),
    %% int8 array gets decoded as list of signed integers
    ?assertEqual(16, length(maps:get(bytes, Struct))).

binary_as_byte_array_test() ->
    %% Test that [byte:N] returns binary directly
    Schema = schema(
        #{
            'Hash' => {struct, [{data, {array, byte, 32}}]},
            test => table([field(hash, 'Hash')])
        },
        #{root_type => test}
    ),
    Bin = crypto:strong_rand_bytes(32),
    Map = #{hash => #{data => Bin}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, hash), Buffer),
    %% byte returns binary directly
    ?assertEqual(Bin, maps:get(data, Struct)).

binary_as_ubyte_array_test() ->
    %% Test that [ubyte:N] returns binary directly
    Schema = schema(
        #{
            'Data' => {struct, [{bytes, {array, ubyte, 16}}]},
            test => table([field(data, 'Data')])
        },
        #{root_type => test}
    ),
    Bin = crypto:strong_rand_bytes(16),
    Map = #{data => #{bytes => Bin}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, data), Buffer),
    %% ubyte returns binary directly
    ?assertEqual(Bin, maps:get(bytes, Struct)).

%% =============================================================================
%% Struct Array Sizing Tests
%% =============================================================================
%% Regression tests for primitive_type_size/1 returning wrong sizes for
%% {array, ElemType, Count} tuples. The catch-all clause returned 4 for any
%% unrecognized type, causing structs with fixed-size arrays > 4 bytes to
%% have incorrect total_size, corrupting vtable tbl_size and all subsequent
%% field offsets.

struct_array_vtable_size_test() ->
    %% Verify the vtable tbl_size is correct for a table with a 32-byte struct.
    %% Before the fix, tbl_size was 16 (treated array as 4 bytes) instead of 44.
    Schema = schema(
        #{
            'Checksum' => {struct, [{bytes, {array, uint8, 32}}]},
            test => table([
                field(name, string),
                field(version, string, #{id => 1}),
                field(hash, 'Checksum', #{id => 2})
            ])
        },
        #{root_type => test}
    ),
    Map = #{name => <<"pkg">>, version => <<"1.0">>, hash => #{bytes => lists:seq(1, 32)}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    %% Read root table vtable
    <<RootOff:32/little, _/binary>> = Buffer,
    <<_:RootOff/binary, SOffset:32/little-signed, _/binary>> = Buffer,
    VTPos = RootOff - SOffset,
    <<_:VTPos/binary, _VTSize:16/little, TblSize:16/little, _/binary>> = Buffer,
    %% tbl_size = 4 (soffset) + 4 (name uoffset) + 4 (version uoffset) + 32 (Checksum) = 44
    ?assertEqual(44, TblSize).

struct_array_roundtrip_test() ->
    %% Roundtrip: table with 32-byte struct array field and strings
    Schema = schema(
        #{
            'Checksum' => {struct, [{bytes, {array, uint8, 32}}]},
            test => table([
                field(name, string),
                field(version, string, #{id => 1}),
                field(hash, 'Checksum', #{id => 2})
            ])
        },
        #{root_type => test}
    ),
    HashBytes = lists:seq(1, 32),
    Map = #{name => <<"test:pkg">>, version => <<"2.0.0">>, hash => #{bytes => HashBytes}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, <<"test:pkg">>} = flatbuferl_reader:get_field(
        Root, 0, field_type(Schema, test, name), Buffer
    ),
    {ok, <<"2.0.0">>} = flatbuferl_reader:get_field(
        Root, 1, field_type(Schema, test, version), Buffer
    ),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 2, field_type(Schema, test, hash), Buffer),
    ?assertEqual(HashBytes, maps:get(bytes, Struct)).

struct_array_with_trailing_scalar_test() ->
    %% Struct with array followed by a scalar — both must be sized correctly
    Schema = schema(
        #{
            'Tagged' => {struct, [{data, {array, ubyte, 16}}, {tag, uint32}]},
            test => table([field(item, 'Tagged'), field(label, string, #{id => 1})])
        },
        #{root_type => test}
    ),
    Bin = crypto:strong_rand_bytes(16),
    Map = #{item => #{data => Bin, tag => 99}, label => <<"hello">>},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, item), Buffer),
    ?assertEqual(Bin, maps:get(data, Struct)),
    ?assertEqual(99, maps:get(tag, Struct)),
    ?assertEqual(
        {ok, <<"hello">>},
        flatbuferl_reader:get_field(Root, 1, field_type(Schema, test, label), Buffer)
    ).

struct_double_array_test() ->
    %% Struct with [double:4] — 8-byte elements, 32 bytes total
    Schema = schema(
        #{
            'Matrix' => {struct, [{vals, {array, double, 4}}]},
            test => table([field(m, 'Matrix')])
        },
        #{root_type => test}
    ),
    Map = #{m => #{vals => [1.0, 2.0, 3.0, 4.0]}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, m), Buffer),
    ?assertEqual([1.0, 2.0, 3.0, 4.0], maps:get(vals, Struct)).

struct_array_nested_in_table_test() ->
    %% Struct with array inside a nested table (not root) — exercises
    %% calc_ref_align_padding path with struct array sizing
    Schema = schema(
        #{
            'Checksum' => {struct, [{bytes, {array, ubyte, 32}}]},
            'Ref' => table([
                field(pkg, string),
                field(hash, 'Checksum', #{id => 1})
            ]),
            test => table([field(name, string), field(ref, 'Ref', #{id => 1})])
        },
        #{root_type => test}
    ),
    HashBin = crypto:strong_rand_bytes(32),
    Map = #{name => <<"outer">>, ref => #{pkg => <<"inner:pkg">>, hash => #{bytes => HashBin}}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Decoded = flatbuferl:to_map(Ctx),
    ?assertEqual(<<"outer">>, maps:get(name, Decoded)),
    Ref = maps:get(ref, Decoded),
    ?assertEqual(<<"inner:pkg">>, maps:get(pkg, Ref)),
    ?assertEqual(HashBin, maps:get(bytes, maps:get(hash, Ref))).

%% =============================================================================
%% Deep Nested Struct Tests
%% =============================================================================

three_level_nested_struct_test() ->
    %% Struct A contains struct B contains struct C (3 levels).
    %% Tests that re-enrichment fixpoint resolves all nesting levels.
    Schema = schema(
        #{
            'Point' => {struct, [{x, float}, {y, float}]},
            'Segment' => {struct, [{start, 'Point'}, {finish, 'Point'}]},
            'Path' => {struct, [{seg, 'Segment'}, {weight, float}]},
            test => table([field(route, 'Path')])
        },
        #{root_type => test}
    ),
    Map = #{
        route => #{
            seg => #{
                start => #{x => 1.0, y => 2.0},
                finish => #{x => 3.0, y => 4.0}
            },
            weight => 0.5
        }
    },
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Path} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, route), Buffer),
    Seg = maps:get(seg, Path),
    Start = maps:get(start, Seg),
    Finish = maps:get(finish, Seg),
    ?assertEqual(1.0, maps:get(x, Start)),
    ?assertEqual(2.0, maps:get(y, Start)),
    ?assertEqual(3.0, maps:get(x, Finish)),
    ?assertEqual(4.0, maps:get(y, Finish)),
    {ok, W} = maps:find(weight, Path),
    ?assert(abs(W - 0.5) < 0.001).

%% =============================================================================
%% Vtable-Sharing Path Tests
%% =============================================================================

vtable_sharing_with_u64_test() ->
    %% Force vtable-sharing (encode_root_vtable_after) by making the root
    %% table have the same vtable as a nested table. Both have a uint64
    %% and a string field in the same order/types, so their vtables match.
    %% The root's vtable is placed after the nested table's, triggering
    %% the vtable-sharing path with 8-byte alignment adjustment.
    Schema = schema(
        #{
            'Inner' => table([field(ts, uint64), field(label, string, #{id => 1})]),
            test => table([field(ts, uint64), field(child, 'Inner', #{id => 1})])
        },
        #{root_type => test}
    ),
    Map = #{ts => 1710000000000, child => #{ts => 1710003600000, label => <<"inner">>}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Decoded = flatbuferl:to_map(Ctx),
    ?assertEqual(1710000000000, maps:get(ts, Decoded)),
    ?assertEqual(1710003600000, maps:get(ts, maps:get(child, Decoded))),
    %% Verify the root u64 is 8-byte aligned
    <<RootOff:32/little, _/binary>> = Buffer,
    %% Root table's ts field should be at an 8-byte aligned position
    <<_:RootOff/binary, SOffset:32/little-signed, _/binary>> = Buffer,
    VTPos = RootOff - SOffset,
    <<_:VTPos/binary, _VTSize:16/little, _TblSize:16/little, TsFieldOffset:16/little, _/binary>> =
        Buffer,
    TsAbsPos = RootOff + TsFieldOffset,
    ?assertEqual(
        0,
        TsAbsPos rem 8,
        lists:flatten(
            io_lib:format(
                "Root u64 at abs ~p must be 8-byte aligned (vtable-sharing path)",
                [TsAbsPos]
            )
        )
    ).

%% =============================================================================
%% Union with 8-byte Field Tests
%% =============================================================================

union_member_with_u64_alignment_test() ->
    %% Union member table containing uint64 fields, nested inside a non-root
    %% table. The union must be a ref field of a nested table (not the root)
    %% to exercise the #union_value_def{} clause in nested_table_first_8byte_offset,
    %% which is only reached via encode_refs_with_positions -> calc_ref_align_padding.
    {ok, Schema} = flatbuferl:parse_schema(
        <<
            "\n"
            "        table MoveAction { x: int; y: int; }\n"
            "        table TimedAction { timestamp: uint64; duration: uint64; label: string; }\n"
            "        union Action { MoveAction, TimedAction }\n"
            "        table Event { name: string; action: Action; }\n"
            "        table Log { id: uint64; event: Event; }\n"
            "        root_type Log;\n"
            "    "
        >>
    ),
    Map = #{
        id => 42,
        event => #{
            name => <<"tick">>,
            action_type => 'TimedAction',
            action => #{timestamp => 1710000000000, duration => 3600000, label => <<"test">>}
        }
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Decoded = flatbuferl:to_map(Ctx),
    ?assertEqual(42, maps:get(id, Decoded)),
    Event = maps:get(event, Decoded),
    ?assertEqual(<<"tick">>, maps:get(name, Event)),
    ?assertEqual('TimedAction', maps:get(action_type, Event)),
    Action = maps:get(action, Event),
    ?assertEqual(1710000000000, maps:get(timestamp, Action)),
    ?assertEqual(3600000, maps:get(duration, Action)),
    ?assertEqual(<<"test">>, maps:get(label, Action)).

%% =============================================================================
%% Struct with Enum Field Tests
%% =============================================================================

struct_with_enum_field_test() ->
    %% Struct containing an enum field. Exercises resolve_struct_field's
    %% #enum_def{} clause during Phase 1b re-enrichment.
    Schema = schema(
        #{
            'Channel' => {{enum, ubyte}, ['Red', 'Green', 'Blue']},
            'Pixel' => {struct, [{r, ubyte}, {g, ubyte}, {b, ubyte}, {channel, 'Channel'}]},
            test => table([field(color, 'Pixel')])
        },
        #{root_type => test}
    ),
    Map = #{color => #{r => 255, g => 128, b => 0, channel => 'Green'}},
    Buffer = iolist_to_binary(flatbuferl_builder:from_map(Map, Schema)),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Struct} = flatbuferl_reader:get_field(Root, 0, field_type(Schema, test, color), Buffer),
    ?assertEqual(255, maps:get(r, Struct)),
    ?assertEqual(128, maps:get(g, Struct)),
    ?assertEqual(0, maps:get(b, Struct)),
    ?assertEqual('Green', maps:get(channel, Struct)).
