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
