-module(builder_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Simple Scalar Tests
%% =============================================================================

simple_int_test() ->
    Defs = #{test => {table, [{a, int, #{id => 0}}]}},
    Map = #{a => 42},
    Buffer = builder:from_map(Map, Defs, test, <<"TEST">>),
    ?assertEqual(<<"TEST">>, reader:get_file_id(Buffer)),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, 42}, reader:get_field(Root, 0, int, Buffer)).

two_ints_test() ->
    Defs = #{test => {table, [{a, int, #{id => 0}}, {b, int, #{id => 1}}]}},
    Map = #{a => 10, b => 20},
    Buffer = builder:from_map(Map, Defs, test, <<"TEST">>),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, 10}, reader:get_field(Root, 0, int, Buffer)),
    ?assertEqual({ok, 20}, reader:get_field(Root, 1, int, Buffer)).

skip_default_value_test() ->
    Defs = #{test => {table, [{a, {int, 100}, #{id => 0}}, {b, int, #{id => 1}}]}},
    Map = #{a => 100, b => 20},  %% a has default value, should be skipped
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual(missing, reader:get_field(Root, 0, int, Buffer)),  %% a not written
    ?assertEqual({ok, 20}, reader:get_field(Root, 1, int, Buffer)).

non_contiguous_field_ids_test() ->
    %% Field IDs 0 and 2 (gap at 1)
    Defs = #{test => {table, [{a, int, #{id => 0}}, {c, int, #{id => 2}}]}},
    Map = #{a => 10, c => 30},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, 10}, reader:get_field(Root, 0, int, Buffer)),
    ?assertEqual(missing, reader:get_field(Root, 1, int, Buffer)),  %% No field 1
    ?assertEqual({ok, 30}, reader:get_field(Root, 2, int, Buffer)).

%% =============================================================================
%% Different Scalar Types
%% =============================================================================

bool_test() ->
    Defs = #{test => {table, [{flag, bool, #{id => 0}}]}},
    Map = #{flag => true},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, true}, reader:get_field(Root, 0, bool, Buffer)).

byte_test() ->
    Defs = #{test => {table, [{val, byte, #{id => 0}}]}},
    Map = #{val => -42},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, -42}, reader:get_field(Root, 0, byte, Buffer)).

short_test() ->
    Defs = #{test => {table, [{val, short, #{id => 0}}]}},
    Map = #{val => -1000},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, -1000}, reader:get_field(Root, 0, short, Buffer)).

long_test() ->
    Defs = #{test => {table, [{val, long, #{id => 0}}]}},
    Map = #{val => 9000000000000},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, 9000000000000}, reader:get_field(Root, 0, long, Buffer)).

float_test() ->
    Defs = #{test => {table, [{val, float, #{id => 0}}]}},
    Map = #{val => 3.14},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    {ok, V} = reader:get_field(Root, 0, float, Buffer),
    ?assert(abs(V - 3.14) < 0.001).

double_test() ->
    Defs = #{test => {table, [{val, double, #{id => 0}}]}},
    Map = #{val => 2.718281828},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    {ok, V} = reader:get_field(Root, 0, double, Buffer),
    ?assert(abs(V - 2.718281828) < 0.0000001).

%% =============================================================================
%% String Tests
%% =============================================================================

simple_string_test() ->
    Defs = #{test => {table, [{name, string, #{id => 0}}]}},
    Map = #{name => <<"hello">>},
    Buffer = builder:from_map(Map, Defs, test, <<"TEST">>),
    ?assertEqual(<<"TEST">>, reader:get_file_id(Buffer)),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, <<"hello">>}, reader:get_field(Root, 0, string, Buffer)).

string_and_int_test() ->
    Defs = #{test => {table, [{name, string, #{id => 0}}, {val, int, #{id => 1}}]}},
    Map = #{name => <<"world">>, val => 42},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, <<"world">>}, reader:get_field(Root, 0, string, Buffer)),
    ?assertEqual({ok, 42}, reader:get_field(Root, 1, int, Buffer)).

%% =============================================================================
%% Vector Tests
%% =============================================================================

int_vector_test() ->
    Defs = #{test => {table, [{nums, {vector, int}, #{id => 0}}]}},
    Map = #{nums => [1, 2, 3]},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, [1, 2, 3]}, reader:get_field(Root, 0, {vector, int}, Buffer)).

string_vector_test() ->
    Defs = #{test => {table, [{items, {vector, string}, #{id => 0}}]}},
    Map = #{items => [<<"a">>, <<"bb">>, <<"ccc">>]},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, [<<"a">>, <<"bb">>, <<"ccc">>]}, reader:get_field(Root, 0, {vector, string}, Buffer)).

mixed_with_vector_test() ->
    Defs = #{test => {table, [{name, string, #{id => 0}}, {scores, {vector, int}, #{id => 1}}]}},
    Map = #{name => <<"test">>, scores => [10, 20, 30]},
    Buffer = builder:from_map(Map, Defs, test),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, <<"test">>}, reader:get_field(Root, 0, string, Buffer)),
    ?assertEqual({ok, [10, 20, 30]}, reader:get_field(Root, 1, {vector, int}, Buffer)).

%% =============================================================================
%% Nested Table Tests
%% =============================================================================

nested_table_test() ->
    Defs = #{
        'Outer' => {table, [{name, string, #{id => 0}}, {inner, 'Inner', #{id => 1}}]},
        'Inner' => {table, [{value, int, #{id => 0}}]}
    },
    Map = #{name => <<"outer">>, inner => #{value => 42}},
    Buffer = builder:from_map(Map, Defs, 'Outer'),
    Root = reader:get_root(Buffer),
    ?assertEqual({ok, <<"outer">>}, reader:get_field(Root, 0, string, Buffer)),
    {ok, InnerRef} = reader:get_field(Root, 1, 'Inner', Buffer),
    ?assertEqual({ok, 42}, reader:get_field(InnerRef, 0, int, Buffer)).

nested_with_string_test() ->
    Defs = #{
        'Parent' => {table, [{child, 'Child', #{id => 0}}]},
        'Child' => {table, [{name, string, #{id => 0}}, {age, int, #{id => 1}}]}
    },
    Map = #{child => #{name => <<"Alice">>, age => 30}},
    Buffer = builder:from_map(Map, Defs, 'Parent'),
    Root = reader:get_root(Buffer),
    {ok, ChildRef} = reader:get_field(Root, 0, 'Child', Buffer),
    ?assertEqual({ok, <<"Alice">>}, reader:get_field(ChildRef, 0, string, Buffer)),
    ?assertEqual({ok, 30}, reader:get_field(ChildRef, 1, int, Buffer)).

%% =============================================================================
%% JSON Roundtrip Tests
%% =============================================================================

json_roundtrip_simple_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_monster.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Monster'),
    Original = eflatbuffers:to_map(Ctx),

    %% Encode to JSON (atom keys) -> decode (binary keys) -> from_map -> to_map
    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = eflatbuffers:from_map(Decoded, Defs, 'Monster', <<"MONS">>),
    NewCtx = eflatbuffers:new(NewBuffer, Defs, 'Monster'),
    Result = eflatbuffers:to_map(NewCtx),

    ?assertEqual(maps:get(name, Original), maps:get(name, Result)),
    ?assertEqual(maps:get(hp, Original), maps:get(hp, Result)),
    ?assertEqual(maps:get(mana, Original), maps:get(mana, Result)).

json_roundtrip_nested_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_nested.bin"),
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_nested.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Entity'),
    Original = eflatbuffers:to_map(Ctx),

    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = eflatbuffers:from_map(Decoded, Defs, 'Entity', <<"NEST">>),
    NewCtx = eflatbuffers:new(NewBuffer, Defs, 'Entity'),
    Result = eflatbuffers:to_map(NewCtx),

    ?assertEqual(maps:get(name, Original), maps:get(name, Result)),
    ?assertEqual(maps:get(hp, Original), maps:get(hp, Result)),
    OrigPos = maps:get(pos, Original),
    ResultPos = maps:get(pos, Result),
    ?assertEqual(maps:get(x, OrigPos), maps:get(x, ResultPos)),
    ?assertEqual(maps:get(y, OrigPos), maps:get(y, ResultPos)),
    ?assertEqual(maps:get(z, OrigPos), maps:get(z, ResultPos)).

json_roundtrip_vectors_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_vector.bin"),
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_vector.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Inventory'),
    Original = eflatbuffers:to_map(Ctx),

    Json = iolist_to_binary(json:encode(Original)),
    Decoded = json:decode(Json),
    NewBuffer = eflatbuffers:from_map(Decoded, Defs, 'Inventory', <<"VECT">>),
    NewCtx = eflatbuffers:new(NewBuffer, Defs, 'Inventory'),
    Result = eflatbuffers:to_map(NewCtx),

    ?assertEqual(maps:get(counts, Original), maps:get(counts, Result)),
    ?assertEqual(maps:get(items, Original), maps:get(items, Result)).

%% =============================================================================
%% Flatc Roundtrip Tests (validates output against official implementation)
%% =============================================================================

flatc_roundtrip_monster_test() ->
    %% Read original, modify, write new buffer, verify flatc can decode it
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_monster.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Monster'),
    Map = eflatbuffers:to_map(Ctx),

    %% Modify the map
    Modified = Map#{name => <<"Troll">>, hp => 200, mana => 75},

    %% Build new buffer
    NewBuffer = eflatbuffers:from_map(Modified, Defs, 'Monster', <<"MONS">>),
    TmpBin = "/tmp/eflatbuffers_test_monster.bin",
    TmpJson = "/tmp/eflatbuffers_test_monster.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode our buffer (--strict-json for proper JSON)
    Cmd = io_lib:format("flatc --json --strict-json -o /tmp test/vectors/test_monster.fbs -- ~s 2>&1", [TmpBin]),
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
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_nested.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Entity'),
    Map = eflatbuffers:to_map(Ctx),

    %% Modify nested values
    Modified = Map#{name => <<"Enemy">>, hp => 50,
                    pos => #{x => 10.0, y => 20.0, z => 30.0}},

    %% Build new buffer
    NewBuffer = eflatbuffers:from_map(Modified, Defs, 'Entity', <<"NEST">>),
    TmpBin = "/tmp/eflatbuffers_test_nested.bin",
    TmpJson = "/tmp/eflatbuffers_test_nested.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode
    Cmd = io_lib:format("flatc --json --strict-json -o /tmp test/vectors/test_nested.fbs -- ~s 2>&1", [TmpBin]),
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
    {ok, {Defs, _}} = schema:parse_file("test/vectors/test_vector.fbs"),
    Ctx = eflatbuffers:new(Buffer, Defs, 'Inventory'),
    Map = eflatbuffers:to_map(Ctx),

    %% Modify vectors
    Modified = Map#{counts => [10, 20, 30],
                    items => [<<"axe">>, <<"bow">>]},

    %% Build new buffer
    NewBuffer = eflatbuffers:from_map(Modified, Defs, 'Inventory', <<"VECT">>),
    TmpBin = "/tmp/eflatbuffers_test_vector.bin",
    TmpJson = "/tmp/eflatbuffers_test_vector.json",
    ok = file:write_file(TmpBin, NewBuffer),

    %% Use flatc to decode
    Cmd = io_lib:format("flatc --json --strict-json -o /tmp test/vectors/test_vector.fbs -- ~s 2>&1", [TmpBin]),
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
