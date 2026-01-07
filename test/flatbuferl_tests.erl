-module(flatbuferl_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test Fixtures
%% =============================================================================

monster_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    flatbuferl:new(Buffer, Schema).

defaults_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_defaults.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    flatbuferl:new(Buffer, Schema).

nested_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_nested.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_nested.fbs"),
    flatbuferl:new(Buffer, Schema).

vector_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_vector.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    flatbuferl:new(Buffer, Schema).

%% =============================================================================
%% Context Creation Tests
%% =============================================================================

new_with_root_type_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(<<"MONS">>, flatbuferl:file_id(Ctx)).

new_auto_root_type_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    %% Should auto-detect root type
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(<<"MONS">>, flatbuferl:file_id(Ctx)).

%% =============================================================================
%% Basic Get Tests
%% =============================================================================

get_string_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(<<"Orc">>, flatbuferl:get(Ctx, [name])).

get_int_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(150, flatbuferl:get(Ctx, [hp])).

get_another_int_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(30, flatbuferl:get(Ctx, [mana])).

%% =============================================================================
%% Schema Default Tests
%% =============================================================================

get_with_schema_default_test() ->
    Ctx = defaults_ctx(),
    %% hp is missing in binary, should return schema default 100
    ?assertEqual(100, flatbuferl:get(Ctx, [hp])).

get_present_field_ignores_default_test() ->
    Ctx = monster_ctx(),
    %% hp is present (150), schema default (100) should be ignored
    ?assertEqual(150, flatbuferl:get(Ctx, [hp])).

%% =============================================================================
%% User Default Tests (get/3)
%% =============================================================================

get_with_user_default_present_test() ->
    Ctx = monster_ctx(),
    %% Field present, user default ignored
    ?assertEqual(<<"Orc">>, flatbuferl:get(Ctx, [name], <<"fallback">>)).

get_with_user_default_schema_default_test() ->
    Ctx = defaults_ctx(),
    %% Field missing, schema default takes precedence over user default
    ?assertEqual(100, flatbuferl:get(Ctx, [hp], 999)).

%% =============================================================================
%% Crash Behavior Tests
%% =============================================================================

get_unknown_field_crashes_test() ->
    Ctx = monster_ctx(),
    ?assertError({unknown_field, nonexistent}, flatbuferl:get(Ctx, [nonexistent])).

%% =============================================================================
%% Has Tests
%% =============================================================================

has_present_field_test() ->
    Ctx = monster_ctx(),
    ?assert(flatbuferl:has(Ctx, [name])).

has_missing_with_default_test() ->
    Ctx = defaults_ctx(),
    %% hp missing but has schema default - returns true (can get value)
    ?assert(flatbuferl:has(Ctx, [hp])).

%% =============================================================================
%% File ID Tests
%% =============================================================================

file_id_from_ctx_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(<<"MONS">>, flatbuferl:file_id(Ctx)).

file_id_from_buffer_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    ?assertEqual(<<"MONS">>, flatbuferl:file_id(Buffer)).

%% =============================================================================
%% Vector Tests
%% =============================================================================

get_int_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual([1, 1, 5], flatbuferl:get(Ctx, [counts])).

get_string_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual([<<"sword">>, <<"shield">>, <<"potion">>], flatbuferl:get(Ctx, [items])).

%% =============================================================================
%% Nested Path Tests
%% =============================================================================

get_nested_float_test() ->
    Ctx = nested_ctx(),
    X = flatbuferl:get(Ctx, [pos, x]),
    ?assert(abs(X - 1.5) < 0.001).

get_nested_all_coords_test() ->
    Ctx = nested_ctx(),
    X = flatbuferl:get(Ctx, [pos, x]),
    Y = flatbuferl:get(Ctx, [pos, y]),
    Z = flatbuferl:get(Ctx, [pos, z]),
    ?assert(abs(X - 1.5) < 0.001),
    ?assert(abs(Y - 2.5) < 0.001),
    ?assert(abs(Z - 3.5) < 0.001).

get_top_level_with_nested_test() ->
    Ctx = nested_ctx(),
    ?assertEqual(<<"Player">>, flatbuferl:get(Ctx, [name])),
    ?assertEqual(200, flatbuferl:get(Ctx, [hp])).

%% =============================================================================
%% Raw Bytes Access Tests (get_bytes)
%% =============================================================================

get_bytes_string_test() ->
    Ctx = monster_ctx(),
    Bytes = flatbuferl:get_bytes(Ctx, [name]),
    %% get_bytes returns slice from data position to end of buffer
    %% The string "Orc" starts with length prefix (3), then "Orc", then null terminator
    <<Len:32/little, Rest/binary>> = Bytes,
    ?assertEqual(3, Len),
    <<Name:3/binary, _/binary>> = Rest,
    ?assertEqual(<<"Orc">>, Name).

get_bytes_missing_field_crashes_test() ->
    Ctx = defaults_ctx(),
    %% hp is missing in defaults - should crash
    ?assertError({missing_field, [hp]}, flatbuferl:get_bytes(Ctx, [hp])).

get_bytes_unknown_field_crashes_test() ->
    Ctx = monster_ctx(),
    ?assertError({unknown_field, nonexistent}, flatbuferl:get_bytes(Ctx, [nonexistent])).

get_bytes_nested_string_test() ->
    Ctx = nested_ctx(),
    %% Get raw bytes for nested name field (string uses offset)
    Bytes = flatbuferl:get_bytes(Ctx, [name]),
    <<Len:32/little, Rest/binary>> = Bytes,
    ?assertEqual(6, Len),
    <<Name:6/binary, _/binary>> = Rest,
    ?assertEqual(<<"Player">>, Name).

%% =============================================================================
%% Full Deserialization Tests (to_map)
%% =============================================================================

to_map_basic_test() ->
    Ctx = monster_ctx(),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual(<<"Orc">>, maps:get(name, Map)),
    ?assertEqual(150, maps:get(hp, Map)),
    ?assertEqual(30, maps:get(mana, Map)).

to_map_with_defaults_test() ->
    Ctx = defaults_ctx(),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual(<<"Goblin">>, maps:get(name, Map)),
    %% hp missing, should use schema default
    ?assertEqual(100, maps:get(hp, Map)),
    ?assertEqual(20, maps:get(mana, Map)).

to_map_nested_test() ->
    Ctx = nested_ctx(),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual(<<"Player">>, maps:get(name, Map)),
    ?assertEqual(200, maps:get(hp, Map)),
    Pos = maps:get(pos, Map),
    ?assert(is_map(Pos)),
    X = maps:get(x, Pos),
    Y = maps:get(y, Pos),
    Z = maps:get(z, Pos),
    ?assert(abs(X - 1.5) < 0.001),
    ?assert(abs(Y - 2.5) < 0.001),
    ?assert(abs(Z - 3.5) < 0.001).

to_map_vectors_test() ->
    Ctx = vector_ctx(),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual([1, 1, 5], maps:get(counts, Map)),
    ?assertEqual([<<"sword">>, <<"shield">>, <<"potion">>], maps:get(items, Map)).

%% =============================================================================
%% Builder Tests (from_map)
%% =============================================================================

from_map_basic_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Goblin">>, hp => 50, mana => 25},
    Buffer = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
    ?assert(is_binary(Buffer)),
    %% Verify we can read it back
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(<<"Goblin">>, flatbuferl:get(Ctx, [name])),
    ?assertEqual(50, flatbuferl:get(Ctx, [hp])),
    ?assertEqual(25, flatbuferl:get(Ctx, [mana])).

from_map_roundtrip_test() ->
    %% Read existing buffer, convert to map, build new buffer, verify
    OrigCtx = monster_ctx(),
    Map = flatbuferl:to_map(OrigCtx),
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    NewBuffer = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    ?assertEqual(<<"Orc">>, flatbuferl:get(NewCtx, [name])),
    ?assertEqual(150, flatbuferl:get(NewCtx, [hp])),
    ?assertEqual(30, flatbuferl:get(NewCtx, [mana])).

from_map_file_id_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Map, Schema, #{file_id => <<"TEST">>})),
    ?assertEqual(<<"TEST">>, flatbuferl:file_id(Buffer)).

%% =============================================================================
%% Validation Tests
%% =============================================================================

validate_valid_map_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Orc">>, hp => 150, mana => 30},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_wrong_scalar_type_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Orc">>, hp => <<"not an int">>},
    ?assertMatch(
        {error, [{type_mismatch, hp, int, <<"not an int">>}]},
        flatbuferl:validate(Map, Schema)
    ).

validate_string_type_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => 12345},
    ?assertMatch(
        {error, [{type_mismatch, name, string, 12345}]},
        flatbuferl:validate(Map, Schema)
    ).

validate_optional_fields_ok_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    %% Only name provided, others optional
    Map = #{name => <<"Goblin">>},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_unknown_fields_ignored_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Orc">>, unknown_field => 123},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_unknown_fields_error_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Orc">>, unknown_field => 123},
    ?assertMatch(
        {error, [{unknown_field, unknown_field}]},
        flatbuferl:validate(Map, Schema, #{unknown_fields => error})
    ).

validate_vector_valid_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Map = #{counts => [1, 2, 3], items => [<<"a">>, <<"b">>]},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_vector_wrong_element_type_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Map = #{counts => [1, <<"not int">>, 3]},
    ?assertMatch(
        {error, [{invalid_vector_element, counts, 1, _}]},
        flatbuferl:validate(Map, Schema)
    ).

validate_vector_not_a_list_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Map = #{counts => 42},
    ?assertMatch(
        {error, [{type_mismatch, counts, {vector, int}, 42}]},
        flatbuferl:validate(Map, Schema)
    ).

validate_nested_table_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_nested.fbs"),
    Map = #{name => <<"Player">>, hp => 100, pos => #{x => 1.0, y => 2.0, z => 3.0}},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_nested_table_invalid_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_nested.fbs"),
    Map = #{name => <<"Player">>, pos => #{x => <<"not a float">>}},
    ?assertMatch(
        {error, [{nested_errors, pos, [{type_mismatch, x, float, _}]}]},
        flatbuferl:validate(Map, Schema)
    ).

validate_binary_keys_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    %% Binary keys should work too
    Map = #{<<"name">> => <<"Orc">>, <<"hp">> => 150},
    ?assertEqual(ok, flatbuferl:validate(Map, Schema)).

validate_int_range_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    %% hp is int (32-bit signed), should be -2147483648 to 2147483647

    %% Out of int32 range
    Map = #{hp => 3000000000},
    ?assertMatch(
        {error, [{type_mismatch, hp, _, 3000000000}]},
        flatbuferl:validate(Map, Schema)
    ).
