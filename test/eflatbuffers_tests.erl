-module(eflatbuffers_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test Fixtures
%% =============================================================================

monster_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    eflatbuffers:new(Buffer, Schema).

defaults_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_defaults.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    eflatbuffers:new(Buffer, Schema).

nested_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_nested.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_nested.fbs"),
    eflatbuffers:new(Buffer, Schema).

vector_ctx() ->
    {ok, Buffer} = file:read_file("test/vectors/test_vector.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_vector.fbs"),
    eflatbuffers:new(Buffer, Schema).

%% =============================================================================
%% Context Creation Tests
%% =============================================================================

new_with_root_type_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    Ctx = eflatbuffers:new(Buffer, Schema),
    ?assertEqual(<<"MONS">>, eflatbuffers:file_id(Ctx)).

new_auto_root_type_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    %% Should auto-detect root type
    Ctx = eflatbuffers:new(Buffer, Schema),
    ?assertEqual(<<"MONS">>, eflatbuffers:file_id(Ctx)).

%% =============================================================================
%% Basic Get Tests
%% =============================================================================

get_string_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(<<"Orc">>, eflatbuffers:get(Ctx, [name])).

get_int_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(150, eflatbuffers:get(Ctx, [hp])).

get_another_int_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(30, eflatbuffers:get(Ctx, [mana])).

%% =============================================================================
%% Schema Default Tests
%% =============================================================================

get_with_schema_default_test() ->
    Ctx = defaults_ctx(),
    %% hp is missing in binary, should return schema default 100
    ?assertEqual(100, eflatbuffers:get(Ctx, [hp])).

get_present_field_ignores_default_test() ->
    Ctx = monster_ctx(),
    %% hp is present (150), schema default (100) should be ignored
    ?assertEqual(150, eflatbuffers:get(Ctx, [hp])).

%% =============================================================================
%% User Default Tests (get/3)
%% =============================================================================

get_with_user_default_present_test() ->
    Ctx = monster_ctx(),
    %% Field present, user default ignored
    ?assertEqual(<<"Orc">>, eflatbuffers:get(Ctx, [name], <<"fallback">>)).

get_with_user_default_schema_default_test() ->
    Ctx = defaults_ctx(),
    %% Field missing, schema default takes precedence over user default
    ?assertEqual(100, eflatbuffers:get(Ctx, [hp], 999)).

%% =============================================================================
%% Crash Behavior Tests
%% =============================================================================

get_unknown_field_crashes_test() ->
    Ctx = monster_ctx(),
    ?assertError({unknown_field, nonexistent}, eflatbuffers:get(Ctx, [nonexistent])).

%% =============================================================================
%% Has Tests
%% =============================================================================

has_present_field_test() ->
    Ctx = monster_ctx(),
    ?assert(eflatbuffers:has(Ctx, [name])).

has_missing_with_default_test() ->
    Ctx = defaults_ctx(),
    %% hp missing but has schema default - returns true (can get value)
    ?assert(eflatbuffers:has(Ctx, [hp])).

%% =============================================================================
%% File ID Tests
%% =============================================================================

file_id_from_ctx_test() ->
    Ctx = monster_ctx(),
    ?assertEqual(<<"MONS">>, eflatbuffers:file_id(Ctx)).

file_id_from_buffer_test() ->
    {ok, Buffer} = file:read_file("test/vectors/test_monster.bin"),
    ?assertEqual(<<"MONS">>, eflatbuffers:file_id(Buffer)).

%% =============================================================================
%% Vector Tests
%% =============================================================================

get_int_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual([1, 1, 5], eflatbuffers:get(Ctx, [counts])).

get_string_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual([<<"sword">>, <<"shield">>, <<"potion">>], eflatbuffers:get(Ctx, [items])).

%% =============================================================================
%% Nested Path Tests
%% =============================================================================

get_nested_float_test() ->
    Ctx = nested_ctx(),
    X = eflatbuffers:get(Ctx, [pos, x]),
    ?assert(abs(X - 1.5) < 0.001).

get_nested_all_coords_test() ->
    Ctx = nested_ctx(),
    X = eflatbuffers:get(Ctx, [pos, x]),
    Y = eflatbuffers:get(Ctx, [pos, y]),
    Z = eflatbuffers:get(Ctx, [pos, z]),
    ?assert(abs(X - 1.5) < 0.001),
    ?assert(abs(Y - 2.5) < 0.001),
    ?assert(abs(Z - 3.5) < 0.001).

get_top_level_with_nested_test() ->
    Ctx = nested_ctx(),
    ?assertEqual(<<"Player">>, eflatbuffers:get(Ctx, [name])),
    ?assertEqual(200, eflatbuffers:get(Ctx, [hp])).

%% =============================================================================
%% Raw Bytes Access Tests (get_bytes)
%% =============================================================================

get_bytes_string_test() ->
    Ctx = monster_ctx(),
    Bytes = eflatbuffers:get_bytes(Ctx, [name]),
    %% get_bytes returns slice from data position to end of buffer
    %% The string "Orc" starts with length prefix (3), then "Orc", then null terminator
    <<Len:32/little, Rest/binary>> = Bytes,
    ?assertEqual(3, Len),
    <<Name:3/binary, _/binary>> = Rest,
    ?assertEqual(<<"Orc">>, Name).

get_bytes_missing_field_crashes_test() ->
    Ctx = defaults_ctx(),
    %% hp is missing in defaults - should crash
    ?assertError({missing_field, [hp]}, eflatbuffers:get_bytes(Ctx, [hp])).

get_bytes_unknown_field_crashes_test() ->
    Ctx = monster_ctx(),
    ?assertError({unknown_field, nonexistent}, eflatbuffers:get_bytes(Ctx, [nonexistent])).

get_bytes_nested_string_test() ->
    Ctx = nested_ctx(),
    %% Get raw bytes for nested name field (string uses offset)
    Bytes = eflatbuffers:get_bytes(Ctx, [name]),
    <<Len:32/little, Rest/binary>> = Bytes,
    ?assertEqual(6, Len),
    <<Name:6/binary, _/binary>> = Rest,
    ?assertEqual(<<"Player">>, Name).

%% =============================================================================
%% Full Deserialization Tests (to_map)
%% =============================================================================

to_map_basic_test() ->
    Ctx = monster_ctx(),
    Map = eflatbuffers:to_map(Ctx),
    ?assertEqual(<<"Orc">>, maps:get(name, Map)),
    ?assertEqual(150, maps:get(hp, Map)),
    ?assertEqual(30, maps:get(mana, Map)).

to_map_with_defaults_test() ->
    Ctx = defaults_ctx(),
    Map = eflatbuffers:to_map(Ctx),
    ?assertEqual(<<"Goblin">>, maps:get(name, Map)),
    %% hp missing, should use schema default
    ?assertEqual(100, maps:get(hp, Map)),
    ?assertEqual(20, maps:get(mana, Map)).

to_map_nested_test() ->
    Ctx = nested_ctx(),
    Map = eflatbuffers:to_map(Ctx),
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
    Map = eflatbuffers:to_map(Ctx),
    ?assertEqual([1, 1, 5], maps:get(counts, Map)),
    ?assertEqual([<<"sword">>, <<"shield">>, <<"potion">>], maps:get(items, Map)).

%% =============================================================================
%% Builder Tests (from_map)
%% =============================================================================

from_map_basic_test() ->
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Goblin">>, hp => 50, mana => 25},
    Buffer = iolist_to_binary(eflatbuffers:from_map(Map, Schema)),
    ?assert(is_binary(Buffer)),
    %% Verify we can read it back
    Ctx = eflatbuffers:new(Buffer, Schema),
    ?assertEqual(<<"Goblin">>, eflatbuffers:get(Ctx, [name])),
    ?assertEqual(50, eflatbuffers:get(Ctx, [hp])),
    ?assertEqual(25, eflatbuffers:get(Ctx, [mana])).

from_map_roundtrip_test() ->
    %% Read existing buffer, convert to map, build new buffer, verify
    OrigCtx = monster_ctx(),
    Map = eflatbuffers:to_map(OrigCtx),
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    NewBuffer = iolist_to_binary(eflatbuffers:from_map(Map, Schema)),
    NewCtx = eflatbuffers:new(NewBuffer, Schema),
    ?assertEqual(<<"Orc">>, eflatbuffers:get(NewCtx, [name])),
    ?assertEqual(150, eflatbuffers:get(NewCtx, [hp])),
    ?assertEqual(30, eflatbuffers:get(NewCtx, [mana])).

from_map_file_id_test() ->
    {ok, Schema} = schema:parse_file("test/vectors/test_monster.fbs"),
    Map = #{name => <<"Test">>},
    Buffer = iolist_to_binary(eflatbuffers:from_map(Map, Schema, #{file_id => <<"TEST">>})),
    ?assertEqual(<<"TEST">>, eflatbuffers:file_id(Buffer)).
