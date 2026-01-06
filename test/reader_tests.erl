-module(reader_tests).
-include_lib("eunit/include/eunit.hrl").

%% Test fixtures
monster_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_monster.bin"),
    Bin.

defaults_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_defaults.bin"),
    Bin.

nested_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_nested.bin"),
    Bin.

vector_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_vector.bin"),
    Bin.

%% =============================================================================
%% Basic Tests
%% =============================================================================

file_identifier_test() ->
    Buffer = monster_binary(),
    ?assertEqual(<<"MONS">>, flatbuferl_reader:get_file_id(Buffer)).

root_table_test() ->
    Buffer = monster_binary(),
    {table, Offset, ReturnedBuffer} = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(20, Offset),
    ?assertEqual(Buffer, ReturnedBuffer).

%% =============================================================================
%% Monster Tests (basic scalars + string)
%% =============================================================================

monster_name_test() ->
    Buffer = monster_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, <<"Orc">>}, flatbuferl_reader:get_field(Root, 0, string, Buffer)).

monster_hp_test() ->
    Buffer = monster_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 150}, flatbuferl_reader:get_field(Root, 1, int, Buffer)).

monster_mana_test() ->
    Buffer = monster_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 30}, flatbuferl_reader:get_field(Root, 2, int, Buffer)).

%% =============================================================================
%% Defaults Tests (missing field should return 'missing')
%% =============================================================================

defaults_file_id_test() ->
    Buffer = defaults_binary(),
    ?assertEqual(<<"MONS">>, flatbuferl_reader:get_file_id(Buffer)).

defaults_name_test() ->
    Buffer = defaults_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, <<"Goblin">>}, flatbuferl_reader:get_field(Root, 0, string, Buffer)).

defaults_hp_missing_test() ->
    Buffer = defaults_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% hp is missing, should return 'missing' (caller applies default)
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 1, int, Buffer)).

defaults_mana_test() ->
    Buffer = defaults_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 20}, flatbuferl_reader:get_field(Root, 2, int, Buffer)).

%% =============================================================================
%% Nested Table Tests
%% =============================================================================

nested_file_id_test() ->
    Buffer = nested_binary(),
    ?assertEqual(<<"NEST">>, flatbuferl_reader:get_file_id(Buffer)).

nested_name_test() ->
    Buffer = nested_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, <<"Player">>}, flatbuferl_reader:get_field(Root, 0, string, Buffer)).

nested_hp_test() ->
    Buffer = nested_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 200}, flatbuferl_reader:get_field(Root, 2, int, Buffer)).

nested_pos_is_table_test() ->
    Buffer = nested_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, {table, _Offset, _}} = flatbuferl_reader:get_field(Root, 1, 'Vec3', Buffer).

nested_pos_fields_test() ->
    Buffer = nested_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, PosTable} = flatbuferl_reader:get_field(Root, 1, 'Vec3', Buffer),
    %% Vec3 has x, y, z as fields 0, 1, 2
    {ok, X} = flatbuferl_reader:get_field(PosTable, 0, float, Buffer),
    {ok, Y} = flatbuferl_reader:get_field(PosTable, 1, float, Buffer),
    {ok, Z} = flatbuferl_reader:get_field(PosTable, 2, float, Buffer),
    ?assert(abs(X - 1.5) < 0.001),
    ?assert(abs(Y - 2.5) < 0.001),
    ?assert(abs(Z - 3.5) < 0.001).

%% =============================================================================
%% Vector Tests
%% =============================================================================

vector_file_id_test() ->
    Buffer = vector_binary(),
    ?assertEqual(<<"VECT">>, flatbuferl_reader:get_file_id(Buffer)).

vector_counts_test() ->
    Buffer = vector_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, [1, 1, 5]}, flatbuferl_reader:get_field(Root, 1, {vector, int}, Buffer)).

vector_items_test() ->
    Buffer = vector_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, [<<"sword">>, <<"shield">>, <<"potion">>]},
        flatbuferl_reader:get_field(Root, 0, {vector, string}, Buffer)
    ).

%% =============================================================================
%% All Types Tests (comprehensive scalar coverage)
%% =============================================================================

alltypes_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_alltypes.bin"),
    Bin.

alltypes_file_id_test() ->
    Buffer = alltypes_binary(),
    ?assertEqual(<<"TYPE">>, flatbuferl_reader:get_file_id(Buffer)).

alltypes_bool_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, true}, flatbuferl_reader:get_field(Root, 0, bool, Buffer)).

alltypes_byte_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -42}, flatbuferl_reader:get_field(Root, 1, byte, Buffer)).

alltypes_ubyte_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 200}, flatbuferl_reader:get_field(Root, 2, ubyte, Buffer)).

alltypes_short_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -1000}, flatbuferl_reader:get_field(Root, 3, short, Buffer)).

alltypes_ushort_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 50000}, flatbuferl_reader:get_field(Root, 4, ushort, Buffer)).

alltypes_int_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -100000}, flatbuferl_reader:get_field(Root, 5, int, Buffer)).

alltypes_uint_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 3000000000}, flatbuferl_reader:get_field(Root, 6, uint, Buffer)).

alltypes_long_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -9000000000000}, flatbuferl_reader:get_field(Root, 7, long, Buffer)).

alltypes_ulong_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 18000000000000000000}, flatbuferl_reader:get_field(Root, 8, ulong, Buffer)).

alltypes_float_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Value} = flatbuferl_reader:get_field(Root, 9, float, Buffer),
    ?assert(abs(Value - 3.14159) < 0.0001).

alltypes_double_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Value} = flatbuferl_reader:get_field(Root, 10, double, Buffer),
    ?assert(abs(Value - 2.718281828459045) < 0.0000001).

alltypes_string_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, <<"hello">>}, flatbuferl_reader:get_field(Root, 11, string, Buffer)).

alltypes_enum_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% Color.Green = 1
    ?assertEqual({ok, 1}, flatbuferl_reader:get_field(Root, 12, {enum, byte}, Buffer)).

%% =============================================================================
%% Type Alias Tests
%% =============================================================================

type_alias_int8_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -42}, flatbuferl_reader:get_field(Root, 1, int8, Buffer)).

type_alias_uint8_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 200}, flatbuferl_reader:get_field(Root, 2, uint8, Buffer)).

type_alias_int16_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -1000}, flatbuferl_reader:get_field(Root, 3, int16, Buffer)).

type_alias_uint16_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 50000}, flatbuferl_reader:get_field(Root, 4, uint16, Buffer)).

type_alias_int32_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -100000}, flatbuferl_reader:get_field(Root, 5, int32, Buffer)).

type_alias_uint32_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 3000000000}, flatbuferl_reader:get_field(Root, 6, uint32, Buffer)).

type_alias_int64_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, -9000000000000}, flatbuferl_reader:get_field(Root, 7, int64, Buffer)).

type_alias_uint64_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 18000000000000000000}, flatbuferl_reader:get_field(Root, 8, uint64, Buffer)).

type_alias_float32_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Value} = flatbuferl_reader:get_field(Root, 9, float32, Buffer),
    ?assert(abs(Value - 3.14159) < 0.0001).

type_alias_float64_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Value} = flatbuferl_reader:get_field(Root, 10, float64, Buffer),
    ?assert(abs(Value - 2.718281828459045) < 0.0000001).

%% =============================================================================
%% High-level API Tests (get/3 with schema)
%% =============================================================================

get_with_schema_test() ->
    Buffer = monster_binary(),
    {ok, {Defs, _}} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    {table, Fields} = maps:get('Monster', Defs),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, <<"Orc">>}, flatbuferl_reader:get(Root, {table, Fields}, [name])).

get_with_schema_int_test() ->
    Buffer = monster_binary(),
    {ok, {Defs, _}} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    {table, Fields} = maps:get('Monster', Defs),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, 150}, flatbuferl_reader:get(Root, {table, Fields}, [hp])).

get_unknown_field_test() ->
    Buffer = monster_binary(),
    {ok, {Defs, _}} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    {table, Fields} = maps:get('Monster', Defs),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {error, {unknown_field, nonexistent}}, flatbuferl_reader:get(Root, {table, Fields}, [nonexistent])
    ).

%% =============================================================================
%% Error Path Tests
%% =============================================================================

unsupported_type_test() ->
    Buffer = monster_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {error, {unsupported_type, {weird_type, foo}}},
        flatbuferl_reader:get_field(Root, 0, {weird_type, foo}, Buffer)
    ).

%% =============================================================================
%% Vector Type Alias Tests
%% =============================================================================

vector_bool_test() ->
    %% Use defaults binary which has bools
    Buffer = defaults_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% Field doesn't exist, just test vector code path
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 99, {vector, bool}, Buffer)).

%% =============================================================================
%% Field Beyond VTable Test
%% =============================================================================

field_beyond_vtable_test() ->
    Buffer = monster_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% Field ID 100 is way beyond the vtable
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 100, int, Buffer)).

%% =============================================================================
%% Schema lookup with full Defs map
%% =============================================================================

get_with_full_schema_map_test() ->
    Buffer = monster_binary(),
    {ok, {Defs, _}} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Root = flatbuferl_reader:get_root(Buffer),
    %% Pass full Defs map instead of {table, Fields}
    ?assertEqual({ok, <<"Orc">>}, flatbuferl_reader:get(Root, Defs, [name])).

%% =============================================================================
%% Nested path traversal test
%% =============================================================================

nested_path_traversal_test() ->
    Buffer = nested_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% This tests the multi-element path case in get/3
    %% But get_nested_schema returns error, so we expect an error
    ?assertEqual(
        {error, {unknown_nested_type, pos}},
        flatbuferl_reader:get(Root, {table, [{pos, 'Vec3', #{id => 1}}]}, [pos, x])
    ).

%% =============================================================================
%% Additional vector type tests
%% =============================================================================

vector_byte_elements_test() ->
    %% Create inline test for byte vector
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    %% Test that byte/ubyte vector reading works (even if field is missing)
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 99, {vector, byte}, Buffer)).

vector_short_elements_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 99, {vector, short}, Buffer)).

vector_long_elements_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 99, {vector, long}, Buffer)).

vector_double_elements_test() ->
    Buffer = alltypes_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(missing, flatbuferl_reader:get_field(Root, 99, {vector, double}, Buffer)).

%% =============================================================================
%% Missing field returns missing test
%% =============================================================================

get_missing_field_via_schema_test() ->
    Buffer = defaults_binary(),
    {ok, {Defs, _}} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    {table, Fields} = maps:get('Monster', Defs),
    Root = flatbuferl_reader:get_root(Buffer),
    %% hp is missing in defaults binary
    ?assertEqual(missing, flatbuferl_reader:get(Root, {table, Fields}, [hp])).

%% =============================================================================
%% Comprehensive Vector Type Tests
%% =============================================================================

vectors2_binary() ->
    {ok, Bin} = file:read_file("test/vectors/test_vectors2.bin"),
    Bin.

vector_ubyte_actual_test() ->
    Buffer = vectors2_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, [1, 2, 255]}, flatbuferl_reader:get_field(Root, 0, {vector, ubyte}, Buffer)).

vector_short_actual_test() ->
    Buffer = vectors2_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, [-100, 0, 100]}, flatbuferl_reader:get_field(Root, 1, {vector, short}, Buffer)).

vector_long_actual_test() ->
    Buffer = vectors2_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual(
        {ok, [9000000000000, -9000000000000]}, flatbuferl_reader:get_field(Root, 2, {vector, long}, Buffer)
    ).

vector_double_actual_test() ->
    Buffer = vectors2_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    {ok, Values} = flatbuferl_reader:get_field(Root, 3, {vector, double}, Buffer),
    ?assertEqual(3, length(Values)),
    [V1, V2, V3] = Values,
    ?assert(abs(V1 - 1.1) < 0.0001),
    ?assert(abs(V2 - 2.2) < 0.0001),
    ?assert(abs(V3 - 3.3) < 0.0001).

vector_bool_actual_test() ->
    Buffer = vectors2_binary(),
    Root = flatbuferl_reader:get_root(Buffer),
    ?assertEqual({ok, [true, false, true]}, flatbuferl_reader:get_field(Root, 4, {vector, bool}, Buffer)).
