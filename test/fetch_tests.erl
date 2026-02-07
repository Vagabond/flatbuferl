-module(fetch_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test Fixtures
%% =============================================================================

simple_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_monster.fbs"),
    Data = #{name => <<"Goblin">>, hp => 50, mana => 25},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

nested_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_nested.fbs"),
    Data = #{name => <<"Player">>, hp => 100, pos => #{x => 1.0, y => 2.0, z => 3.0}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Data = #{items => [<<"sword">>, <<"shield">>, <<"potion">>], counts => [1, 2, 3]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

table_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/table_vector.fbs"),
    Data = #{
        inner => [
            #{value_inner => <<"one">>},
            #{value_inner => <<"two">>},
            #{value_inner => <<"three">>}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

%% =============================================================================
%% Basic Field Access
%% =============================================================================

fetch_string_test() ->
    Ctx = simple_ctx(),
    ?assertEqual(<<"Goblin">>, flatbuferl_fetch:fetch(Ctx, [name])).

fetch_int_test() ->
    Ctx = simple_ctx(),
    ?assertEqual(50, flatbuferl_fetch:fetch(Ctx, [hp])).

fetch_missing_field_test() ->
    Ctx = simple_ctx(),
    ?assertError({unknown_field, nonexistent}, flatbuferl_fetch:fetch(Ctx, [nonexistent])).

%% =============================================================================
%% Nested Table Access
%% =============================================================================

fetch_nested_field_test() ->
    Ctx = nested_ctx(),
    ?assertEqual(1.0, flatbuferl_fetch:fetch(Ctx, [pos, x])).

fetch_nested_multiple_test() ->
    Ctx = nested_ctx(),
    ?assertEqual(2.0, flatbuferl_fetch:fetch(Ctx, [pos, y])),
    ?assertEqual(3.0, flatbuferl_fetch:fetch(Ctx, [pos, z])).

%% =============================================================================
%% Vector Indexing
%% =============================================================================

fetch_vector_first_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(<<"sword">>, flatbuferl_fetch:fetch(Ctx, [items, 0])).

fetch_vector_middle_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(<<"shield">>, flatbuferl_fetch:fetch(Ctx, [items, 1])).

fetch_vector_last_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(<<"potion">>, flatbuferl_fetch:fetch(Ctx, [items, 2])).

fetch_vector_negative_index_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(<<"potion">>, flatbuferl_fetch:fetch(Ctx, [items, -1])),
    ?assertEqual(<<"shield">>, flatbuferl_fetch:fetch(Ctx, [items, -2])),
    ?assertEqual(<<"sword">>, flatbuferl_fetch:fetch(Ctx, [items, -3])).

fetch_vector_out_of_bounds_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items, 100])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items, -100])).

fetch_int_vector_index_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(1, flatbuferl_fetch:fetch(Ctx, [counts, 0])),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [counts, -1])).

%% =============================================================================
%% Vector of Tables Indexing
%% =============================================================================

fetch_table_vector_index_test() ->
    Ctx = table_vector_ctx(),
    ?assertEqual(#{value_inner => <<"one">>}, flatbuferl_fetch:fetch(Ctx, [inner, 0])).

fetch_table_vector_nested_test() ->
    Ctx = table_vector_ctx(),
    ?assertEqual(<<"one">>, flatbuferl_fetch:fetch(Ctx, [inner, 0, value_inner])),
    ?assertEqual(<<"two">>, flatbuferl_fetch:fetch(Ctx, [inner, 1, value_inner])),
    ?assertEqual(<<"three">>, flatbuferl_fetch:fetch(Ctx, [inner, -1, value_inner])).

%% =============================================================================
%% Wildcards
%% =============================================================================

fetch_wildcard_strings_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(
        [<<"sword">>, <<"shield">>, <<"potion">>], flatbuferl_fetch:fetch(Ctx, [items, '*'])
    ).

fetch_wildcard_ints_test() ->
    Ctx = vector_ctx(),
    ?assertEqual([1, 2, 3], flatbuferl_fetch:fetch(Ctx, [counts, '*'])).

fetch_wildcard_tables_test() ->
    Ctx = table_vector_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [inner, '*']),
    ?assertEqual(
        [
            #{value_inner => <<"one">>},
            #{value_inner => <<"two">>},
            #{value_inner => <<"three">>}
        ],
        Result
    ).

fetch_wildcard_nested_field_test() ->
    Ctx = table_vector_ctx(),
    ?assertEqual(
        [<<"one">>, <<"two">>, <<"three">>], flatbuferl_fetch:fetch(Ctx, [inner, '*', value_inner])
    ).

fetch_wildcard_empty_vector_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Data = #{items => [], counts => []},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [items, '*'])).

%% =============================================================================
%% Multi-Field Extraction
%% =============================================================================

fetch_multi_field_test() ->
    Ctx = simple_ctx(),
    ?assertEqual([<<"Goblin">>, 50], flatbuferl_fetch:fetch(Ctx, [[name, hp]])).

fetch_multi_field_nested_test() ->
    Ctx = nested_ctx(),
    ?assertEqual([1.0, 2.0, 3.0], flatbuferl_fetch:fetch(Ctx, [pos, [x, y, z]])).

fetch_multi_field_with_wildcard_test() ->
    Ctx = table_vector_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [inner, '*', [value_inner]]),
    ?assertEqual([[<<"one">>], [<<"two">>], [<<"three">>]], Result).

fetch_multi_field_star_test() ->
    Ctx = simple_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [['*']]),
    ?assert(is_list(Result)),
    ?assertEqual(1, length(Result)),
    Map = hd(Result),
    ?assertEqual(<<"Goblin">>, maps:get(name, Map)).

fetch_multi_field_with_missing_data_test() ->
    %% Test extraction where a field exists in schema but is missing in buffer
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_monster.fbs"),
    %% Only include 'name', hp/mana will use defaults
    Data = #{name => <<"Sparse">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% hp has schema default 100, mana has schema default 50
    ?assertEqual([<<"Sparse">>, 100, 50], flatbuferl_fetch:fetch(Ctx, [[name, hp, mana]])).

%% =============================================================================
%% Struct Access
%% =============================================================================

fetch_struct_wildcard_test() ->
    Ctx = nested_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [pos, '*']),
    ?assert(is_map(Result)),
    ?assertEqual(1.0, maps:get(x, Result)),
    ?assertEqual(2.0, maps:get(y, Result)),
    ?assertEqual(3.0, maps:get(z, Result)).

%% =============================================================================
%% Fixed-Size Array Access
%% =============================================================================

array_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [10, 20, 30, 40]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_array_index_test() ->
    Ctx = array_ctx(),
    ?assertEqual(1.0, flatbuferl_fetch:fetch(Ctx, [floats, 0])),
    ?assertEqual(3.0, flatbuferl_fetch:fetch(Ctx, [floats, 2])).

fetch_array_negative_index_test() ->
    Ctx = array_ctx(),
    ?assertEqual(3.0, flatbuferl_fetch:fetch(Ctx, [floats, -1])),
    ?assertEqual(1.0, flatbuferl_fetch:fetch(Ctx, [floats, -3])).

fetch_array_wildcard_test() ->
    Ctx = array_ctx(),
    ?assertEqual([1.0, 2.0, 3.0], flatbuferl_fetch:fetch(Ctx, [floats, '*'])),
    ?assertEqual([10, 20, 30, 40], flatbuferl_fetch:fetch(Ctx, [ints, '*'])).

fetch_array_size_test() ->
    Ctx = array_ctx(),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [floats, '_size'])),
    ?assertEqual(4, flatbuferl_fetch:fetch(Ctx, [ints, '_size'])).

%% =============================================================================
%% _size Pseudo-field
%% =============================================================================

fetch_size_of_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [items, '_size'])),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [counts, '_size'])).

fetch_size_of_empty_vector_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Data = #{items => [], counts => []},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [items, '_size'])).

fetch_size_of_missing_vector_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    Data = #{items => [<<"a">>]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [counts, '_size'])).

fetch_size_of_table_vector_test() ->
    Ctx = table_vector_ctx(),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [inner, '_size'])).

fetch_size_of_string_test() ->
    Ctx = simple_ctx(),
    %% <<"Goblin">> = 6 bytes
    ?assertEqual(6, flatbuferl_fetch:fetch(Ctx, [name, '_size'])).

fetch_size_of_missing_string_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_monster.fbs"),
    Data = #{hp => 50},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [name, '_size'])).

%% =============================================================================
%% _type Pseudo-field (Unions)
%% =============================================================================

union_ctx(Type, Value) ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_field.fbs"),
    Data = #{data => Value, data_type => Type, additions_value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_union_type_hello_test() ->
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    ?assertEqual(hello, flatbuferl_fetch:fetch(Ctx, [data, '_type'])).

fetch_union_type_bye_test() ->
    Ctx = union_ctx(bye, #{greeting => 99}),
    ?assertEqual(bye, flatbuferl_fetch:fetch(Ctx, [data, '_type'])).

fetch_union_field_test() ->
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    ?assertEqual(<<"Hi">>, flatbuferl_fetch:fetch(Ctx, [data, salute])).

fetch_union_field_other_member_test() ->
    %% Accessing 'greeting' when the union is 'hello' (which has 'salute')
    %% Since 'greeting' exists on 'bye' (another union member), this returns
    %% undefined rather than erroring - it's valid schema, just missing data.
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, greeting])).

fetch_union_extract_field_test() ->
    Ctx = union_ctx(bye, #{greeting => 99}),
    ?assertEqual([99], flatbuferl_fetch:fetch(Ctx, [data, [greeting]])).

fetch_union_extract_type_and_field_test() ->
    %% Extract both '_type' and a field from a union
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    ?assertEqual([hello, <<"Hi">>], flatbuferl_fetch:fetch(Ctx, [data, ['_type', salute]])).

%% =============================================================================
%% Guards (Filters)
%% =============================================================================

fetch_guard_filter_test() ->
    Ctx = table_vector_ctx(),
    %% Filter to only elements where value_inner == <<"two">>
    ?assertEqual(
        [[<<"two">>]],
        flatbuferl_fetch:fetch(Ctx, [inner, '*', [{value_inner, <<"two">>}, value_inner]])
    ).

fetch_guard_filter_all_test() ->
    Ctx = table_vector_ctx(),
    %% Filter that matches nothing
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [inner, '*', [{value_inner, <<"nonexistent">>}, value_inner]])
    ).

fetch_guard_filter_multiple_test() ->
    Ctx = table_vector_ctx(),
    %% Filter to elements where value_inner is "one" or "three" - but guards are AND
    %% So this won't match anything since value_inner can't be both
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [
            inner, '*', [{value_inner, <<"one">>}, {value_inner, <<"three">>}, value_inner]
        ])
    ).

fetch_guard_with_union_type_test() ->
    %% Filter by union type
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    %% This is a single union, not a vector, so guards apply to extraction
    ?assertEqual(
        [<<"Hi">>],
        flatbuferl_fetch:fetch(Ctx, [data, [{'_type', hello}, salute]])
    ).

fetch_guard_with_union_type_mismatch_test() ->
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    %% Guard for bye type should filter out, returning undefined
    ?assertEqual(
        undefined,
        flatbuferl_fetch:fetch(Ctx, [data, [{'_type', bye}, salute]])
    ).

%% =============================================================================
%% Comparison Guards
%% =============================================================================

fetch_comparison_guard_greater_test() ->
    Ctx = simple_ctx(),
    %% hp is 50, filter for hp > 40 should pass
    ?assertEqual([50], flatbuferl_fetch:fetch(Ctx, [[{hp, '>', 40}, hp]])),
    %% hp > 60 should filter out
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [[{hp, '>', 60}, hp]])).

fetch_comparison_guard_less_test() ->
    Ctx = simple_ctx(),
    %% hp is 50, filter for hp < 60 should pass
    ?assertEqual([50], flatbuferl_fetch:fetch(Ctx, [[{hp, '<', 60}, hp]])),
    %% hp < 40 should filter out
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [[{hp, '<', 40}, hp]])).

fetch_comparison_guard_range_test() ->
    Ctx = simple_ctx(),
    %% hp is 50, filter for 40 < hp < 60 should pass
    ?assertEqual([50], flatbuferl_fetch:fetch(Ctx, [[{hp, '>', 40}, {hp, '<', 60}, hp]])),
    %% 60 < hp < 80 should filter out
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [[{hp, '>', 60}, {hp, '<', 80}, hp]])).

fetch_comparison_guard_equality_test() ->
    Ctx = simple_ctx(),
    %% hp is 50, filter for hp == 50 should pass
    ?assertEqual([50], flatbuferl_fetch:fetch(Ctx, [[{hp, '==', 50}, hp]])),
    %% hp /= 50 should filter out
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [[{hp, '/=', 50}, hp]])).

fetch_comparison_guard_on_vector_of_tables_test() ->
    %% Test comparison guards with wildcards on vector of tables
    %% Using table_vector_ctx - filter by string comparison
    Ctx = table_vector_ctx(),
    %% Filter for value_inner > "one" (lexicographic)
    %% Extraction spec returns list per match: [[value], [value], ...]
    Result = flatbuferl_fetch:fetch(Ctx, [inner, '*', [{value_inner, '>', <<"one">>}, value_inner]]),
    %% "two" and "three" are > "one" lexicographically (in original order)
    ?assertEqual([[<<"two">>], [<<"three">>]], Result).

fetch_membership_guard_in_test() ->
    Ctx = table_vector_ctx(),
    %% Filter for value_inner in ["one", "three"]
    Result = flatbuferl_fetch:fetch(Ctx, [
        inner, '*', [{value_inner, in, [<<"one">>, <<"three">>]}, value_inner]
    ]),
    ?assertEqual([[<<"one">>], [<<"three">>]], Result).

fetch_membership_guard_not_in_test() ->
    Ctx = table_vector_ctx(),
    %% Filter for value_inner not in ["one", "three"]
    Result = flatbuferl_fetch:fetch(Ctx, [
        inner, '*', [{value_inner, not_in, [<<"one">>, <<"three">>]}, value_inner]
    ]),
    ?assertEqual([[<<"two">>]], Result).

%% =============================================================================
%% Nested Paths in Extraction
%% =============================================================================

fetch_nested_path_extraction_test() ->
    Ctx = nested_ctx(),
    %% Extract name and nested pos.x in a single extraction spec
    Result = flatbuferl_fetch:fetch(Ctx, [[name, [pos, x]]]),
    ?assertEqual([<<"Player">>, 1.0], Result).

fetch_nested_path_extraction_multiple_test() ->
    Ctx = nested_ctx(),
    %% Extract multiple nested paths
    Result = flatbuferl_fetch:fetch(Ctx, [[[pos, x], [pos, y], [pos, z]]]),
    ?assertEqual([1.0, 2.0, 3.0], Result).

fetch_nested_path_in_nested_extraction_test() ->
    Ctx = nested_ctx(),
    %% Extract with nested extraction inside
    Result = flatbuferl_fetch:fetch(Ctx, [[name, [pos, [x, y]]]]),
    ?assertEqual([<<"Player">>, [1.0, 2.0]], Result).

%% =============================================================================
%% Nested Paths in Guards
%% =============================================================================

fetch_nested_path_guard_test() ->
    Ctx = nested_ctx(),
    %% Guard on nested path [pos, x]
    Result = flatbuferl_fetch:fetch(Ctx, [[{[pos, x], 1.0}, name]]),
    ?assertEqual([<<"Player">>], Result).

fetch_nested_path_guard_comparison_test() ->
    Ctx = nested_ctx(),
    %% Comparison guard on nested path
    Result = flatbuferl_fetch:fetch(Ctx, [[{[pos, x], '>', 0.0}, name]]),
    ?assertEqual([<<"Player">>], Result).

fetch_nested_path_guard_fails_test() ->
    Ctx = nested_ctx(),
    %% Guard that fails
    Result = flatbuferl_fetch:fetch(Ctx, [[{[pos, x], '>', 10.0}, name]]),
    ?assertEqual(undefined, Result).

%% =============================================================================
%% Vector of Unions
%% =============================================================================

union_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData', 'IntData'],
        data => [
            #{data => [<<"hello">>, <<"world">>]},
            #{data => [1, 2, 3]}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_union_vector_types_test() ->
    Ctx = union_vector_ctx(),
    ?assertEqual(['StringData', 'IntData'], flatbuferl_fetch:fetch(Ctx, [data, '*', '_type'])).

fetch_union_vector_index_test() ->
    Ctx = union_vector_ctx(),
    %% Access first union element
    Result = flatbuferl_fetch:fetch(Ctx, [data, 0]),
    ?assertEqual(#{data => [<<"hello">>, <<"world">>]}, Result).

fetch_union_vector_index_type_test() ->
    Ctx = union_vector_ctx(),
    ?assertEqual('StringData', flatbuferl_fetch:fetch(Ctx, [data, 0, '_type'])),
    ?assertEqual('IntData', flatbuferl_fetch:fetch(Ctx, [data, 1, '_type'])).

fetch_union_vector_filter_by_type_test() ->
    Ctx = union_vector_ctx(),
    %% Filter to only StringData unions and extract their data
    Result = flatbuferl_fetch:fetch(Ctx, [data, '*', [{'_type', 'StringData'}, data]]),
    ?assertEqual([[[<<"hello">>, <<"world">>]]], Result).

fetch_union_vector_size_test() ->
    Ctx = union_vector_ctx(),
    ?assertEqual(2, flatbuferl_fetch:fetch(Ctx, [data, '_size'])).

%% =============================================================================
%% Edge Cases
%% =============================================================================

fetch_undefined_for_missing_vector_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/vectors/test_vector.fbs"),
    %% Create buffer with only items, no counts
    Data = #{items => [<<"a">>]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [counts, '*'])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [counts, 0])).

%% =============================================================================
%% Struct Fetch Tests (exercises struct field reading paths)
%% =============================================================================

struct_field_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Entity { pos: Vec3; name: string; }\n"
        "root_type Entity;\n"
    ),
    Data = #{pos => #{x => 1.5, y => 2.5, z => 3.5}, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_struct_field_x_test() ->
    Ctx = struct_field_ctx(),
    X = flatbuferl_fetch:fetch(Ctx, [pos, x]),
    ?assert(abs(X - 1.5) < 0.01).

fetch_struct_field_y_test() ->
    Ctx = struct_field_ctx(),
    Y = flatbuferl_fetch:fetch(Ctx, [pos, y]),
    ?assert(abs(Y - 2.5) < 0.01).

fetch_struct_field_z_test() ->
    Ctx = struct_field_ctx(),
    Z = flatbuferl_fetch:fetch(Ctx, [pos, z]),
    ?assert(abs(Z - 3.5) < 0.01).

fetch_struct_full_test() ->
    Ctx = struct_field_ctx(),
    Pos = flatbuferl_fetch:fetch(Ctx, [pos]),
    ?assert(abs(maps:get(x, Pos) - 1.5) < 0.01),
    ?assert(abs(maps:get(y, Pos) - 2.5) < 0.01),
    ?assert(abs(maps:get(z, Pos) - 3.5) < 0.01).

%% =============================================================================
%% Missing Struct Field Tests
%% =============================================================================

missing_struct_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Entity { pos: Vec3; name: string; }\n"
        "root_type Entity;\n"
    ),
    %% Create entity without pos field
    Data = #{name => <<"no_position">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_missing_struct_test() ->
    Ctx = missing_struct_ctx(),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [pos])).

fetch_missing_struct_field_test() ->
    Ctx = missing_struct_ctx(),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [pos, x])).

%% =============================================================================
%% Long/Double Field Tests (exercises 64-bit types)
%% =============================================================================

large_types_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { big_int: long; big_uint: ulong; big_float: double; }\n"
        "root_type Data;\n"
    ),
    Data = #{
        big_int => -9223372036854775808,
        big_uint => 18446744073709551615,
        big_float => 3.141592653589793
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_long_test() ->
    Ctx = large_types_ctx(),
    ?assertEqual(-9223372036854775808, flatbuferl_fetch:fetch(Ctx, [big_int])).

fetch_ulong_test() ->
    Ctx = large_types_ctx(),
    ?assertEqual(18446744073709551615, flatbuferl_fetch:fetch(Ctx, [big_uint])).

fetch_double_test() ->
    Ctx = large_types_ctx(),
    D = flatbuferl_fetch:fetch(Ctx, [big_float]),
    ?assert(abs(D - 3.141592653589793) < 0.0000001).

%% =============================================================================
%% Error Path Tests (exercises error handling branches)
%% =============================================================================

fetch_unknown_field_error_test() ->
    Ctx = struct_field_ctx(),
    ?assertError({unknown_field, nonexistent}, flatbuferl_fetch:fetch(Ctx, [nonexistent])).

fetch_unknown_nested_field_returns_undefined_test() ->
    Ctx = struct_field_ctx(),
    %% pos exists but 'w' doesn't exist in Vec3 - fetch returns undefined
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [pos, w])).

fetch_cannot_traverse_scalar_test() ->
    Ctx = large_types_ctx(),
    %% big_int is a scalar, can't traverse into it
    ?assertError({not_a_table, big_int, long}, flatbuferl_fetch:fetch(Ctx, [big_int, foo])).

%% =============================================================================
%% Sparse VTable Tests (fields missing from buffer)
%% =============================================================================

sparse_vtable_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 5); c: int (id: 10); }\n"
        "root_type Data;\n"
    ),
    %% Only set 'a', leave b and c missing - vtable will be small
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_sparse_present_field_test() ->
    Ctx = sparse_vtable_ctx(),
    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])).

fetch_sparse_missing_field_test() ->
    Ctx = sparse_vtable_ctx(),
    %% b has id:5, should be missing
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [b])).

fetch_sparse_high_id_missing_test() ->
    Ctx = sparse_vtable_ctx(),
    %% c has id:10, vtable likely too small to even have slot
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [c])).

%% =============================================================================
%% Vector Out of Bounds Tests
%% =============================================================================

vector_oob_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { items: [int]; }\n"
        "root_type Data;\n"
    ),
    Data = #{items => [1, 2, 3]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_vector_oob_positive_test() ->
    Ctx = vector_oob_ctx(),
    %% Index 10 is out of bounds for 3-element vector
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items, 10])).

fetch_vector_oob_negative_test() ->
    Ctx = vector_oob_ctx(),
    %% Index -10 is out of bounds
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items, -10])).

%% =============================================================================
%% Empty Vector Tests
%% =============================================================================

empty_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { items: [int]; name: string; }\n"
        "root_type Data;\n"
    ),
    Data = #{items => [], name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_empty_vector_wildcard_test() ->
    Ctx = empty_vector_ctx(),
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [items, '*'])).

fetch_empty_vector_size_test() ->
    Ctx = empty_vector_ctx(),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [items, '_size'])).

fetch_empty_vector_index_test() ->
    Ctx = empty_vector_ctx(),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items, 0])).

%% =============================================================================
%% Union Vector Tests
%% =============================================================================

union_vector_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "table Shield { defense: int; }\n"
        "union Equipment { Sword, Shield }\n"
        "table Player { items: [Equipment]; }\n"
        "root_type Player;\n"
    ),
    Data = #{
        items => [#{damage => 50}, #{defense => 30}],
        items_type => ['Sword', 'Shield']
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_union_vector_element_test() ->
    Ctx = union_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [items, 0]),
    %% Fetch returns just the map contents, not a tuple
    ?assertEqual(50, maps:get(damage, Result)).

fetch_union_vector_second_element_test() ->
    Ctx = union_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [items, 1]),
    ?assertEqual(30, maps:get(defense, Result)).

fetch_union_vector_element_type_test() ->
    Ctx = union_vector_fetch_ctx(),
    %% Can get type separately with '_type'
    ?assertEqual('Sword', flatbuferl_fetch:fetch(Ctx, [items, 0, '_type'])),
    ?assertEqual('Shield', flatbuferl_fetch:fetch(Ctx, [items, 1, '_type'])).

fetch_union_vector_wildcard_test() ->
    Ctx = union_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [items, '*']),
    ?assertEqual(2, length(Result)).

%% =============================================================================
%% Table Vector Tests
%% =============================================================================

table_vector_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Item { name: string; value: int; }\n"
        "table Inventory { items: [Item]; }\n"
        "root_type Inventory;\n"
    ),
    Data = #{
        items => [
            #{name => <<"Sword">>, value => 100},
            #{name => <<"Shield">>, value => 50}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_table_vector_element_test() ->
    Ctx = table_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [items, 0]),
    ?assertEqual(<<"Sword">>, maps:get(name, Result)).

fetch_table_vector_field_test() ->
    Ctx = table_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [items, 0, value]),
    ?assertEqual(100, Result).

fetch_table_vector_negative_index_test() ->
    Ctx = table_vector_fetch_ctx(),
    %% -1 is last element
    Result = flatbuferl_fetch:fetch(Ctx, [items, -1, name]),
    ?assertEqual(<<"Shield">>, Result).

%% =============================================================================
%% Nested Struct Tests
%% =============================================================================

nested_struct_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Entity { pos: Vec3; name: string; }\n"
        "root_type Entity;\n"
    ),
    Data = #{pos => #{x => 1.0, y => 2.0, z => 3.0}, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_nested_struct_field_test() ->
    Ctx = nested_struct_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [pos, x]),
    ?assert(abs(Result - 1.0) < 0.001).

fetch_nested_struct_all_test() ->
    Ctx = nested_struct_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [pos]),
    ?assertEqual(3, map_size(Result)).

%% =============================================================================
%% Missing Nested Table Tests
%% =============================================================================

missing_nested_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { x: int; y: int; }\n"
        "table Outer { inner: Inner; name: string; }\n"
        "root_type Outer;\n"
    ),
    %% Create without inner table
    Data = #{name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_missing_nested_table_test() ->
    Ctx = missing_nested_fetch_ctx(),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [inner])).

fetch_missing_nested_table_field_test() ->
    Ctx = missing_nested_fetch_ctx(),
    %% Traversing into missing nested table
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [inner, x])).

%% =============================================================================
%% Array Type Tests
%% =============================================================================

array_type_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Point { coords: [float:3]; }\n"
        "table Data { pt: Point; }\n"
        "root_type Data;\n"
    ),
    Data = #{pt => #{coords => [1.0, 2.0, 3.0]}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_array_field_test() ->
    Ctx = array_type_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [pt, coords]),
    ?assertEqual(3, length(Result)),
    [X, Y, Z] = Result,
    ?assert(abs(X - 1.0) < 0.001),
    ?assert(abs(Y - 2.0) < 0.001),
    ?assert(abs(Z - 3.0) < 0.001).

%% =============================================================================
%% Byte Array (Binary) Tests
%% =============================================================================

byte_array_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Hash { data: [byte:4]; }\n"
        "table Doc { hash: Hash; }\n"
        "root_type Doc;\n"
    ),
    Data = #{hash => #{data => <<1, 2, 3, 4>>}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_byte_array_test() ->
    Ctx = byte_array_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [hash, data]),
    ?assertEqual(<<1, 2, 3, 4>>, Result).

%% =============================================================================
%% Default Value Tests
%% =============================================================================

default_value_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Monster { hp: int = 100; mana: int = 50; name: string; }\n"
        "root_type Monster;\n"
    ),
    %% Only set name, rely on defaults for hp/mana
    Data = #{name => <<"Goblin">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_unset_field_with_default_test() ->
    Ctx = default_value_fetch_ctx(),
    %% hp wasn't set but has default 100 - fetch returns the default
    ?assertEqual(100, flatbuferl_fetch:fetch(Ctx, [hp])).

fetch_set_field_test() ->
    Ctx = default_value_fetch_ctx(),
    %% name was set
    ?assertEqual(<<"Goblin">>, flatbuferl_fetch:fetch(Ctx, [name])).

%% =============================================================================
%% Enum Field Tests
%% =============================================================================

enum_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "enum Color : byte { Red = 0, Green = 1, Blue = 2 }\n"
        "table Item { color: Color; value: int; }\n"
        "root_type Item;\n"
    ),
    Data = #{color => 'Green', value => 100},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_enum_field_test() ->
    Ctx = enum_fetch_ctx(),
    %% Fetch returns raw enum value (1), not the symbolic name
    Result = flatbuferl_fetch:fetch(Ctx, [color]),
    ?assertEqual(1, Result).

%% =============================================================================
%% Deeply Nested Tests
%% =============================================================================

deeply_nested_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { value: int; }\n"
        "table Middle { inner: Inner; }\n"
        "table Outer { middle: Middle; }\n"
        "root_type Outer;\n"
    ),
    Data = #{middle => #{inner => #{value => 42}}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_deeply_nested_test() ->
    Ctx = deeply_nested_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [middle, inner, value]),
    ?assertEqual(42, Result).

%% =============================================================================
%% Vector of Vectors Test
%% =============================================================================

nested_vector_fetch_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Row { values: [int]; }\n"
        "table Matrix { rows: [Row]; }\n"
        "root_type Matrix;\n"
    ),
    Data = #{
        rows => [
            #{values => [1, 2, 3]},
            #{values => [4, 5, 6]}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_nested_vector_element_test() ->
    Ctx = nested_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [rows, 0, values, 1]),
    ?assertEqual(2, Result).

fetch_nested_vector_size_test() ->
    Ctx = nested_vector_fetch_ctx(),
    Result = flatbuferl_fetch:fetch(Ctx, [rows, '_size']),
    ?assertEqual(2, Result).

%% =============================================================================
%% Additional Missing Field Tests
%% =============================================================================

sparse_with_many_missing_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 3); c: int (id: 7); d: int (id: 15); }\n"
        "root_type Data;\n"
    ),
    %% Only set 'a', all others will be missing from vtable
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

fetch_sparse_many_missing_test() ->
    Ctx = sparse_with_many_missing_ctx(),
    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [b])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [c])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [d])).

%% =============================================================================
%% VTable Too Small Tests (Schema Evolution)
%% =============================================================================

%% Tests the case where a buffer was created with an older schema
%% and is read with a newer schema that has more fields.
vtable_too_small_test() ->
    %% Create buffer with minimal schema (only field id:0)
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    %% Read with extended schema (has field id:5 which doesn't exist in vtable)
    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 5); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),

    %% Field 'a' exists and has value
    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    %% Field 'b' has id:5 which is beyond vtable size - returns undefined
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [b])).

%% Similar test for nested table reference fields
vtable_too_small_ref_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { a: int (id: 0); inner: Inner (id: 5); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),

    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [inner])).

%% Test vtable too small for struct field
vtable_too_small_struct_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Data { a: int (id: 0); pos: Vec3 (id: 5); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),

    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [pos])).

%% Test vtable too small for union type field
vtable_too_small_union_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { a: int (id: 0); weapon: Weapon (id: 6); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),

    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [weapon])).

%% Test vtable too small for vector field
vtable_too_small_vector_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); items: [int] (id: 5); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),

    ?assertEqual(42, flatbuferl_fetch:fetch(Ctx, [a])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [items])).
