-module(fetch_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test Fixtures
%% =============================================================================

simple_ctx() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Data = #{name => <<"Goblin">>, hp => 50, mana => 25},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

nested_ctx() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_nested.fbs"),
    Data = #{name => <<"Player">>, hp => 100, pos => #{x => 1.0, y => 2.0, z => 3.0}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

vector_ctx() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Data = #{items => [<<"sword">>, <<"shield">>, <<"potion">>], counts => [1, 2, 3]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

table_vector_ctx() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/schemas/table_vector.fbs"),
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
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
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
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
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
%% _size Pseudo-field
%% =============================================================================

fetch_size_of_vector_test() ->
    Ctx = vector_ctx(),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [items, '_size'])),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [counts, '_size'])).

fetch_size_of_empty_vector_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Data = #{items => [], counts => []},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [items, '_size'])).

fetch_size_of_missing_vector_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    Data = #{items => [<<"a">>]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [counts, '_size'])).

fetch_size_of_table_vector_test() ->
    Ctx = table_vector_ctx(),
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [inner, '_size'])).

fetch_size_of_string_test() ->
    Ctx = simple_ctx(),
    ?assertEqual(6, flatbuferl_fetch:fetch(Ctx, [name, '_size'])).  %% <<"Goblin">> = 6 bytes

fetch_size_of_missing_string_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),
    Data = #{hp => 50},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [name, '_size'])).

%% =============================================================================
%% _type Pseudo-field (Unions)
%% =============================================================================

union_ctx(Type, Value) ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/schemas/union_field.fbs"),
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

fetch_union_field_wrong_type_test() ->
    %% Accessing 'greeting' when the union is 'hello' (which has 'salute')
    Ctx = union_ctx(hello, #{salute => <<"Hi">>}),
    ?assertError({unknown_field, greeting}, flatbuferl_fetch:fetch(Ctx, [data, greeting])).

fetch_union_extract_field_test() ->
    Ctx = union_ctx(bye, #{greeting => 99}),
    ?assertEqual([99], flatbuferl_fetch:fetch(Ctx, [data, [greeting]])).

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
        flatbuferl_fetch:fetch(Ctx, [inner, '*', [{value_inner, <<"one">>}, {value_inner, <<"three">>}, value_inner]])
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
%% Edge Cases
%% =============================================================================

fetch_undefined_for_missing_vector_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_vector.fbs"),
    %% Create buffer with only items, no counts
    Data = #{items => [<<"a">>]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [counts, '*'])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [counts, 0])).
