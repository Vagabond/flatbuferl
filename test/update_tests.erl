-module(update_tests).
-include_lib("eunit/include/eunit.hrl").

%% Test helpers
ctx_schema(Ctx) ->
    %% Reconstruct schema from ctx record fields
    %% ctx record: {ctx, buffer, defs, root_type, root}
    {element(3, Ctx), #{root_type => element(4, Ctx)}}.

simple_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Monster {\n"
        "            name: string;\n"
        "            hp: int = 100;\n"
        "            mana: int = 50;\n"
        "            level: ubyte = 1;\n"
        "        }\n"
        "        root_type Monster;\n"
        "    "
    ),
    Data = #{name => <<"Goblin">>, hp => 75, level => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

nested_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        struct Vec3 {\n"
        "            x: float;\n"
        "            y: float;\n"
        "            z: float;\n"
        "        }\n"
        "        table Entity {\n"
        "            name: string;\n"
        "            pos: Vec3;\n"
        "            hp: int = 100;\n"
        "        }\n"
        "        root_type Entity;\n"
        "    "
    ),
    Data = #{name => <<"Player">>, pos => #{x => 1.0, y => 2.0, z => 3.0}, hp => 200},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

%% =============================================================================
%% Preflight Tests
%% =============================================================================

preflight_simple_scalar_test() ->
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{hp => 150}),
    ?assertMatch({simple, [{_, 4, int, [hp], 150}]}, Result).

preflight_multiple_scalars_test() ->
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{hp => 150, level => 10}),
    ?assertMatch({simple, _}, Result),
    {simple, Updates} = Result,
    ?assertEqual(2, length(Updates)).

preflight_string_shrink_is_simple_test() ->
    %% Original name is "Goblin" (6 chars), "Orc" (3 chars) fits - simple shrink
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{name => <<"Orc">>}),
    ?assertMatch({simple, [{_, _, {string_shrink, _}, [name], <<"Orc">>}]}, Result).

preflight_string_grow_is_complex_test() ->
    %% Original name is "Goblin" (6 chars), "Dragon Lord" (11 chars) doesn't fit
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{name => <<"Dragon Lord">>}),
    ?assertMatch({complex, _}, Result).

preflight_mixed_with_shrinkable_string_test() ->
    %% Scalar + shrinkable string = all simple
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{hp => 150, name => <<"Orc">>}),
    ?assertMatch({simple, _}, Result),
    {simple, Updates} = Result,
    ?assertEqual(2, length(Updates)).

preflight_missing_field_is_complex_test() ->
    Ctx = simple_ctx(),
    %% mana was not set in the buffer, so it's using default - can't splice
    Result = flatbuferl_update:preflight(Ctx, #{mana => 100}),
    ?assertMatch({complex, _}, Result).

preflight_unknown_field_error_test() ->
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{bogus => 123}),
    ?assertMatch({error, {unknown_field, bogus}}, Result).

preflight_type_mismatch_error_test() ->
    Ctx = simple_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{hp => <<"not an int">>}),
    ?assertMatch({error, {type_mismatch, int, _}}, Result).

preflight_nested_struct_field_test() ->
    Ctx = nested_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{pos => #{x => 5.0}}),
    ?assertMatch({simple, [{_, 4, float, [pos, x], 5.0}]}, Result).

preflight_nested_struct_multiple_test() ->
    Ctx = nested_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{pos => #{x => 5.0, z => 10.0}}),
    ?assertMatch({simple, _}, Result),
    {simple, Updates} = Result,
    ?assertEqual(2, length(Updates)).

%% =============================================================================
%% Union Tests
%% =============================================================================

union_ctx(Type, Value) ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Sword { damage: int; }\n"
        "        table Shield { defense: int; }\n"
        "        union Equipment { Sword, Shield }\n"
        "        table Player {\n"
        "            name: string;\n"
        "            equipped: Equipment;\n"
        "        }\n"
        "        root_type Player;\n"
        "    "
    ),
    Data = #{name => <<"Hero">>, equipped => Value, equipped_type => Type},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_union_field_present_test() ->
    %% Sword has 'damage', and we have a Sword equipped
    Ctx = union_ctx('Sword', #{damage => 50}),
    Result = flatbuferl_update:preflight(Ctx, #{equipped => #{damage => 100}}),
    ?assertMatch({simple, [{_, 4, int, [equipped, damage], 100}]}, Result).

preflight_union_field_wrong_type_test() ->
    %% Shield has 'defense' not 'damage', but schema allows damage on Sword
    %% This buffer has Shield, so we can't splice damage - it doesn't exist
    Ctx = union_ctx('Shield', #{defense => 25}),
    Result = flatbuferl_update:preflight(Ctx, #{equipped => #{damage => 100}}),
    %% Should be complex (or error?) because field doesn't exist in this union variant
    ?assertMatch({error, {unknown_field, damage}}, Result).

preflight_union_correct_field_for_type_test() ->
    %% Shield has 'defense', buffer has Shield
    Ctx = union_ctx('Shield', #{defense => 25}),
    Result = flatbuferl_update:preflight(Ctx, #{equipped => #{defense => 50}}),
    ?assertMatch({simple, [{_, 4, int, [equipped, defense], 50}]}, Result).

%% =============================================================================
%% Union Vector Tests
%% =============================================================================

union_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Sword { damage: int; }\n"
        "        table Shield { defense: int; }\n"
        "        union Equipment { Sword, Shield }\n"
        "        table Inventory {\n"
        "            items: [Equipment];\n"
        "        }\n"
        "        root_type Inventory;\n"
        "    "
    ),
    Data = #{
        items => [#{damage => 10}, #{defense => 20}, #{damage => 30}],
        items_type => ['Sword', 'Shield', 'Sword']
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_union_vector_element_test() ->
    Ctx = union_vector_ctx(),
    %% Update damage on first item (a Sword)
    Result = flatbuferl_update:preflight(Ctx, #{items => #{0 => #{damage => 100}}}),
    ?assertMatch({simple, [{_, 4, int, [items, 0, damage], 100}]}, Result).

preflight_union_vector_wrong_type_test() ->
    Ctx = union_vector_ctx(),
    %% Try to update damage on second item (a Shield - doesn't have damage)
    Result = flatbuferl_update:preflight(Ctx, #{items => #{1 => #{damage => 100}}}),
    ?assertMatch({error, {unknown_field, damage}}, Result).

preflight_union_vector_third_element_test() ->
    Ctx = union_vector_ctx(),
    %% Update damage on third item (also a Sword)
    Result = flatbuferl_update:preflight(Ctx, #{items => #{2 => #{damage => 200}}}),
    ?assertMatch({simple, [{_, 4, int, [items, 2, damage], 200}]}, Result).

%% =============================================================================
%% Enum Tests
%% =============================================================================

enum_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        enum Color : ubyte { Red, Green, Blue }\n"
        "        table Pixel {\n"
        "            x: int;\n"
        "            y: int;\n"
        "            color: Color = Red;\n"
        "        }\n"
        "        root_type Pixel;\n"
        "    "
    ),
    Data = #{x => 10, y => 20, color => 'Green'},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_enum_field_test() ->
    Ctx = enum_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{color => 'Blue'}),
    %% Value is converted to integer (Blue = 2, 0-indexed) for encoding
    ?assertMatch({simple, [{_, 1, _, [color], 2}]}, Result).

preflight_enum_invalid_value_test() ->
    Ctx = enum_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{color => 'Purple'}),
    ?assertMatch({error, {invalid_enum_value, 'Color', 'Purple'}}, Result).

%% =============================================================================
%% Update Tests (actual splice)
%% =============================================================================

update_simple_scalar_test() ->
    Ctx = simple_ctx(),
    %% Original hp is 75
    ?assertEqual(75, flatbuferl:get(Ctx, [hp])),
    %% Update to 150
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 150})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(150, flatbuferl:get(NewCtx, [hp])),
    %% Other fields unchanged
    ?assertEqual(<<"Goblin">>, flatbuferl:get(NewCtx, [name])),
    ?assertEqual(5, flatbuferl:get(NewCtx, [level])).

update_multiple_scalars_test() ->
    Ctx = simple_ctx(),
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 200, level => 99})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(200, flatbuferl:get(NewCtx, [hp])),
    ?assertEqual(99, flatbuferl:get(NewCtx, [level])),
    ?assertEqual(<<"Goblin">>, flatbuferl:get(NewCtx, [name])).

update_nested_struct_test() ->
    Ctx = nested_ctx(),
    %% Use fetch for struct path access (get/2 doesn't support struct traversal)
    ?assertEqual(1.0, flatbuferl_fetch:fetch(Ctx, [pos, x])),
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{pos => #{x => 99.5}})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(99.5, flatbuferl_fetch:fetch(NewCtx, [pos, x])),
    %% Other struct fields unchanged
    ?assertEqual(2.0, flatbuferl_fetch:fetch(NewCtx, [pos, y])),
    ?assertEqual(3.0, flatbuferl_fetch:fetch(NewCtx, [pos, z])).

update_string_shrink_test() ->
    Ctx = simple_ctx(),
    %% Original name "Goblin" (6), new name "Orc" (3) - fits, simple splice
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{name => <<"Orc">>})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(<<"Orc">>, flatbuferl:get(NewCtx, [name])),
    %% Other fields unchanged
    ?assertEqual(75, flatbuferl:get(NewCtx, [hp])).

update_string_grow_fallback_test() ->
    Ctx = simple_ctx(),
    %% Original name "Goblin" (6), new name "Dragon Lord" (11) - doesn't fit, complex fallback
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{name => <<"Dragon Lord">>})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(<<"Dragon Lord">>, flatbuferl:get(NewCtx, [name])),
    %% Other fields unchanged
    ?assertEqual(75, flatbuferl:get(NewCtx, [hp])).

update_enum_field_test() ->
    Ctx = enum_ctx(),
    ?assertEqual('Green', flatbuferl:get(Ctx, [color])),
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{color => 'Blue'})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual('Blue', flatbuferl:get(NewCtx, [color])),
    %% Other fields unchanged
    ?assertEqual(10, flatbuferl:get(NewCtx, [x])),
    ?assertEqual(20, flatbuferl:get(NewCtx, [y])).

%% =============================================================================
%% Zero-Copy Verification Tests
%% =============================================================================

large_buffer_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table BigData {\n"
        "            id: int;\n"
        "            payload: [ubyte];\n"
        "            count: int;\n"
        "        }\n"
        "        root_type BigData;\n"
        "    "
    ),
    %% Create a 1MB payload - binary accepted for [ubyte]
    Payload = binary:copy(<<0>>, 1024 * 1024),
    Data = #{id => 1, payload => Payload, count => 100},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    {flatbuferl:new(Buffer, Schema), Schema}.

update_scalar_no_large_binary_copy_test() ->
    %% Verify that updating a scalar in a large buffer doesn't create a new
    %% large binary. We check the binary table before and after the update.
    %% Sub-binaries reference the original refc binary - no new allocation.
    {Ctx, Schema} = large_buffer_ctx(),
    Buffer = flatbuferl:ctx_buffer(Ctx),
    BufferSize = byte_size(Buffer),

    %% Sanity check - buffer should be > 1MB
    ?assert(BufferSize > 1000000),

    Parent = self(),
    spawn_link(fun() ->
        %% Snapshot binary table BEFORE update
        erlang:garbage_collect(),
        {binary, BinsBefore} = process_info(self(), binary),
        LargeBinsBefore = [Size || {_, Size, _} <- BinsBefore, Size > 10000],

        %% Do the update - should return iolist with sub-binary refs
        IoList = flatbuferl:update(Ctx, #{count => 999}),

        %% Snapshot binary table AFTER update (before flattening)
        erlang:garbage_collect(),
        {binary, BinsAfter} = process_info(self(), binary),
        LargeBinsAfter = [Size || {_, Size, _} <- BinsAfter, Size > 10000],

        %% Get heap size
        {heap_size, HeapWords} = process_info(self(), heap_size),
        HeapBytes = HeapWords * erlang:system_info(wordsize),

        %% Flatten to verify correctness (this WILL create a new binary)
        NewBuffer = iolist_to_binary(IoList),
        NewCtx = flatbuferl:new(NewBuffer, Schema),
        Value = flatbuferl:get(NewCtx, [count]),

        Parent ! {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, Value, byte_size(NewBuffer)}
    end),

    receive
        {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, Value, NewSize} ->
            %% KEY ASSERTION 1: Heap should be tiny (sub-binaries are just pointers)
            %% If we were copying the 1MB buffer, heap would be > 1MB
            ?assert(HeapBytes < 100000),

            %% KEY ASSERTION 2: No NEW large binaries created
            %% After GC, we should have <= the number we started with
            %% (GC may collect some references, but update shouldn't ADD any)
            ?assert(length(LargeBinsAfter) =< length(LargeBinsBefore)),

            %% The large binary should still be the original buffer size
            %% (not a copy, not doubled, etc.)
            ?assert(lists:member(BufferSize, LargeBinsAfter)),

            %% Verify the update worked
            ?assertEqual(999, Value),
            ?assertEqual(BufferSize, NewSize)
    after 5000 ->
        ?assert(false)
    end.

%% =============================================================================
%% Vector Shrink Tests
%% =============================================================================

vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Data {\n"
        "            id: int;\n"
        "            scores: [int];\n"
        "            name: string;\n"
        "        }\n"
        "        root_type Data;\n"
        "    "
    ),
    Data = #{id => 1, scores => [10, 20, 30, 40, 50], name => <<"Test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    {flatbuferl:new(Buffer, Schema), Schema}.

preflight_vector_shrink_is_simple_test() ->
    {Ctx, _Schema} = vector_ctx(),
    %% Original: 5 elements, new: 3 elements - fits
    Result = flatbuferl_update:preflight(Ctx, #{scores => [1, 2, 3]}),
    ?assertMatch({simple, [{_, _, {vector_shrink, int, 4, _}, [scores], [1, 2, 3]}]}, Result).

preflight_vector_grow_is_complex_test() ->
    {Ctx, _Schema} = vector_ctx(),
    %% Original: 5 elements, new: 7 elements - doesn't fit
    Result = flatbuferl_update:preflight(Ctx, #{scores => [1, 2, 3, 4, 5, 6, 7]}),
    ?assertMatch({complex, _}, Result).

update_vector_shrink_test() ->
    {Ctx, Schema} = vector_ctx(),
    %% Shrink from 5 to 3 elements
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{scores => [100, 200, 300]})),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    ?assertEqual([100, 200, 300], flatbuferl:get(NewCtx, [scores])),
    %% Other fields unchanged
    ?assertEqual(1, flatbuferl:get(NewCtx, [id])),
    ?assertEqual(<<"Test">>, flatbuferl:get(NewCtx, [name])).

update_vector_grow_fallback_test() ->
    {Ctx, Schema} = vector_ctx(),
    %% Grow from 5 to 7 elements - falls back to re-encode
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{scores => [1, 2, 3, 4, 5, 6, 7]})),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    ?assertEqual([1, 2, 3, 4, 5, 6, 7], flatbuferl:get(NewCtx, [scores])),
    ?assertEqual(1, flatbuferl:get(NewCtx, [id])).

update_byte_vector_shrink_test() ->
    {Ctx, Schema} = large_buffer_ctx(),
    %% Original payload is 1MB, shrink to 100 bytes
    SmallPayload = binary:copy(<<1>>, 100),
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{payload => SmallPayload})),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    %% Verify the payload was updated (returns as list for [ubyte])
    Payload = flatbuferl:get(NewCtx, [payload]),
    ?assertEqual(100, length(Payload)),
    ?assert(lists:all(fun(X) -> X == 1 end, Payload)),
    %% Buffer size unchanged (in-place update)
    ?assertEqual(byte_size(flatbuferl:ctx_buffer(Ctx)), byte_size(NewBuffer)).

%% =============================================================================
%% Zero-Copy Shrink Tests
%% =============================================================================

shrink_no_large_binary_copy_test() ->
    %% Verify that shrinking a large string/vector doesn't create new large binaries
    {Ctx, _Schema} = large_buffer_ctx(),
    Buffer = flatbuferl:ctx_buffer(Ctx),
    BufferSize = byte_size(Buffer),

    Parent = self(),
    spawn_link(fun() ->
        %% Snapshot binary table BEFORE shrink
        erlang:garbage_collect(),
        {binary, BinsBefore} = process_info(self(), binary),
        LargeBinsBefore = [Size || {_, Size, _} <- BinsBefore, Size > 10000],

        %% Shrink the 1MB payload to 100 bytes - should be in-place
        SmallPayload = binary:copy(<<1>>, 100),
        IoList = flatbuferl:update(Ctx, #{payload => SmallPayload}),

        %% Snapshot binary table AFTER shrink (before flattening)
        erlang:garbage_collect(),
        {binary, BinsAfter} = process_info(self(), binary),
        LargeBinsAfter = [Size || {_, Size, _} <- BinsAfter, Size > 10000],

        %% Get heap size
        {heap_size, HeapWords} = process_info(self(), heap_size),
        HeapBytes = HeapWords * erlang:system_info(wordsize),

        %% Flatten and verify
        NewBuffer = iolist_to_binary(IoList),

        Parent ! {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, byte_size(NewBuffer)}
    end),

    receive
        {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, NewSize} ->
            %% Heap should be small - no large copies
            ?assert(HeapBytes < 100000),
            %% No new large binaries created
            ?assert(length(LargeBinsAfter) =< length(LargeBinsBefore)),
            %% Buffer size unchanged (in-place shrink)
            ?assertEqual(BufferSize, NewSize)
    after 5000 ->
        ?assert(false)
    end.

same_size_string_update_test() ->
    %% Same-size string update should also be in-place
    Ctx = simple_ctx(),
    %% Original name is "Goblin" (6 chars), replace with "Dragon" (6 chars)
    Result = flatbuferl_update:preflight(Ctx, #{name => <<"Dragon">>}),
    ?assertMatch({simple, [{_, _, {string_shrink, _}, [name], <<"Dragon">>}]}, Result),
    %% Verify it works
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{name => <<"Dragon">>})),
    NewCtx = flatbuferl:new(NewBuffer, ctx_schema(Ctx)),
    ?assertEqual(<<"Dragon">>, flatbuferl:get(NewCtx, [name])).

same_size_vector_update_test() ->
    %% Same-size vector update should also be in-place
    {Ctx, Schema} = vector_ctx(),
    %% Original: 5 elements, replace with 5 different elements
    Result = flatbuferl_update:preflight(Ctx, #{scores => [5, 4, 3, 2, 1]}),
    ?assertMatch({simple, [{_, _, {vector_shrink, int, 4, _}, [scores], [5, 4, 3, 2, 1]}]}, Result),
    %% Verify it works
    NewBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{scores => [5, 4, 3, 2, 1]})),
    NewCtx = flatbuferl:new(NewBuffer, Schema),
    ?assertEqual([5, 4, 3, 2, 1], flatbuferl:get(NewCtx, [scores])).

same_size_no_copy_test() ->
    %% Verify same-size update of 1MB string is zero-copy
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Doc {\n"
        "            id: int;\n"
        "            content: string;\n"
        "        }\n"
        "        root_type Doc;\n"
        "    "
    ),
    BigContent = binary:copy(<<"A">>, 1024 * 1024),
    Data = #{id => 1, content => BigContent},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    BufferSize = byte_size(Buffer),

    %% Replace with same-size but different content
    NewContent = binary:copy(<<"B">>, 1024 * 1024),

    Parent = self(),
    spawn_link(fun() ->
        erlang:garbage_collect(),
        {binary, BinsBefore} = process_info(self(), binary),
        LargeBinsBefore = [Size || {_, Size, _} <- BinsBefore, Size > 10000],

        IoList = flatbuferl:update(Ctx, #{content => NewContent}),

        erlang:garbage_collect(),
        {binary, BinsAfter} = process_info(self(), binary),
        LargeBinsAfter = [Size || {_, Size, _} <- BinsAfter, Size > 10000],

        {heap_size, HeapWords} = process_info(self(), heap_size),
        HeapBytes = HeapWords * erlang:system_info(wordsize),

        NewBuffer = iolist_to_binary(IoList),
        NewCtx = flatbuferl:new(NewBuffer, Schema),
        %% Just check first byte changed
        <<FirstByte, _/binary>> = flatbuferl:get(NewCtx, [content]),

        Parent ! {result, HeapBytes, LargeBinsBefore, LargeBinsAfter,
                  byte_size(NewBuffer), FirstByte}
    end),

    receive
        {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, NewSize, FirstByte} ->
            ?assert(HeapBytes < 100000),
            ?assert(length(LargeBinsAfter) =< length(LargeBinsBefore)),
            ?assertEqual(BufferSize, NewSize),
            ?assertEqual($B, FirstByte)
    after 5000 ->
        ?assert(false)
    end.

string_shrink_no_copy_test() ->
    %% Test with a large string field
    {ok, Schema} = flatbuferl:parse_schema(
        "\n"
        "        table Doc {\n"
        "            id: int;\n"
        "            content: string;\n"
        "        }\n"
        "        root_type Doc;\n"
        "    "
    ),
    %% Create a 1MB string
    BigContent = binary:copy(<<"X">>, 1024 * 1024),
    Data = #{id => 1, content => BigContent},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    BufferSize = byte_size(Buffer),

    Parent = self(),
    spawn_link(fun() ->
        erlang:garbage_collect(),
        {binary, BinsBefore} = process_info(self(), binary),
        LargeBinsBefore = [Size || {_, Size, _} <- BinsBefore, Size > 10000],

        %% Shrink string from 1MB to 10 bytes
        IoList = flatbuferl:update(Ctx, #{content => <<"tiny">>}),

        erlang:garbage_collect(),
        {binary, BinsAfter} = process_info(self(), binary),
        LargeBinsAfter = [Size || {_, Size, _} <- BinsAfter, Size > 10000],

        {heap_size, HeapWords} = process_info(self(), heap_size),
        HeapBytes = HeapWords * erlang:system_info(wordsize),

        NewBuffer = iolist_to_binary(IoList),
        NewCtx = flatbuferl:new(NewBuffer, Schema),
        Content = flatbuferl:get(NewCtx, [content]),

        Parent ! {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, byte_size(NewBuffer), Content}
    end),

    receive
        {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, NewSize, Content} ->
            ?assert(HeapBytes < 100000),
            ?assert(length(LargeBinsAfter) =< length(LargeBinsBefore)),
            ?assertEqual(BufferSize, NewSize),
            ?assertEqual(<<"tiny">>, Content)
    after 5000 ->
        ?assert(false)
    end.

%% =============================================================================
%% Flatc Compatibility Tests
%% =============================================================================

flatc_scalar_update_test() ->
    %% Verify that a partial scalar update produces valid flatc-readable buffer
    TmpSchema = "/tmp/flatbuferl_update_test.fbs",
    TmpBin = "/tmp/flatbuferl_update_test.bin",
    TmpJson = "/tmp/flatbuferl_update_test.json",

    SchemaStr = "table Monster { name: string; hp: int = 100; level: ubyte = 1; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75, level => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Do partial update of hp
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 999})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format("flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
                        [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify
    {ok, JsonBin} = file:read_file(TmpJson),
    Json = json:decode(JsonBin),
    ?assertEqual(999, maps:get(<<"hp">>, Json)),
    ?assertEqual(<<"Goblin">>, maps:get(<<"name">>, Json)),
    ?assertEqual(5, maps:get(<<"level">>, Json)),

    %% Cleanup
    file:delete(TmpSchema),
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_string_shrink_test() ->
    %% Verify that a string shrink produces valid flatc-readable buffer
    TmpSchema = "/tmp/flatbuferl_shrink_test.fbs",
    TmpBin = "/tmp/flatbuferl_shrink_test.bin",
    TmpJson = "/tmp/flatbuferl_shrink_test.json",

    SchemaStr = "table Monster { name: string; hp: int = 100; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Shrink name from "Goblin" (6) to "Orc" (3)
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{name => <<"Orc">>})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format("flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
                        [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify
    {ok, JsonBin} = file:read_file(TmpJson),
    Json = json:decode(JsonBin),
    ?assertEqual(<<"Orc">>, maps:get(<<"name">>, Json)),
    ?assertEqual(75, maps:get(<<"hp">>, Json)),

    %% Cleanup
    file:delete(TmpSchema),
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_vector_shrink_test() ->
    %% Verify that a vector shrink produces valid flatc-readable buffer
    TmpSchema = "/tmp/flatbuferl_vecshrink_test.fbs",
    TmpBin = "/tmp/flatbuferl_vecshrink_test.bin",
    TmpJson = "/tmp/flatbuferl_vecshrink_test.json",

    SchemaStr = "table Data { id: int; scores: [int]; }\nroot_type Data;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{id => 42, scores => [10, 20, 30, 40, 50]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Shrink vector from 5 to 3 elements
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{scores => [100, 200, 300]})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format("flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
                        [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify
    {ok, JsonBin} = file:read_file(TmpJson),
    Json = json:decode(JsonBin),
    ?assertEqual(42, maps:get(<<"id">>, Json)),
    ?assertEqual([100, 200, 300], maps:get(<<"scores">>, Json)),

    %% Cleanup
    file:delete(TmpSchema),
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_same_size_string_test() ->
    %% Verify that same-size string replacement is valid per flatc
    TmpSchema = "/tmp/flatbuferl_samesize_test.fbs",
    TmpBin = "/tmp/flatbuferl_samesize_test.bin",
    TmpJson = "/tmp/flatbuferl_samesize_test.json",

    SchemaStr = "table Monster { name: string; hp: int; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Replace "Goblin" (6) with "Dragon" (6) - same size
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{name => <<"Dragon">>})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format("flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
                        [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify
    {ok, JsonBin} = file:read_file(TmpJson),
    Json = json:decode(JsonBin),
    ?assertEqual(<<"Dragon">>, maps:get(<<"name">>, Json)),
    ?assertEqual(75, maps:get(<<"hp">>, Json)),

    %% Cleanup
    file:delete(TmpSchema),
    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_mixed_update_test() ->
    %% Verify combined scalar + string shrink update is valid per flatc
    TmpSchema = "/tmp/flatbuferl_mixed_test.fbs",
    TmpBin = "/tmp/flatbuferl_mixed_test.bin",
    TmpJson = "/tmp/flatbuferl_mixed_test.json",

    SchemaStr = "table Monster { name: string; hp: int = 100; level: ubyte; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75, level => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Update both hp (scalar) and name (shrink)
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 999, name => <<"Orc">>})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format("flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
                        [TmpSchema, TmpBin]),
    Result = os:cmd(lists:flatten(Cmd)),
    ?assertEqual("", Result),

    %% Parse JSON and verify
    {ok, JsonBin} = file:read_file(TmpJson),
    Json = json:decode(JsonBin),
    ?assertEqual(<<"Orc">>, maps:get(<<"name">>, Json)),
    ?assertEqual(999, maps:get(<<"hp">>, Json)),
    ?assertEqual(5, maps:get(<<"level">>, Json)),

    %% Cleanup
    file:delete(TmpSchema),
    file:delete(TmpBin),
    file:delete(TmpJson).
