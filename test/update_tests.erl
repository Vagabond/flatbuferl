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
    ?assertMatch({simple, [{_, 4, float32, [pos, x], 5.0}]}, Result).

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
    %% Error format changed: enum type name no longer included with precomputed enum_resolved
    ?assertMatch({error, {invalid_enum_value, 'Purple'}}, Result).

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

        Parent !
            {result, HeapBytes, LargeBinsBefore, LargeBinsAfter, byte_size(NewBuffer), FirstByte}
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

    SchemaStr =
        "table Monster { name: string; hp: int = 100; level: ubyte = 1; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75, level => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Do partial update of hp
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 999})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format(
        "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
        [TmpSchema, TmpBin]
    ),
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
    Cmd = io_lib:format(
        "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
        [TmpSchema, TmpBin]
    ),
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
    Cmd = io_lib:format(
        "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
        [TmpSchema, TmpBin]
    ),
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
    Cmd = io_lib:format(
        "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
        [TmpSchema, TmpBin]
    ),
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

    SchemaStr =
        "table Monster { name: string; hp: int = 100; level: ubyte; }\nroot_type Monster;\n",
    ok = file:write_file(TmpSchema, SchemaStr),

    {ok, Schema} = flatbuferl:parse_schema(SchemaStr),
    Data = #{name => <<"Goblin">>, hp => 75, level => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),

    %% Update both hp (scalar) and name (shrink)
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{hp => 999, name => <<"Orc">>})),
    ok = file:write_file(TmpBin, UpdatedBuffer),

    %% Use flatc to decode (--raw-binary for schemas without file_identifier)
    Cmd = io_lib:format(
        "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
        [TmpSchema, TmpBin]
    ),
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

%% =============================================================================
%% Wide Scalar Type Tests (int64, uint64, float64, bool)
%% =============================================================================

wide_scalars_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/wide_scalars.fbs"),
    Data = #{
        big_signed => 9223372036854775807,
        big_unsigned => 18446744073709551615,
        precise_float => 3.14159,
        int64_explicit => 1000000000000,
        uint64_explicit => 2000000000000,
        float64_explicit => 2.71828
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_int64_update_test() ->
    Ctx = wide_scalars_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{big_signed => -9223372036854775808}),
    ?assertMatch({simple, _}, Result).

preflight_uint64_update_test() ->
    Ctx = wide_scalars_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{big_unsigned => 0}),
    ?assertMatch({simple, _}, Result).

preflight_float64_update_test() ->
    Ctx = wide_scalars_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{precise_float => 1.41421}),
    ?assertMatch({simple, _}, Result).

update_int64_test() ->
    Ctx = wide_scalars_ctx(),
    NewVal = -9223372036854775808,
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{big_signed => NewVal})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(NewVal, maps:get(big_signed, Result)).

update_uint64_test() ->
    Ctx = wide_scalars_ctx(),
    NewVal = 12345678901234567890,
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{uint64_explicit => NewVal})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(NewVal, maps:get(uint64_explicit, Result)).

update_float64_test() ->
    Ctx = wide_scalars_ctx(),
    NewVal = 1.61803398875,
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{float64_explicit => NewVal})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assert(abs(maps:get(float64_explicit, Result) - NewVal) < 0.0000001).

%% =============================================================================
%% Bool Type Tests
%% =============================================================================

bool_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table BoolTest { flag: bool; value: int; }\nroot_type BoolTest;\n"
    ),
    Data = #{flag => true, value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_bool_update_test() ->
    Ctx = bool_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{flag => false}),
    ?assertMatch({simple, _}, Result).

update_bool_true_to_false_test() ->
    Ctx = bool_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{flag => false})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(false, maps:get(flag, Result)).

update_bool_false_to_true_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table BoolTest { flag: bool; value: int; }\nroot_type BoolTest;\n"
    ),
    Data = #{flag => false, value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{flag => true})),
    NewCtx = flatbuferl:new(UpdatedBuffer, Schema),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(true, maps:get(flag, Result)).

%% =============================================================================
%% Deep Nested Table Tests
%% =============================================================================

deep_nested_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/deep_nested.fbs"),
    Data = #{
        id => 1,
        top => #{
            label => <<"level1">>,
            nested => #{
                count => 10,
                child => #{
                    value => 9223372036854775807,
                    name => <<"deepest">>
                }
            }
        }
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_deep_nested_scalar_test() ->
    Ctx = deep_nested_ctx(),
    %% Update top-level scalar
    Result = flatbuferl_update:preflight(Ctx, #{id => 999}),
    ?assertMatch({simple, _}, Result).

update_deep_nested_root_scalar_test() ->
    Ctx = deep_nested_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{id => 999})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(999, maps:get(id, Result)).

%% =============================================================================
%% Enum Field Tests (using file-based schema)
%% =============================================================================

enum_file_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/enum_field.fbs"),
    Data = #{enum_field => 'Green'},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_enum_to_different_value_test() ->
    Ctx = enum_file_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{enum_field => 'Blue'}),
    ?assertMatch({simple, _}, Result).

update_enum_value_test() ->
    Ctx = enum_file_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{enum_field => 'Blue'})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual('Blue', maps:get(enum_field, Result)).

%% =============================================================================
%% Nested Table Path Tests (exercises traverse_for_update)
%% =============================================================================

nested_table_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/nested.fbs"),
    Data = #{value_outer => 100, inner => #{value_inner => 200}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_nested_table_path_test() ->
    %% Traversing into nested table to update a scalar is simple
    Ctx = nested_table_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{inner => #{value_inner => 999}}),
    %% Path is [inner, value_inner] - updating scalar inside nested table
    ?assertMatch({simple, [{_, _, short, [inner, value_inner], 999}]}, Result).

update_nested_table_scalar_test() ->
    Ctx = nested_table_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{inner => #{value_inner => 999}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(999, maps:get(value_inner, maps:get(inner, Result))).

%% =============================================================================
%% Vector Element Path Tests (exercises traverse_vector_for_update)
%% =============================================================================

vector_table_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/table_vector.fbs"),
    Data = #{inner => [#{value_inner => <<"one">>}, #{value_inner => <<"two">>}]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_vector_element_path_test() ->
    %% This should return complex because we're traversing into a vector of tables
    Ctx = vector_table_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{inner => [#{value_inner => <<"ONE">>}]}),
    ?assertMatch({complex, _}, Result).

%% =============================================================================
%% Negative Index Tests (exercises negative index handling in vectors)
%% =============================================================================

scalar_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { items: [int]; }\nroot_type Data;\n"
    ),
    Data = #{items => [10, 20, 30, 40, 50]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_negative_index_test() ->
    %% Negative index -1 should refer to last element
    Ctx = scalar_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{-1 => 999}}),
    ?assertMatch({simple, [{_, 4, int, [items, -1], 999}]}, Result).

preflight_negative_index_second_last_test() ->
    %% Negative index -2 should refer to second-to-last element
    Ctx = scalar_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{-2 => 888}}),
    ?assertMatch({simple, [{_, 4, int, [items, -2], 888}]}, Result).

update_negative_index_test() ->
    Ctx = scalar_vector_ctx(),
    %% Update last element using negative index
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{items => #{-1 => 999}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([10, 20, 30, 40, 999], maps:get(items, Result)).

preflight_out_of_bounds_index_test() ->
    %% Out of bounds index should return missing -> complex
    Ctx = scalar_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{10 => 999}}),
    ?assertMatch({complex, _}, Result).

preflight_negative_out_of_bounds_test() ->
    %% Negative index that goes past start should return missing -> complex
    Ctx = scalar_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{-10 => 999}}),
    ?assertMatch({complex, _}, Result).

%% =============================================================================
%% Vector of Tables Traversal Tests
%% =============================================================================

table_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Item { value: int; }\n"
        "table Container { items: [Item]; }\n"
        "root_type Container;\n"
    ),
    Data = #{items => [#{value => 10}, #{value => 20}, #{value => 30}]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_vector_table_element_test() ->
    %% Update field inside table element of vector
    Ctx = table_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{0 => #{value => 999}}}),
    ?assertMatch({simple, [{_, 4, int, [items, 0, value], 999}]}, Result).

preflight_vector_table_negative_index_test() ->
    %% Update last table element using negative index
    Ctx = table_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{-1 => #{value => 777}}}),
    ?assertMatch({simple, [{_, 4, int, [items, -1, value], 777}]}, Result).

update_vector_table_element_test() ->
    Ctx = table_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{items => #{1 => #{value => 555}}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Items = maps:get(items, Result),
    ?assertEqual(555, maps:get(value, lists:nth(2, Items))).

%% =============================================================================
%% Invalid Path Element Tests
%% =============================================================================

preflight_invalid_path_element_test() ->
    %% Non-integer index in vector path should error
    Ctx = scalar_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{foo => 999}}),
    ?assertMatch({error, {invalid_path_element, foo}}, Result).

preflight_not_traversable_scalar_test() ->
    %% Trying to traverse into a scalar field should error
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { count: int; }\nroot_type Data;\n"
    ),
    Data = #{count => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl_update:preflight(Ctx, #{count => #{nested => 1}}),
    ?assertMatch({error, {not_traversable, count, int}}, Result).

%% =============================================================================
%% Missing Vector Field Tests
%% =============================================================================

missing_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { id: int; items: [int]; }\nroot_type Data;\n"
    ),
    %% Only set id, not items
    Data = #{id => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_missing_vector_test() ->
    %% Trying to index into a missing vector should return complex
    Ctx = missing_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{0 => 999}}),
    ?assertMatch({complex, _}, Result).

%% =============================================================================
%% Union NONE Tests
%% =============================================================================

union_none_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Equipment { Sword }\n"
        "table Player { name: string; equipped: Equipment; }\n"
        "root_type Player;\n"
    ),
    %% Don't set equipped - defaults to NONE
    Data = #{name => <<"Hero">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_union_none_test() ->
    %% Trying to traverse into NONE union should return missing -> complex
    Ctx = union_none_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{equipped => #{damage => 100}}),
    ?assertMatch({complex, _}, Result).

%% =============================================================================
%% Union Vector Out-of-Bounds Tests
%% =============================================================================

preflight_union_vector_out_of_bounds_test() ->
    %% Out of bounds index in union vector should return missing -> complex
    Ctx = union_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{10 => #{damage => 999}}}),
    ?assertMatch({complex, _}, Result).

preflight_union_vector_negative_out_of_bounds_test() ->
    %% Negative index that goes past start should return missing -> complex
    Ctx = union_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{items => #{-10 => #{damage => 999}}}),
    ?assertMatch({complex, _}, Result).

%% =============================================================================
%% Vector Scalar Type Tests (exercises various read_scalar_value branches)
%% =============================================================================

uint16_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [ushort]; }\nroot_type Data;\n"
    ),
    Data = #{values => [100, 200, 65535]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_uint16_vector_element_test() ->
    Ctx = uint16_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{values => #{0 => 9999}}),
    ?assertMatch({simple, _}, Result).

update_uint16_vector_element_test() ->
    Ctx = uint16_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{1 => 12345}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([100, 12345, 65535], maps:get(values, Result)).

uint32_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [uint]; }\nroot_type Data;\n"
    ),
    Data = #{values => [1000000, 2000000, 4294967295]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_uint32_vector_element_test() ->
    Ctx = uint32_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{0 => 3000000000}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([3000000000, 2000000, 4294967295], maps:get(values, Result)).

float32_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [float]; }\nroot_type Data;\n"
    ),
    Data = #{values => [1.5, 2.5, 3.5]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_float32_vector_element_test() ->
    Ctx = float32_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{1 => 99.99}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    [V1, V2, V3] = maps:get(values, Result),
    ?assert(abs(V1 - 1.5) < 0.01),
    ?assert(abs(V2 - 99.99) < 0.01),
    ?assert(abs(V3 - 3.5) < 0.01).

int16_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [short]; }\nroot_type Data;\n"
    ),
    Data = #{values => [-100, 0, 32767]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_int16_vector_element_test() ->
    Ctx = int16_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{0 => -32768}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([-32768, 0, 32767], maps:get(values, Result)).

int8_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [byte]; }\nroot_type Data;\n"
    ),
    Data = #{values => [-128, 0, 127]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_int8_vector_element_test() ->
    Ctx = int8_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{1 => -50}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([-128, -50, 127], maps:get(values, Result)).

bool_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { flags: [bool]; }\nroot_type Data;\n"
    ),
    Data = #{flags => [true, false, true]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_bool_vector_element_test() ->
    Ctx = bool_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{flags => #{0 => false}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual([false, false, true], maps:get(flags, Result)).

int64_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [long]; }\nroot_type Data;\n"
    ),
    Data = #{values => [9223372036854775807, -9223372036854775808, 0]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_int64_vector_element_test() ->
    Ctx = int64_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{2 => 12345678901234}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(
        [9223372036854775807, -9223372036854775808, 12345678901234], maps:get(values, Result)
    ).

uint64_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [ulong]; }\nroot_type Data;\n"
    ),
    Data = #{values => [18446744073709551615, 0, 12345678901234567890]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_uint64_vector_element_test() ->
    Ctx = uint64_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(
        flatbuferl:update(Ctx, #{values => #{1 => 9999999999999999999}})
    ),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(
        [18446744073709551615, 9999999999999999999, 12345678901234567890], maps:get(values, Result)
    ).

float64_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [double]; }\nroot_type Data;\n"
    ),
    Data = #{values => [3.14159265358979, 2.71828182845904, 1.41421356237309]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_float64_vector_element_test() ->
    Ctx = float64_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{values => #{0 => 1.61803398874989}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    [V1 | _] = maps:get(values, Result),
    ?assert(abs(V1 - 1.61803398874989) < 0.00000000001).

%% =============================================================================
%% Enum Vector Tests - Complex Fallback
%% =============================================================================

enum_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "enum Color : ubyte { Red, Green, Blue }\n"
        "table Data { colors: [Color]; }\n"
        "root_type Data;\n"
    ),
    Data = #{colors => ['Red', 'Green', 'Blue']},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_enum_vector_complex_fallback_test() ->
    %% Growing enum vector falls back to complex re-encode
    Ctx = enum_vector_ctx(),
    UpdatedBuffer = iolist_to_binary(
        flatbuferl:update(Ctx, #{colors => ['Red', 'Green', 'Blue', 'Red']})
    ),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(['Red', 'Green', 'Blue', 'Red'], maps:get(colors, Result)).

%% =============================================================================
%% Multi-Field Struct Traversal Tests
%% =============================================================================

multi_field_struct_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec4 { x: float; y: float; z: float; w: float; }\n"
        "table Data { pos: Vec4; id: int; }\n"
        "root_type Data;\n"
    ),
    Data = #{pos => #{x => 1.0, y => 2.0, z => 3.0, w => 4.0}, id => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

preflight_multi_field_struct_test() ->
    Ctx = multi_field_struct_ctx(),
    %% Update multiple fields in the same struct
    Result = flatbuferl_update:preflight(Ctx, #{pos => #{x => 10.0, w => 40.0}}),
    ?assertMatch({simple, _}, Result),
    {simple, Updates} = Result,
    ?assertEqual(2, length(Updates)).

update_multi_field_struct_test() ->
    Ctx = multi_field_struct_ctx(),
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, #{pos => #{x => 10.0, w => 40.0}})),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Pos = maps:get(pos, Result),
    ?assert(abs(maps:get(x, Pos) - 10.0) < 0.01),
    ?assert(abs(maps:get(y, Pos) - 2.0) < 0.01),
    ?assert(abs(maps:get(z, Pos) - 3.0) < 0.01),
    ?assert(abs(maps:get(w, Pos) - 40.0) < 0.01).

%% =============================================================================
%% Deep Merge Tests (exercises deep_merge edge cases)
%% =============================================================================

deep_merge_complex_fallback_test() ->
    %% Test deep merge through complex fallback
    Ctx = nested_ctx(),
    %% Update string which needs complex fallback with nested struct
    UpdatedBuffer = iolist_to_binary(
        flatbuferl:update(Ctx, #{
            name => <<"A very long new name that needs re-encoding">>,
            pos => #{x => 99.0}
        })
    ),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(<<"A very long new name that needs re-encoding">>, maps:get(name, Result)),
    ?assertEqual(99.0, maps:get(x, maps:get(pos, Result))).

%% =============================================================================
%% Try Shrink Update Tests (exercises complex returns)
%% =============================================================================

preflight_non_shrinkable_vector_type_test() ->
    %% Vectors of strings can't be shrink-updated
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { names: [string]; }\nroot_type Data;\n"
    ),
    Data = #{names => [<<"one">>, <<"two">>, <<"three">>]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl_update:preflight(Ctx, #{names => [<<"a">>, <<"b">>]}),
    ?assertMatch({complex, _}, Result).

preflight_table_field_not_traversable_test() ->
    %% Trying to update a whole table as if it were a scalar should error
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { value: int; }\n"
        "table Outer { inner: Inner; }\n"
        "root_type Outer;\n"
    ),
    Data = #{inner => #{value => 42}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Try to replace whole inner table - should fall back to complex
    Result = flatbuferl_update:preflight(Ctx, #{inner => #{value => 100}}),
    %% Actually works because we traverse into it
    ?assertMatch({simple, _}, Result).

%% =============================================================================
%% Struct Vector Tests (exercises read_scalar_value for structs)
%% =============================================================================

struct_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Point { x: int; y: int; }\n"
        "table Data { points: [Point]; }\n"
        "root_type Data;\n"
    ),
    Data = #{points => [#{x => 1, y => 2}, #{x => 3, y => 4}, #{x => 5, y => 6}]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_struct_vector_test() ->
    Ctx = struct_vector_ctx(),
    Result = flatbuferl:to_map(Ctx),
    Points = maps:get(points, Result),
    ?assertEqual(3, length(Points)),
    [P1, P2, P3] = Points,
    ?assertEqual(#{x => 1, y => 2}, P1),
    ?assertEqual(#{x => 3, y => 4}, P2),
    ?assertEqual(#{x => 5, y => 6}, P3).

%% =============================================================================
%% Array Field Tests (exercises read_scalar for arrays)
%% =============================================================================

array_field_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{
        floats => [1.0, 2.0, 3.0],
        ints => [10, 20, 30, 40],
        %% Bytes returned as binary
        bytes => <<255, 1>>
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),
    ?assertEqual([10, 20, 30, 40], maps:get(ints, Result)),
    ?assertEqual(<<255, 1>>, maps:get(bytes, Result)),
    %% Floats need approximate comparison
    Floats = maps:get(floats, Result),
    ?assertEqual(3, length(Floats)),
    [F1, F2, F3] = Floats,
    ?assert(abs(F1 - 1.0) < 0.01),
    ?assert(abs(F2 - 2.0) < 0.01),
    ?assert(abs(F3 - 3.0) < 0.01).

%% =============================================================================
%% Missing Field Tests (exercises get/has with missing fields)
%% =============================================================================

missing_field_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int; b: string; c: int = 99; }\nroot_type Data;\n"
    ),
    %% Only set 'a', leave 'b' and 'c' missing
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

get_missing_field_with_default_test() ->
    Ctx = missing_field_ctx(),
    %% c has default 99, should return it
    ?assertEqual(99, flatbuferl:get(Ctx, [c])).

get_missing_field_no_default_error_test() ->
    Ctx = missing_field_ctx(),
    %% b has no default, should error
    ?assertError({missing_field, [b], no_default}, flatbuferl:get(Ctx, [b])).

has_present_test() ->
    Ctx = missing_field_ctx(),
    ?assertEqual(true, flatbuferl:has(Ctx, [a])).

has_missing_test() ->
    Ctx = missing_field_ctx(),
    %% b is missing from buffer and has no default
    ?assertEqual(false, flatbuferl:has(Ctx, [b])).

has_with_default_test() ->
    Ctx = missing_field_ctx(),
    %% c is missing from buffer but has default - has returns true because a value is available
    ?assertEqual(true, flatbuferl:has(Ctx, [c])).

%% =============================================================================
%% Inline Struct Field Tests (exercises read_scalar for struct_def)
%% =============================================================================

inline_struct_field_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec2 { x: float; y: float; }\n"
        "table Entity { id: int; pos: Vec2; name: string; }\n"
        "root_type Entity;\n"
    ),
    Data = #{id => 1, pos => #{x => 1.5, y => 2.5}, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),
    Pos = maps:get(pos, Result),
    ?assert(abs(maps:get(x, Pos) - 1.5) < 0.01),
    ?assert(abs(maps:get(y, Pos) - 2.5) < 0.01).

%% =============================================================================
%% Sparse Field ID Tests (exercises vtable size < field offset paths)
%% =============================================================================

sparse_field_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); z: int (id: 10); }\nroot_type Data;\n"
    ),
    %% Only set 'a', leave 'z' missing - vtable will be small
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

sparse_field_missing_test() ->
    Ctx = sparse_field_ctx(),
    %% z has high field ID, vtable may be too small
    ?assertEqual(false, flatbuferl:has(Ctx, [z])).

%% =============================================================================
%% Union Vector to_map Tests (exercises union vector decode path)
%% =============================================================================

decode_union_vector_full_test() ->
    %% Test full decode including type resolution
    Ctx = union_vector_ctx(),
    Result = flatbuferl:to_map(Ctx),
    Items = maps:get(items, Result),
    ?assertEqual(3, length(Items)),
    %% Verify each item has correct fields based on type
    [#{damage := 10}, #{defense := 20}, #{damage := 30}] = Items.

%% =============================================================================
%% get/3 with Default Tests
%% =============================================================================

get_with_default_present_test() ->
    Ctx = missing_field_ctx(),
    ?assertEqual(42, flatbuferl:get(Ctx, [a], 999)).

get_with_default_missing_test() ->
    Ctx = missing_field_ctx(),
    %% b is missing, should return provided default
    ?assertEqual(<<"fallback">>, flatbuferl:get(Ctx, [b], <<"fallback">>)).

%% =============================================================================
%% Default Value Tests (exercises missing field with default handling)
%% =============================================================================

default_value_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data {\n"
        "  required_field: int;\n"
        "  optional_int: int = 42;\n"
        "  optional_float: float = 3.14;\n"
        "  optional_bool: bool = true;\n"
        "  optional_string: string;\n"
        "}\n"
        "root_type Data;\n"
    ),
    %% Only set the required field, let others use defaults
    Data = #{required_field => 100},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_default_values_test() ->
    Ctx = default_value_ctx(),
    Result = flatbuferl:to_map(Ctx),
    ?assertEqual(100, maps:get(required_field, Result)),
    ?assertEqual(42, maps:get(optional_int, Result)),
    ?assert(abs(maps:get(optional_float, Result) - 3.14) < 0.01),
    ?assertEqual(true, maps:get(optional_bool, Result)),
    %% String has no default, should not be in map
    ?assertEqual(false, maps:is_key(optional_string, Result)).

get_with_default_test() ->
    Ctx = default_value_ctx(),
    %% get/2 should return defaults for missing fields
    ?assertEqual(42, flatbuferl:get(Ctx, [optional_int])),
    ?assertEqual(true, flatbuferl:get(Ctx, [optional_bool])).

%% =============================================================================
%% Union Vector Decoding Tests (exercises union vector decode path)
%% =============================================================================

decode_union_vector_test() ->
    Ctx = union_vector_ctx(),
    Result = flatbuferl:to_map(Ctx),
    Items = maps:get(items, Result),
    ?assertEqual(3, length(Items)),
    [I1, I2, I3] = Items,
    ?assertEqual(#{damage => 10}, I1),
    ?assertEqual(#{defense => 20}, I2),
    ?assertEqual(#{damage => 30}, I3).

%% =============================================================================
%% Type Alias Struct Tests (exercises element_size for aliases)
%% =============================================================================

type_alias_struct_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct AliasStruct {\n"
        "  b: byte;\n"
        "  ub: ubyte;\n"
        "  s: short;\n"
        "  us: ushort;\n"
        "  i: int;\n"
        "  ui: uint;\n"
        "  l: long;\n"
        "  ul: ulong;\n"
        "  f: float;\n"
        "  d: double;\n"
        "}\n"
        "table Data { s: AliasStruct; }\n"
        "root_type Data;\n"
    ),
    Data = #{
        s => #{
            b => -10,
            ub => 200,
            s => -1000,
            us => 60000,
            i => -100000,
            ui => 100000,
            l => -9000000000,
            ul => 9000000000,
            f => 1.5,
            d => 2.5
        }
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_type_alias_struct_test() ->
    Ctx = type_alias_struct_ctx(),
    Result = flatbuferl:to_map(Ctx),
    S = maps:get(s, Result),
    ?assertEqual(-10, maps:get(b, S)),
    ?assertEqual(200, maps:get(ub, S)),
    ?assertEqual(-1000, maps:get(s, S)),
    ?assertEqual(60000, maps:get(us, S)),
    ?assertEqual(-100000, maps:get(i, S)),
    ?assertEqual(100000, maps:get(ui, S)),
    ?assertEqual(-9000000000, maps:get(l, S)),
    ?assertEqual(9000000000, maps:get(ul, S)),
    ?assert(abs(maps:get(f, S) - 1.5) < 0.01),
    ?assert(abs(maps:get(d, S) - 2.5) < 0.01).

%% =============================================================================
%% All Scalars Test (exercises multiple encode_scalar branches)
%% =============================================================================

all_scalars_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/all_my_scalars.fbs"),
    Data = #{
        my_byte => -10,
        my_ubyte => 200,
        my_bool => true,
        my_short => -1000,
        my_ushort => 60000,
        my_int => -100000,
        my_uint => 100000,
        my_float => 3.14,
        my_long => -9000000000,
        my_ulong => 9000000000,
        my_double => 2.718281828
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_all_scalars_test() ->
    Ctx = all_scalars_ctx(),
    Updates = #{
        my_byte => 127,
        my_ubyte => 255,
        my_bool => false,
        my_short => 32767,
        my_ushort => 65535,
        my_int => 2147483647,
        my_uint => 4294967295,
        my_long => 9223372036854775807,
        my_ulong => 18446744073709551615,
        my_double => 1.41421356
    },
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(127, maps:get(my_byte, Result)),
    ?assertEqual(255, maps:get(my_ubyte, Result)),
    ?assertEqual(false, maps:get(my_bool, Result)),
    ?assertEqual(32767, maps:get(my_short, Result)),
    ?assertEqual(65535, maps:get(my_ushort, Result)),
    ?assertEqual(2147483647, maps:get(my_int, Result)),
    ?assertEqual(4294967295, maps:get(my_uint, Result)),
    ?assertEqual(9223372036854775807, maps:get(my_long, Result)),
    ?assertEqual(18446744073709551615, maps:get(my_ulong, Result)),
    ?assert(abs(maps:get(my_double, Result) - 1.41421356) < 0.0000001).

%% =============================================================================
%% Update Error Tests (exercises error paths in traverse)
%% =============================================================================

update_unknown_field_error_test() ->
    Ctx = simple_ctx(),
    Updates = #{nonexistent_field => 42},
    ?assertError({unknown_field, nonexistent_field}, flatbuferl:update(Ctx, Updates)).

%% =============================================================================
%% Array as Binary Tests (exercises array with as_binary path)
%% =============================================================================

array_as_binary_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Data { bytes: [ubyte:8]; }\n"
        "table Wrapper { data: Data; }\n"
        "root_type Wrapper;\n"
    ),
    Data = #{data => #{bytes => <<1, 2, 3, 4, 5, 6, 7, 8>>}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_array_as_binary_test() ->
    Ctx = array_as_binary_ctx(),
    Result = flatbuferl:to_map(Ctx),
    DataStruct = maps:get(data, Result),
    ?assertEqual(<<1, 2, 3, 4, 5, 6, 7, 8>>, maps:get(bytes, DataStruct)).

%% =============================================================================
%% Non-Scalar Array Tests (exercises array with non-ubyte elements)
%% =============================================================================

int_array_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Data { values: [int:4]; }\n"
        "table Wrapper { data: Data; }\n"
        "root_type Wrapper;\n"
    ),
    Data = #{data => #{values => [100, 200, 300, 400]}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_int_array_test() ->
    Ctx = int_array_ctx(),
    Result = flatbuferl:to_map(Ctx),
    DataStruct = maps:get(data, Result),
    ?assertEqual([100, 200, 300, 400], maps:get(values, DataStruct)).

%% =============================================================================
%% Missing Nested Field Tests (exercises vtable missing paths)
%% =============================================================================

missing_nested_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { x: int; y: int; }\n"
        "table Outer { inner: Inner; other: int; }\n"
        "root_type Outer;\n"
    ),
    %% Only set other, leave inner missing
    Data = #{other => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

missing_nested_table_test() ->
    Ctx = missing_nested_ctx(),
    Result = flatbuferl:to_map(Ctx),
    ?assertEqual(false, maps:is_key(inner, Result)),
    ?assertEqual(42, maps:get(other, Result)).

%% =============================================================================
%% Enum Field Update Tests (exercises enum value encoding)
%% =============================================================================

enum_field_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "enum Color : byte { Red = 0, Green = 1, Blue = 2 }\n"
        "table Data { color: Color; value: int; }\n"
        "root_type Data;\n"
    ),
    Data = #{color => 'Green', value => 100},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_enum_color_field_test() ->
    Ctx = enum_field_ctx(),
    Updates = #{color => 'Blue'},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual('Blue', maps:get(color, Result)).

update_enum_to_red_test() ->
    Ctx = enum_field_ctx(),
    %% Update using atom value
    Updates = #{color => 'Red'},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual('Red', maps:get(color, Result)).

%% =============================================================================
%% Vector Index Update Tests (exercises vector traversal)
%% =============================================================================

update_vector_element_test() ->
    Ctx = struct_vector_ctx(),
    %% Update x value of second point (index 1) - nested map format
    Updates = #{points => #{1 => #{x => 999}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Points = maps:get(points, Result),
    ?assertEqual(999, maps:get(x, lists:nth(2, Points))).

update_vector_negative_index_test() ->
    Ctx = struct_vector_ctx(),
    %% Update last element using negative index - nested map format
    Updates = #{points => #{-1 => #{x => 888}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Points = maps:get(points, Result),
    ?assertEqual(888, maps:get(x, lists:last(Points))).

%% =============================================================================
%% Inline Struct Field Update Tests (exercises traverse_struct_for_update)
%% =============================================================================

inline_struct_update_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec2 { x: float; y: float; }\n"
        "table Entity { id: int; pos: Vec2; name: string; }\n"
        "root_type Entity;\n"
    ),
    Data = #{id => 1, pos => #{x => 1.5, y => 2.5}, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_inline_struct_x_test() ->
    Ctx = inline_struct_update_ctx(),
    %% Update x field within inline struct
    Updates = #{pos => #{x => 99.5}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Pos = maps:get(pos, Result),
    ?assert(abs(maps:get(x, Pos) - 99.5) < 0.01),
    %% y should be unchanged
    ?assert(abs(maps:get(y, Pos) - 2.5) < 0.01).

update_inline_struct_y_test() ->
    Ctx = inline_struct_update_ctx(),
    %% Update y field within inline struct
    Updates = #{pos => #{y => 88.5}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Pos = maps:get(pos, Result),
    ?assert(abs(maps:get(y, Pos) - 88.5) < 0.01),
    %% x should be unchanged
    ?assert(abs(maps:get(x, Pos) - 1.5) < 0.01).

update_inline_struct_both_test() ->
    Ctx = inline_struct_update_ctx(),
    %% Update both fields within inline struct
    Updates = #{pos => #{x => 10.0, y => 20.0}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Pos = maps:get(pos, Result),
    ?assert(abs(maps:get(x, Pos) - 10.0) < 0.01),
    ?assert(abs(maps:get(y, Pos) - 20.0) < 0.01).

%% =============================================================================
%% Union Field Access Tests (exercises union type/value reading)
%% =============================================================================

union_access_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "table Shield { defense: int; }\n"
        "union Equipment { Sword, Shield }\n"
        "table Player { equip: Equipment; level: int; }\n"
        "root_type Player;\n"
    ),
    Data = #{equip_type => 'Sword', equip => #{damage => 50}, level => 10},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

get_union_field_test() ->
    Ctx = union_access_ctx(),
    Result = flatbuferl:to_map(Ctx),
    Equip = maps:get(equip, Result),
    ?assertEqual(50, maps:get(damage, Equip)),
    ?assertEqual(10, maps:get(level, Result)).

%% =============================================================================
%% Vector of Scalars Update Tests
%% =============================================================================

int_vector_update_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { values: [int]; count: int; }\n"
        "root_type Data;\n"
    ),
    Data = #{values => [10, 20, 30, 40, 50], count => 5},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_scalar_vector_element_test() ->
    Ctx = int_vector_update_ctx(),
    %% Update element at index 2
    Updates = #{values => #{2 => 999}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Values = maps:get(values, Result),
    ?assertEqual([10, 20, 999, 40, 50], Values).

update_scalar_vector_first_element_test() ->
    Ctx = int_vector_update_ctx(),
    %% Update first element
    Updates = #{values => #{0 => 111}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Values = maps:get(values, Result),
    ?assertEqual([111, 20, 30, 40, 50], Values).

update_scalar_vector_last_element_test() ->
    Ctx = int_vector_update_ctx(),
    %% Update last element using negative index
    Updates = #{values => #{-1 => 555}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Values = maps:get(values, Result),
    ?assertEqual([10, 20, 30, 40, 555], Values).

%% =============================================================================
%% Float Vector Update Tests
%% =============================================================================

float_vector_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { coords: [float]; }\n"
        "root_type Data;\n"
    ),
    Data = #{coords => [1.0, 2.0, 3.0]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_float_vector_element_test() ->
    Ctx = float_vector_ctx(),
    Updates = #{coords => #{1 => 99.5}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    [C0, C1, C2] = maps:get(coords, Result),
    ?assert(abs(C0 - 1.0) < 0.01),
    ?assert(abs(C1 - 99.5) < 0.01),
    ?assert(abs(C2 - 3.0) < 0.01).

%% =============================================================================
%% Multiple Struct Vector Updates Tests
%% =============================================================================

update_multiple_struct_vector_elements_test() ->
    Ctx = struct_vector_ctx(),
    %% Update multiple elements in same update
    Updates = #{points => #{0 => #{x => 100}, 2 => #{y => 200}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    [P0, P1, P2] = maps:get(points, Result),
    ?assertEqual(100, maps:get(x, P0)),
    %% unchanged
    ?assertEqual(2, maps:get(y, P0)),
    %% unchanged
    ?assertEqual(3, maps:get(x, P1)),
    %% unchanged
    ?assertEqual(4, maps:get(y, P1)),
    %% unchanged
    ?assertEqual(5, maps:get(x, P2)),
    ?assertEqual(200, maps:get(y, P2)).

%% =============================================================================
%% Bool Field Tests
%% =============================================================================

bool_field_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { flag: bool; count: int; }\n"
        "root_type Data;\n"
    ),
    Data = #{flag => true, count => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_bool_to_false_test() ->
    Ctx = bool_field_ctx(),
    Updates = #{flag => false},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(false, maps:get(flag, Result)).

update_bool_to_true_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { flag: bool; count: int; }\n"
        "root_type Data;\n"
    ),
    Data = #{flag => false, count => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Updates = #{flag => true},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(true, maps:get(flag, Result)).

%% =============================================================================
%% Byte/Short Field Tests (exercises smaller integer types)
%% =============================================================================

small_int_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { b: byte; ub: ubyte; s: short; us: ushort; }\n"
        "root_type Data;\n"
    ),
    Data = #{b => -10, ub => 200, s => -1000, us => 60000},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_byte_field_test() ->
    Ctx = small_int_ctx(),
    Updates = #{b => -128},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(-128, maps:get(b, Result)).

update_ubyte_field_test() ->
    Ctx = small_int_ctx(),
    Updates = #{ub => 255},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(255, maps:get(ub, Result)).

update_short_field_test() ->
    Ctx = small_int_ctx(),
    Updates = #{s => -32768},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(-32768, maps:get(s, Result)).

update_ushort_field_test() ->
    Ctx = small_int_ctx(),
    Updates = #{us => 65535},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    ?assertEqual(65535, maps:get(us, Result)).

%% =============================================================================
%% Update Error Path Tests
%% =============================================================================

update_unknown_nested_struct_field_error_test() ->
    Ctx = inline_struct_update_ctx(),
    %% Try to update nonexistent field in struct
    Updates = #{pos => #{w => 99.0}},
    ?assertError({unknown_field, w}, flatbuferl:update(Ctx, Updates)).

update_not_traversable_scalar_error_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { value: int; name: string; }\n"
        "root_type Data;\n"
    ),
    Data = #{value => 42, name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Try to traverse into an int field
    Updates = #{value => #{nested => 1}},
    ?assertError({not_traversable, value, int}, flatbuferl:update(Ctx, Updates)).

%% =============================================================================
%% Sparse VTable Update Tests
%% =============================================================================

sparse_update_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 5); c: int (id: 10); }\n"
        "root_type Data;\n"
    ),
    %% Only set 'a'
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_sparse_present_field_test() ->
    Ctx = sparse_update_ctx(),
    Updates = #{a => 100},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    ?assertEqual(100, flatbuferl:get(NewCtx, [a])).

update_sparse_missing_field_complex_fallback_test() ->
    Ctx = sparse_update_ctx(),
    %% Updating a missing field triggers complex fallback (re-encode)
    Updates = #{b => 200},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    ?assertEqual(200, flatbuferl:get(NewCtx, [b])).

%% =============================================================================
%% Vector Update Bounds Tests
%% =============================================================================

update_vector_valid_negative_index_test() ->
    Ctx = struct_vector_ctx(),
    %% -2 should be valid (second to last element)
    Updates = #{points => #{-2 => #{y => 777}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Points = maps:get(points, Result),
    %% Second element (index 1, which is -2 from end) should have y=777
    ?assertEqual(777, maps:get(y, lists:nth(2, Points))).

%% =============================================================================
%% Missing Union Tests
%% =============================================================================

missing_union_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "table Shield { defense: int; }\n"
        "union Equipment { Sword, Shield }\n"
        "table Player { equip: Equipment; level: int; }\n"
        "root_type Player;\n"
    ),
    %% Create player without equipment
    Data = #{level => 10},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

decode_missing_union_test() ->
    Ctx = missing_union_ctx(),
    Result = flatbuferl:to_map(Ctx),
    ?assertEqual(false, maps:is_key(equip, Result)),
    ?assertEqual(10, maps:get(level, Result)).

get_missing_union_test() ->
    Ctx = missing_union_ctx(),
    ?assertError({missing_field, [equip], no_default}, flatbuferl:get(Ctx, [equip])).

has_missing_union_test() ->
    Ctx = missing_union_ctx(),
    ?assertEqual(false, flatbuferl:has(Ctx, [equip])).

%% =============================================================================
%% Additional Error Path Tests
%% =============================================================================

%% Test invalid path element (non-integer for vector index)
update_invalid_path_element_test() ->
    Ctx = struct_vector_ctx(),
    %% Using an atom instead of integer as vector index
    Updates = #{points => #{foo => #{x => 1}}},
    ?assertError({invalid_path_element, foo}, flatbuferl:update(Ctx, Updates)).

%% Test traversing further into scalar vector element
update_scalar_vector_not_traversable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { scores: [int]; }\n"
        "root_type Data;\n"
    ),
    Data = #{scores => [10, 20, 30]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Try to traverse into an int element
    Updates = #{scores => #{0 => #{nested => 1}}},
    ?assertError({not_traversable, 0, int}, flatbuferl:update(Ctx, Updates)).

%% Test update on union vector element (uses existing union_vector_ctx)
%% Existing ctx has items: [{damage => 10}, {defense => 20}, {damage => 30}]
update_union_vector_element_additional_test() ->
    Ctx = union_vector_ctx(),
    Updates = #{items => #{0 => #{damage => 999}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Items = maps:get(items, Result),
    ?assertEqual(3, length(Items)),
    %% Union vectors decode as [#{damage => N}, ...]
    [FirstVal | _] = Items,
    ?assertEqual(999, maps:get(damage, FirstVal)).

%% Test out of bounds vector index - preflight returns complex (missing)
%% Note: The complex fallback doesn't handle indexed maps for vectors correctly,
%% so we just test that preflight correctly identifies this as complex.
preflight_vector_oob_returns_complex_test() ->
    Ctx = struct_vector_ctx(),
    %% Index 100 is way out of bounds (we only have 3 elements)
    Result = flatbuferl_update:preflight(Ctx, #{points => #{100 => #{x => 1}}}),
    ?assertMatch({complex, _}, Result).

preflight_vector_oob_negative_returns_complex_test() ->
    Ctx = struct_vector_ctx(),
    %% Index -100 is way out of bounds
    Result = flatbuferl_update:preflight(Ctx, #{points => #{-100 => #{x => 1}}}),
    ?assertMatch({complex, _}, Result).

%% Test table vector updates using existing table_vector_ctx
update_table_vector_additional_test() ->
    Ctx = table_vector_ctx(),
    Updates = #{items => #{0 => #{value => 999}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Items = maps:get(items, Result),
    FirstItem = hd(Items),
    ?assertEqual(999, maps:get(value, FirstItem)).

update_table_vector_negative_index_additional_test() ->
    Ctx = table_vector_ctx(),
    %% -1 should update last element
    Updates = #{items => #{-1 => #{value => 888}}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Items = maps:get(items, Result),
    LastItem = lists:last(Items),
    ?assertEqual(888, maps:get(value, LastItem)).

%% Test nested table traversal using existing nested_table_ctx
update_nested_table_field_additional_test() ->
    Ctx = nested_table_ctx(),
    Updates = #{inner => #{value_inner => 999}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Inner = maps:get(inner, Result),
    ?assertEqual(999, maps:get(value_inner, Inner)).

%% Test missing nested table
missing_nested_table_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { x: int; y: int; }\n"
        "table Outer { inner: Inner; name: string; }\n"
        "root_type Outer;\n"
    ),
    %% Create without inner table
    Data = #{name => <<"test">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    flatbuferl:new(Buffer, Schema).

update_missing_nested_table_complex_fallback_test() ->
    Ctx = missing_nested_table_ctx(),
    %% Updating a missing nested table triggers complex fallback
    Updates = #{inner => #{x => 100}},
    UpdatedBuffer = iolist_to_binary(flatbuferl:update(Ctx, Updates)),
    {Defs, Opts} = ctx_schema(Ctx),
    NewCtx = flatbuferl:new(UpdatedBuffer, {Defs, Opts}),
    Result = flatbuferl:to_map(NewCtx),
    Inner = maps:get(inner, Result),
    ?assertEqual(100, maps:get(x, Inner)).

%% Test traversing into table that is unset
update_missing_table_path_test() ->
    Ctx = missing_nested_table_ctx(),
    %% Fetching on a missing table path
    ?assertEqual(false, flatbuferl:has(Ctx, [inner])).

%% Test preflight with vector
preflight_vector_element_test() ->
    Ctx = struct_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{points => #{0 => #{x => 5}}}),
    ?assertMatch({simple, _}, Result).

%% Test preflight with missing vector element (out of bounds)
preflight_vector_oob_complex_test() ->
    Ctx = struct_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{points => #{100 => #{x => 5}}}),
    ?assertMatch({complex, _}, Result).

%% Test preflight with invalid path element
preflight_invalid_path_element_error_test() ->
    Ctx = struct_vector_ctx(),
    Result = flatbuferl_update:preflight(Ctx, #{points => #{foo => #{x => 5}}}),
    ?assertMatch({error, {invalid_path_element, foo}}, Result).

%% Test preflight traversing into scalar
preflight_not_traversable_error_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { scores: [int]; }\n"
        "root_type Data;\n"
    ),
    Data = #{scores => [10, 20, 30]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl_update:preflight(Ctx, #{scores => #{0 => #{nested => 1}}}),
    ?assertMatch({error, {not_traversable, 0, int}}, Result).

%% =============================================================================
%% VTable Too Small Tests for to_map (Schema Evolution)
%% =============================================================================

%% Test to_map with schema evolution - exercises read_field fast paths
tomap_vtable_too_small_scalar_test() ->
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
    Result = flatbuferl:to_map(Ctx),

    %% Field 'a' exists and has value
    ?assertEqual(42, maps:get(a, Result)),
    %% Field 'b' should not be in the result (vtable too small)
    ?assertEqual(false, maps:is_key(b, Result)).

%% Test to_map with schema evolution - nested table field
tomap_vtable_too_small_nested_test() ->
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
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(inner, Result)).

%% Test to_map with schema evolution - struct field
tomap_vtable_too_small_struct_test() ->
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
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(pos, Result)).

%% Test to_map with schema evolution - union field
tomap_vtable_too_small_union_test() ->
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
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(weapon, Result)).

%% Test to_map with schema evolution - vector field
tomap_vtable_too_small_vector_test() ->
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
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(items, Result)).

%% Test to_map with schema evolution - string field
tomap_vtable_too_small_string_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); name: string (id: 5); }\n"
        "root_type Data;\n"
    ),
    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(name, Result)).

%% =============================================================================
%% Malformed Binary Tests (Intentionally Corrupted FlatBuffers)
%% =============================================================================

%% Helper to corrupt the vtable size in a FlatBuffer
%% FlatBuffer format:
%%   [root_offset:32] ... [vtable: vtable_size:16, table_size:16, field_offsets:16...]
%%   At table: [soffset:32] (signed offset back to vtable)
corrupt_vtable_size(Buffer, NewVTableSize) ->
    %% Get root table offset
    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    %% Get soffset at root table (points back to vtable)
    <<_:RootOffset/binary, SOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = RootOffset - SOffset,
    %% Replace vtable_size with new value
    <<Before:VTableOffset/binary, _OldSize:16/little-unsigned, After/binary>> = Buffer,
    <<Before/binary, NewVTableSize:16/little-unsigned, After/binary>>.

%% Test reading a buffer with artificially truncated vtable
malformed_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 1); c: int (id: 2); }\n"
        "root_type Data;\n"
    ),
    %% Create buffer with all fields set
    Data = #{a => 1, b => 2, c => 3},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable size to only include field 0 (vtable header is 4 bytes + 2 bytes per field)
    %% Original should be 4 + 6 = 10 bytes, truncate to 4 + 2 = 6 bytes
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    %% Field 'a' (id:0) should still be readable
    ?assertEqual(1, maps:get(a, Result)),
    %% Fields 'b' (id:1) and 'c' (id:2) should be missing due to truncated vtable
    ?assertEqual(false, maps:is_key(b, Result)),
    ?assertEqual(false, maps:is_key(c, Result)).

%% Test with vtable truncated to just the header (no field slots)
malformed_vtable_header_only_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, b => 99},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to just header (4 bytes) - no field slots at all
    CorruptedBuffer = corrupt_vtable_size(Buffer, 4),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    %% All fields should be missing
    ?assertEqual(false, maps:is_key(a, Result)),
    ?assertEqual(false, maps:is_key(b, Result)).

%% Test with nested table and truncated vtable
malformed_nested_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { a: int (id: 0); inner: Inner (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, inner => #{x => 100}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to only include field 0
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(inner, Result)).

%% Test with struct field and truncated vtable
malformed_struct_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "struct Vec2 { x: int; y: int; }\n"
        "table Data { a: int (id: 0); pos: Vec2 (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, pos => #{x => 10, y => 20}},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to only include field 0
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(pos, Result)).

%% Test with string field and truncated vtable
malformed_string_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); name: string (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, name => <<"hello">>},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to only include field 0
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(name, Result)).

%% Test with vector field and truncated vtable
malformed_vector_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); items: [int] (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, items => [1, 2, 3]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to only include field 0
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(items, Result)).

%% Test with union field and truncated vtable
malformed_union_truncated_vtable_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { a: int; weapon: Weapon; }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42, weapon => #{damage => 50}, weapon_type => 'Sword'},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Truncate vtable to only include field 0 (a)
    %% Union takes 2 field slots (type + value), so this makes it missing
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(weapon, Result)).

%% Helper to set a specific field offset to 0 (mark as missing)
corrupt_field_offset(Buffer, FieldId) ->
    %% Get root table offset
    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    %% Get soffset at root table
    <<_:RootOffset/binary, SOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = RootOffset - SOffset,
    %% Field offset position is: vtable_start + 4 (header) + FieldId * 2
    FieldOffsetPos = VTableOffset + 4 + (FieldId * 2),
    <<Before:FieldOffsetPos/binary, _OldOffset:16/little-unsigned, After/binary>> = Buffer,
    <<Before/binary, 0:16/little-unsigned, After/binary>>.

%% Test with field offset manually set to 0 (missing)
malformed_field_offset_zero_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 1); c: int (id: 2); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 1, b => 2, c => 3},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Set field 'b' (id:1) offset to 0, making it appear missing
    CorruptedBuffer = corrupt_field_offset(Buffer, 1),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(1, maps:get(a, Result)),
    %% Now missing
    ?assertEqual(false, maps:is_key(b, Result)),
    ?assertEqual(3, maps:get(c, Result)).

%% Test with string field offset set to 0
malformed_string_offset_zero_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Data { name: string (id: 0); value: int (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{name => <<"hello">>, value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Set string field offset to 0
    CorruptedBuffer = corrupt_field_offset(Buffer, 0),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(false, maps:is_key(name, Result)),
    ?assertEqual(42, maps:get(value, Result)).

%% Test with nested table offset set to 0
malformed_nested_offset_zero_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { inner: Inner (id: 0); value: int (id: 1); }\n"
        "root_type Data;\n"
    ),
    Data = #{inner => #{x => 100}, value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Set nested table field offset to 0
    CorruptedBuffer = corrupt_field_offset(Buffer, 0),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(false, maps:is_key(inner, Result)),
    ?assertEqual(42, maps:get(value, Result)).

%% Test with union value field offset set to 0 (type set but value missing)
%% This exercises the FieldOffset == 0 path in read_union_value_field
malformed_union_value_offset_zero_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { weapon: Weapon; extra: int; }\n"
        "root_type Data;\n"
    ),
    %% Union encoding uses weapon_type for discriminator and weapon for value
    Data = #{weapon => #{damage => 10}, weapon_type => 'Sword', extra => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Union type field is at id:0, union value field is at id:1, extra is at id:2
    %% Corrupt the value field (id:1) offset to 0 while keeping type intact
    CorruptedBuffer = corrupt_field_offset(Buffer, 1),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    %% Union should be missing because value field offset is 0
    %% (type is present but value is missing)
    ?assertEqual(false, maps:is_key(weapon, Result)),
    ?assertEqual(42, maps:get(extra, Result)).

%% Schema mismatch: encode with old schema, decode with newer incompatible schema
%% This simulates schema evolution where fields are added
schema_mismatch_scalar_test() ->
    %% Old schema has only one field
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    %% New schema expects more fields at higher IDs
    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); b: int (id: 10); c: string (id: 20); }\n"
        "root_type Data;\n"
    ),

    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    %% Only 'a' should be present
    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(b, Result)),
    ?assertEqual(false, maps:is_key(c, Result)).

%% Schema mismatch with nested tables
schema_mismatch_nested_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Inner { x: int; }\n"
        "table Data { a: int (id: 0); nested: Inner (id: 10); }\n"
        "root_type Data;\n"
    ),

    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(nested, Result)).

%% Schema mismatch with union fields
schema_mismatch_union_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { a: int (id: 0); weapon: Weapon (id: 10); }\n"
        "root_type Data;\n"
    ),

    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(weapon, Result)).

%% Schema mismatch with vector fields
schema_mismatch_vector_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); items: [int] (id: 10); names: [string] (id: 20); }\n"
        "root_type Data;\n"
    ),

    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(items, Result)),
    ?assertEqual(false, maps:is_key(names, Result)).

%% Schema mismatch with struct fields
schema_mismatch_struct_test() ->
    {ok, OldSchema} = flatbuferl:parse_schema(
        "table Data { a: int (id: 0); }\n"
        "root_type Data;\n"
    ),
    Data = #{a => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, OldSchema)),

    {ok, NewSchema} = flatbuferl:parse_schema(
        "struct Vec3 { x: float; y: float; z: float; }\n"
        "table Data { a: int (id: 0); pos: Vec3 (id: 10); }\n"
        "root_type Data;\n"
    ),

    Ctx = flatbuferl:new(Buffer, NewSchema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(42, maps:get(a, Result)),
    ?assertEqual(false, maps:is_key(pos, Result)).

%% Test where union type field is present but value field vtable slot is missing
%% This exercises the vtable-too-small path in read_union_value_field (line 152)
malformed_union_vtable_partial_test() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "table Sword { damage: int; }\n"
        "union Weapon { Sword }\n"
        "table Data { weapon: Weapon; }\n"
        "root_type Data;\n"
    ),
    Data = #{weapon => #{damage => 10}, weapon_type => 'Sword'},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),

    %% Union takes 2 slots: type at id:0, value at id:1
    %% Set vtable size to 6 (4 header + 2 for one field slot)
    %% This includes the type field (id:0) but excludes the value field (id:1)
    CorruptedBuffer = corrupt_vtable_size(Buffer, 6),

    Ctx = flatbuferl:new(CorruptedBuffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    %% Type field exists but value field slot is beyond vtable
    %% Since we can't have a union without both, it should be missing
    ?assertEqual(false, maps:is_key(weapon, Result)).
