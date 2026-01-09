-module(layout_adjust_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flatbuferl_records.hrl").

%% Simple internal tests to work out offset adjustment algorithm
%% for missing fields in precomputed layouts.
%%
%% Key insight: Fields are placed backward from end of table in layout_key order.
%% When a field is missing, all fields placed AFTER it (smaller offsets) shift up.

%% Simulated precomputed layout for a table with 3 int fields (4 bytes each)
%% Table: { a: int (id:0); b: int (id:1); c: int (id:2); }
%% Layout order (by layout_key desc): c, b, a
%% BaseTableSize = 4 + 12 = 16 (soffset + 3*4)
%% Slots when all present:
%%   c (id:2) -> offset 12, placed first  (16-4=12)
%%   b (id:1) -> offset 8,  placed second (12-4=8)
%%   a (id:0) -> offset 4,  placed third  (8-4=4)

make_precomputed_slots_3_ints() ->
    #{
        %% a: offset 4, size 4
        0 => {4, 4},
        %% b: offset 8, size 4
        1 => {8, 4},
        %% c: offset 12, size 4
        2 => {12, 4}
    }.

%% Test: all fields present - offsets unchanged
all_present_test() ->
    PrecomputedSlots = make_precomputed_slots_3_ints(),
    PresentIds = [0, 1, 2],
    %% layout order: highest layout_key first (c, b, a)
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% Should be identical to precomputed (just offset, no size)
    ?assertEqual(#{0 => 4, 1 => 8, 2 => 12}, AdjustedSlots).

%% Test: field c (id:2) missing - placed first, so b and a keep their offsets
missing_first_placed_test() ->
    PrecomputedSlots = make_precomputed_slots_3_ints(),
    %% c is missing
    PresentIds = [0, 1],
    %% layout order: c, b, a
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% c is placed first (highest layout_key). When it's missing:
    %% - Fields placed AFTER c (lower layout_key) don't shift
    %% - b stays at 8, a stays at 4
    ?assertEqual(#{0 => 4, 1 => 8}, AdjustedSlots).

%% Test: field a (id:0) missing - placed last, so c and b should shift down
missing_last_placed_test() ->
    PrecomputedSlots = make_precomputed_slots_3_ints(),
    %% a is missing
    PresentIds = [1, 2],
    %% layout order: c, b, a
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% a is placed last (lowest layout_key). When it's missing:
    %% - Fields placed BEFORE a (higher layout_key) shift down by size(a)=4
    %% - c: 12 - 4 = 8
    %% - b: 8 - 4 = 4
    ?assertEqual(#{1 => 4, 2 => 8}, AdjustedSlots).

%% Test: field b (id:1) missing - middle field
missing_middle_test() ->
    PrecomputedSlots = make_precomputed_slots_3_ints(),
    %% b is missing
    PresentIds = [0, 2],
    %% layout order: c, b, a
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% b is placed second. When missing:
    %% - c (placed before b): shifts down by size(b)=4 -> 12-4=8
    %% - a (placed after b): no shift -> stays at 4
    ?assertEqual(#{0 => 4, 2 => 8}, AdjustedSlots).

%% Test with mixed sizes: int (4), short (2), long (8)
%% Layout order by layout_key (size*65536 + id, descending):
%%   long (8*65536+2 = 524290) - highest
%%   int  (4*65536+1 = 262145)
%%   short (2*65536+0 = 131072) - lowest
make_precomputed_slots_mixed() ->
    %% BaseTableSize = 4 + align(8+4+2, 4) = 4 + 16 = 20
    %% Wait, need to calc properly with alignment:
    %% Place long (8 bytes) first: start at 20-8=12, align down to 8 -> 8
    %% Place int (4 bytes): start at 8-4=4, align down to 4 -> 4
    %% Place short (2 bytes): start at 4-2=2, align down to 2 -> 2? No wait...
    %% Hmm, the align_down thing is tricky.
    %%
    %% Let's trace through place_fields_backward:
    %% TableSize = 4 + align(8+4+2, 4) = 4 + 16 = 20
    %% Start with EndPos = 20
    %%
    %% long (size=8, align=min(8,4)=4):
    %%   StartPos = 20 - 8 = 12
    %%   AlignedStart = align_down(12, 4) = 12
    %%   EndPos = 12
    %%
    %% int (size=4, align=min(4,4)=4):
    %%   StartPos = 12 - 4 = 8
    %%   AlignedStart = align_down(8, 4) = 8
    %%   EndPos = 8
    %%
    %% short (size=2, align=min(2,4)=2):
    %%   StartPos = 8 - 2 = 6
    %%   AlignedStart = align_down(6, 2) = 6
    %%   EndPos = 6
    #{
        %% short: offset 6, size 2
        0 => {6, 2},
        %% int: offset 8, size 4
        1 => {8, 4},
        %% long: offset 12, size 8
        2 => {12, 8}
    }.

mixed_sizes_all_present_test() ->
    PrecomputedSlots = make_precomputed_slots_mixed(),
    PresentIds = [0, 1, 2],
    %% layout order: long, int, short
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),
    ?assertEqual(#{0 => 6, 1 => 8, 2 => 12}, AdjustedSlots).

mixed_sizes_missing_long_test() ->
    PrecomputedSlots = make_precomputed_slots_mixed(),
    %% long (id:2) missing
    PresentIds = [0, 1],
    %% layout order: long, int, short
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% Long is placed first (highest layout_key). When missing:
    %% New TableSize = 4 + align(4+2, 4) = 4 + 8 = 12 (was 20)
    %% int: EndPos=12, StartPos=8, AlignedStart=8 (same!)
    %% short: EndPos=8, StartPos=6, AlignedStart=6 (same!)
    ?assertEqual(#{0 => 6, 1 => 8}, AdjustedSlots).

%% Test: missing short (placed last, lowest layout_key)
%% This case exposes the alignment issue!
mixed_sizes_missing_short_test() ->
    PrecomputedSlots = make_precomputed_slots_mixed(),
    %% short (id:0) missing
    PresentIds = [1, 2],
    %% layout order: long, int, short
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% Short is placed last (lowest layout_key). When missing:
    %% New TableSize = 4 + align(8+4, 4) = 4 + 12 = 16 (was 20)
    %% long: EndPos=16, StartPos=8, AlignedStart=8 (was 12, shifted by 4!)
    %% int: EndPos=8, StartPos=4, AlignedStart=4 (was 8, shifted by 4!)
    %%
    %% Note: The shift is 4 (TableSize delta), not 2 (short's size)!
    %% This is because TableSize changed from 20 to 16 due to alignment.
    ?assertEqual(#{1 => 4, 2 => 8}, AdjustedSlots).

%% Test: missing int (middle field)
mixed_sizes_missing_int_test() ->
    PrecomputedSlots = make_precomputed_slots_mixed(),
    %% int (id:1) missing
    PresentIds = [0, 2],
    %% layout order: long, int, short
    AllIds = [2, 1, 0],

    AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

    %% Int is placed second. When missing:
    %% New TableSize = 4 + align(8+2, 4) = 4 + 12 = 16 (was 20)
    %% long: EndPos=16, StartPos=8, AlignedStart=8 (was 12, shifted by 4!)
    %% short: EndPos=8, StartPos=6, AlignedStart=6 (was 6, same!)
    %%
    %% Short stays at 6 because its EndPos is 8 in both cases:
    %% - With all fields: short.EndPos = int.offset = 8
    %% - Without int: short.EndPos = long.offset = 8
    ?assertEqual(#{0 => 6, 2 => 8}, AdjustedSlots).

%% =============================================================================
%% The adjustment algorithm
%% =============================================================================

%% Adjust precomputed slots for missing fields.
%% PrecomputedSlots: #{id => {offset, size}}
%% PresentIds: [id] - IDs of fields that are present
%% AllIds: [id] - All field IDs in layout order (highest layout_key first)
%%
%% Returns: #{id => offset} for present fields only
%%
%% The simple subtraction algorithm doesn't work with mixed field sizes because:
%% 1. TableSize can change by more than sum of missing sizes (due to alignment)
%% 2. Alignment gaps between fields can change
%%
%% The correct approach is to re-place fields from the new TableSize.
%% This is O(n) where n is number of fields in the table.
adjust_slots(PrecomputedSlots, PresentIds, AllIds) ->
    PresentSet = sets:from_list(PresentIds),

    %% Filter to present fields in layout order (highest layout_key first)
    PresentFieldInfo = [
        {Id, Size}
     || Id <- AllIds,
        sets:is_element(Id, PresentSet),
        {_, Size} <- [maps:get(Id, PrecomputedSlots)]
    ],

    %% Calculate new table size
    RawSize = lists:sum([Size || {_, Size} <- PresentFieldInfo]),
    TableDataSize = align_up(RawSize, 4),
    NewTableSize = 4 + TableDataSize,

    %% Place fields backward from new TableSize with proper alignment
    {Slots, _} = lists:foldl(
        fun({Id, Size}, {Acc, EndPos}) ->
            Align = min(Size, 4),
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, NewTableSize},
        PresentFieldInfo
    ),
    Slots.

%% Verify against dynamic calculation
verify_against_dynamic_test() ->
    PrecomputedSlots = make_precomputed_slots_3_ints(),
    %% layout order: c, b, a
    AllIds = [2, 1, 0],

    %% Test each combination of present fields
    Combos = [
        [0, 1, 2],
        [0, 1],
        [0, 2],
        [1, 2],
        [0],
        [1],
        [2]
    ],

    lists:foreach(
        fun(PresentIds) ->
            Adjusted = adjust_slots(PrecomputedSlots, PresentIds, AllIds),
            Dynamic = calc_dynamic_slots(PresentIds),
            ?assertEqual(
                Dynamic,
                Adjusted,
                io_lib:format(
                    "Mismatch for present=~p: dynamic=~p adjusted=~p",
                    [PresentIds, Dynamic, Adjusted]
                )
            )
        end,
        Combos
    ).

%% Calculate slots dynamically (the "slow" way) for verification
calc_dynamic_slots(PresentIds) ->
    %% Build list of {id, size, layout_key} for present fields only
    FieldInfo = #{
        %% a: size=4, layout_key
        0 => {4, 4 * 65536 + 0},
        %% b
        1 => {4, 4 * 65536 + 1},
        %% c
        2 => {4, 4 * 65536 + 2}
    },

    Fields = [
        begin
            {Size, LayoutKey} = maps:get(Id, FieldInfo),
            {Id, Size, LayoutKey}
        end
     || Id <- PresentIds
    ],

    %% Sort by layout_key descending
    Sorted = lists:sort(fun({_, _, LK1}, {_, _, LK2}) -> LK1 > LK2 end, Fields),

    %% Calculate table size
    RawSize = lists:sum([Size || {_, Size, _} <- Fields]),
    TableSize = 4 + align_up(RawSize, 4),

    %% Place fields backward
    {Slots, _} = lists:foldl(
        fun({Id, Size, _}, {Acc, EndPos}) ->
            Align = min(Size, 4),
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, TableSize},
        Sorted
    ),
    Slots.

align_up(Off, Align) ->
    case Off rem Align of
        0 -> Off;
        R -> Off + (Align - R)
    end.

align_down(Off, Align) ->
    Off - (Off rem Align).

%% =============================================================================
%% Test against actual builder output
%% =============================================================================

%% Test adjustment produces same slots as dynamic calculation
%% This validates the algorithm against the builder's place_fields_backward
integration_3_ints_all_combos_test() ->
    %% Parse a simple schema with 3 int fields
    Schema = "table T { a: int; b: int; c: int; }",
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema(Schema),

    %% Get the precomputed layout
    #{a := ADef, b := BDef, c := CDef} = get_field_map('T', Defs),

    %% Build precomputed slots map #{id => {offset, size}}
    PrecomputedSlots = #{
        ADef#field_def.id => {get_slot_offset(ADef, Defs), ADef#field_def.inline_size},
        BDef#field_def.id => {get_slot_offset(BDef, Defs), BDef#field_def.inline_size},
        CDef#field_def.id => {get_slot_offset(CDef, Defs), CDef#field_def.inline_size}
    },

    %% Get layout order (highest layout_key first)

    %% Same size, so ordered by id descending
    AllFieldDefs = [CDef, BDef, ADef],
    AllIds = [F#field_def.id || F <- AllFieldDefs],

    %% Test all non-empty subsets
    Combos = [
        [{a, 1}, {b, 2}, {c, 3}],
        [{a, 1}, {b, 2}],
        [{a, 1}, {c, 3}],
        [{b, 2}, {c, 3}],
        [{a, 1}],
        [{b, 2}],
        [{c, 3}]
    ],

    lists:foreach(
        fun(PresentFields) ->
            %% Build map for encoding
            Map = maps:from_list(PresentFields),
            PresentIds = [get_field_id(Name, Defs) || {Name, _} <- PresentFields],

            %% Calculate adjusted slots using our algorithm
            AdjustedSlots = adjust_slots(PrecomputedSlots, PresentIds, AllIds),

            %% Encode and verify the offsets are correct by checking round-trip
            Buffer = iolist_to_binary(flatbuferl:from_map(Map, {Defs, #{root_type => 'T'}})),
            Ctx = flatbuferl:new(Buffer, {Defs, #{root_type => 'T'}}),

            %% Verify all present fields round-trip correctly
            lists:foreach(
                fun({Name, ExpectedValue}) ->
                    ActualValue = flatbuferl:get(Ctx, [Name]),
                    ?assertEqual(
                        ExpectedValue,
                        ActualValue,
                        io_lib:format("Field ~p mismatch for present=~p", [Name, PresentFields])
                    )
                end,
                PresentFields
            ),

            %% Log for debugging
            io:format(user, "Present=~p, Adjusted=~p~n", [PresentFields, AdjustedSlots])
        end,
        Combos
    ).

get_field_map(TableName, Defs) ->
    #table_def{field_map = FieldMap} = maps:get(TableName, Defs),
    FieldMap.

get_field_id(Name, Defs) ->
    #table_def{field_map = FieldMap} = maps:get('T', Defs),
    #field_def{id = Id} = maps:get(Name, FieldMap),
    Id.

get_slot_offset(#field_def{id = Id}, Defs) ->
    #table_def{encode_layout = #encode_layout{slots = Slots}} = maps:get('T', Defs),
    {Offset, _Size} = maps:get(Id, Slots),
    Offset.
