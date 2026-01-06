-module(builder).

-export([from_map/3, from_map/4]).

%% =============================================================================
%% Public API
%% =============================================================================

from_map(Map, Defs, RootType) ->
    from_map(Map, Defs, RootType, no_file_id).

from_map(Map, Defs, RootType, FileId) ->
    {table, Fields} = maps:get(RootType, Defs),
    FieldValues = collect_fields(Map, Fields, Defs),
    {Scalars, Refs} = lists:partition(
        fun({_Id, Type, _Val}) -> is_scalar_type(Type) end,
        FieldValues
    ),

    %% Header size: 4 (root offset) + 4 (file id) if file id present
    HeaderSize =
        case FileId of
            no_file_id -> 4;
            _ -> 8
        end,

    %% Calculate table size including ref padding
    TableSizeWithPadding = calc_table_size_with_padding(Scalars, Refs, HeaderSize, Defs),

    %% Build root vtable
    VTable = build_vtable_with_size(Scalars, Refs, 4, TableSizeWithPadding),
    VTableSize = byte_size(VTable),

    %% Pre-build ref data to discover vtables for potential sharing
    %% We need to know if any nested vtable matches root vtable
    {RefVTables, _} = collect_ref_vtables(Refs, Defs),

    %% Check if root vtable can be shared with ref data
    case lists:member(VTable, RefVTables) of
        true ->
            %% Root vtable matches a ref vtable - use vtable-after layout
            encode_root_vtable_after(
                Map,
                Defs,
                RootType,
                FileId,
                Scalars,
                Refs,
                VTable,
                TableSizeWithPadding,
                HeaderSize
            );
        false ->
            %% No match - use standard vtable-before layout
            encode_root_vtable_before(
                Scalars,
                Refs,
                VTable,
                VTableSize,
                TableSizeWithPadding,
                HeaderSize,
                FileId,
                Defs
            )
    end.

%% Standard layout: header | [pad] | vtable | soffset | table | ref_data
encode_root_vtable_before(
    Scalars,
    Refs,
    VTable,
    VTableSize,
    TableSizeWithPadding,
    HeaderSize,
    FileId,
    Defs
) ->
    %% Calculate table position (4-byte aligned, plus adjustment for 8-byte fields)
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos4 = align_offset(TablePosUnaligned, 4),

    %% If table has 8-byte fields, ensure they're 8-byte aligned in the buffer
    First8ByteOffset = find_first_8byte_field_offset(Scalars ++ Refs, TableSizeWithPadding),
    TablePos =
        case First8ByteOffset of
            none ->
                TablePos4;
            Offset ->
                case (TablePos4 + Offset) rem 8 of
                    0 -> TablePos4;
                    _ -> TablePos4 + 4
                end
        end,
    PreVTablePad = TablePos - VTableSize - HeaderSize,

    %% Build table data
    {TableData, RefDataBin} = build_table_data(Scalars, Refs, TablePos, Defs),

    %% soffset points back to vtable (positive)
    VTablePos = HeaderSize + PreVTablePad,
    SOffset = TablePos - VTablePos,

    [
        <<TablePos:32/little-unsigned>>,
        file_id_bin(FileId),
        <<0:(PreVTablePad * 8)>>,
        VTable,
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ].

%% Shared vtable layout: header | soffset | table | ref_data (vtable is inside ref_data)
encode_root_vtable_after(
    _Map,
    Defs,
    _RootType,
    FileId,
    Scalars,
    Refs,
    VTable,
    TableSizeWithPadding,
    HeaderSize
) ->
    %% Root table starts right after header (no vtable before it)
    TablePos = HeaderSize,
    RefDataStart = TablePos + TableSizeWithPadding,

    %% Build field layout to get ref field offsets
    AllFields = lists:sort(fun field_layout_order/2, Scalars ++ Refs),
    Slots = place_fields_backward(AllFields, TableSizeWithPadding),
    FieldLayout = [{Id, Type, Value, maps:get(Id, Slots)} || {Id, Type, Value} <- AllFields],
    SortedLayout = lists:sort(fun({_, _, _, A}, {_, _, _, B}) -> A =< B end, FieldLayout),

    %% Extract refs in flatc order with field offsets
    RefFields = [
        {Id, Type, Value, Off}
     || {Id, Type, Value, Off} <- SortedLayout,
        not is_scalar_type(Type)
    ],
    RefFieldsOrdered = sort_refs_flatc_order_4(RefFields),

    %% Build ref data with vtable sharing, getting ref positions and shared vtable position
    {RefDataBin, RefPositions, SharedVTablePos} = build_ref_data_with_vtable_sharing(
        RefFieldsOrdered, RefDataStart, VTable, Defs
    ),

    %% Build table data with correct uoffsets pointing to ref positions
    TableData = build_inline_data(SortedLayout, TablePos, RefPositions),

    %% soffset points forward to shared vtable (negative because vtable is after)

    %% Will be negative
    SOffset = TablePos - SharedVTablePos,

    [
        <<TablePos:32/little-unsigned>>,
        file_id_bin(FileId),
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ].

file_id_bin(no_file_id) -> <<>>;
file_id_bin(B) when byte_size(B) =:= 4 -> B;
file_id_bin(_) -> error(invalid_file_id).

%% =============================================================================
%% Field Collection
%% =============================================================================

collect_fields(Map, Fields, Defs) ->
    lists:flatmap(
        fun(FieldDef) ->
            {Name, FieldId, Type, Default} = parse_field_def(FieldDef),
            case Type of
                {union_type, UnionName} ->
                    %% Union type field - look for <field>_type key directly
                    %% The type is an atom/string of the member name
                    case get_field_value(Map, Name) of
                        undefined ->
                            [];
                        MemberType when is_atom(MemberType) ->
                            {union, Members} = maps:get(UnionName, Defs),
                            TypeIndex = find_union_index(MemberType, Members, 1),
                            [{FieldId, {union_type, UnionName}, TypeIndex}];
                        MemberType when is_binary(MemberType) ->
                            {union, Members} = maps:get(UnionName, Defs),
                            TypeIndex = find_union_index(binary_to_atom(MemberType), Members, 1),
                            [{FieldId, {union_type, UnionName}, TypeIndex}];
                        _ ->
                            []
                    end;
                {union_value, UnionName} ->
                    %% Union value field - the map is the table value directly
                    %% Get the type from the corresponding _type field
                    TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
                    case get_field_value(Map, Name) of
                        undefined ->
                            [];
                        TableValue when is_map(TableValue) ->
                            MemberType =
                                case get_field_value(Map, TypeFieldName) of
                                    T when is_atom(T) -> T;
                                    T when is_binary(T) -> binary_to_atom(T);
                                    _ -> error({missing_union_type_field, TypeFieldName})
                                end,
                            [
                                {FieldId, {union_value, UnionName}, #{
                                    type => MemberType, value => TableValue
                                }}
                            ];
                        _ ->
                            []
                    end;
                _ ->
                    case get_field_value(Map, Name) of
                        undefined -> [];
                        Value when Value =:= Default -> [];
                        Value -> [{FieldId, resolve_type(Type, Defs), Value}]
                    end
            end
        end,
        Fields
    ).

find_union_index(Type, [Type | _], Index) -> Index;
%% Handle enum with explicit value
find_union_index(Type, [{Type, _Val} | _], Index) -> Index;
find_union_index(Type, [_ | Rest], Index) -> find_union_index(Type, Rest, Index + 1);
%% NONE
find_union_index(_Type, [], _Index) -> 0.

%% Look up field by atom key or binary key
get_field_value(Map, Name) when is_atom(Name) ->
    case maps:get(Name, Map, undefined) of
        undefined -> maps:get(atom_to_binary(Name), Map, undefined);
        Value -> Value
    end.

is_scalar_type({enum, _}) -> true;
%% Structs are inline fixed-size data
is_scalar_type({struct, _}) -> true;
%% Union type field is ubyte
is_scalar_type({union_type, _}) -> true;
is_scalar_type(bool) -> true;
is_scalar_type(byte) -> true;
is_scalar_type(ubyte) -> true;
is_scalar_type(int8) -> true;
is_scalar_type(uint8) -> true;
is_scalar_type(short) -> true;
is_scalar_type(ushort) -> true;
is_scalar_type(int16) -> true;
is_scalar_type(uint16) -> true;
is_scalar_type(int) -> true;
is_scalar_type(uint) -> true;
is_scalar_type(int32) -> true;
is_scalar_type(uint32) -> true;
is_scalar_type(long) -> true;
is_scalar_type(ulong) -> true;
is_scalar_type(int64) -> true;
is_scalar_type(uint64) -> true;
is_scalar_type(float) -> true;
is_scalar_type(float32) -> true;
is_scalar_type(double) -> true;
is_scalar_type(float64) -> true;
is_scalar_type(_) -> false.

%% Resolve type name to its definition (for enums and structs)
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _Values} -> {enum, Base};
        {struct, Fields} -> {struct, Fields};
        _ -> Type
    end;
resolve_type({vector, ElemType}, Defs) ->
    {vector, resolve_type(ElemType, Defs)};
resolve_type(Type, _Defs) ->
    Type.

%% =============================================================================
%% VTable Building
%% =============================================================================

build_vtable(Scalars, Refs, MaxAlign) ->
    AllFields = Scalars ++ Refs,
    case AllFields of
        [] ->
            <<4:16/little, 4:16/little>>;
        _ ->
            MaxId = lists:max([Id || {Id, _, _} <- AllFields]),
            NumSlots = MaxId + 1,
            VTableSize = 4 + (NumSlots * 2),

            TableDataSize = calc_table_data_size(Scalars, Refs),
            TableSize = 4 + TableDataSize,

            Slots = build_slots(Scalars, Refs, NumSlots, MaxAlign),

            iolist_to_binary([
                <<VTableSize:16/little, TableSize:16/little>>,
                Slots
            ])
    end.

%% Build vtable with pre-calculated table size (including ref padding)
build_vtable_with_size(Scalars, Refs, MaxAlign, TableSize) ->
    AllFields = Scalars ++ Refs,
    case AllFields of
        [] ->
            <<4:16/little, 4:16/little>>;
        _ ->
            MaxId = lists:max([Id || {Id, _, _} <- AllFields]),
            NumSlots = MaxId + 1,
            VTableSize = 4 + (NumSlots * 2),
            Slots = build_slots(Scalars, Refs, NumSlots, MaxAlign),
            iolist_to_binary([
                <<VTableSize:16/little, TableSize:16/little>>,
                Slots
            ])
    end.

%% Calculate table size including padding for ref data alignment
calc_table_size_with_padding(Scalars, Refs, HeaderSize, Defs) ->
    AllFields = lists:sort(fun field_layout_order/2, Scalars ++ Refs),
    RawSize = calc_backward_table_size(AllFields),
    %% Table data must be 4-byte aligned (for soffset alignment)
    TableDataSize = align_offset(RawSize, 4),
    BaseTableSize = 4 + TableDataSize,

    %% Calculate vtable size to determine table position
    NumSlots =
        case AllFields of
            [] -> 0;
            _ -> lists:max([Id || {Id, _, _} <- AllFields]) + 1
        end,
    VTableSize = 4 + (NumSlots * 2),
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos = align_offset(TablePosUnaligned, 4),

    %% Extract refs in flatc order: IDs 1, 2, 3, ..., N, then 0
    RefFields = [{Id, Type, Value} || {Id, Type, Value} <- AllFields, not is_scalar_type(Type)],
    RefFieldsOrdered = sort_refs_flatc_order(RefFields),

    %% Calculate ref padding
    RefPadding = calc_ref_padding_for_refs(RefFieldsOrdered, TablePos + BaseTableSize, Defs),
    BaseTableSize + RefPadding.

%% Sort refs in flatc order:
%% - If there's an id=0 ref: ascending order (1, 2, ..., N, 0)
%% - If NO id=0 ref: descending order (N, N-1, ..., 1)
sort_refs_flatc_order(Refs) ->
    {ZeroRefs, NonZeroRefs} = lists:partition(fun({Id, _, _}) -> Id =:= 0 end, Refs),
    case ZeroRefs of
        [] ->
            %% No id=0 ref: descending order
            lists:sort(fun({IdA, _, _}, {IdB, _, _}) -> IdA >= IdB end, NonZeroRefs);
        _ ->
            %% Has id=0 ref: ascending with 0 at end
            SortedNonZero = lists:sort(
                fun({IdA, _, _}, {IdB, _, _}) -> IdA =< IdB end, NonZeroRefs
            ),
            SortedNonZero ++ ZeroRefs
    end.

%% 4-tuple version for refs with offset
sort_refs_flatc_order_4(Refs) ->
    {ZeroRefs, NonZeroRefs} = lists:partition(fun({Id, _, _, _}) -> Id =:= 0 end, Refs),
    case ZeroRefs of
        [] ->
            %% No id=0 ref: descending order
            lists:sort(fun({IdA, _, _, _}, {IdB, _, _, _}) -> IdA >= IdB end, NonZeroRefs);
        _ ->
            %% Has id=0 ref: ascending with 0 at end
            SortedNonZero = lists:sort(
                fun({IdA, _, _, _}, {IdB, _, _, _}) -> IdA =< IdB end, NonZeroRefs
            ),
            SortedNonZero ++ ZeroRefs
    end.

%% Find the offset of the first 8-byte field using backward placement
find_first_8byte_field_offset(Fields, TableSize) ->
    AllFields = lists:sort(fun field_layout_order/2, Fields),
    Slots = place_fields_backward(AllFields, TableSize),
    %% Find 8-byte fields and get their offsets
    EightByteFields = [{Id, Type} || {Id, Type, _} <- AllFields, type_size(Type) =:= 8],
    case EightByteFields of
        [] ->
            none;
        _ ->
            Offsets = [maps:get(Id, Slots) || {Id, _} <- EightByteFields],
            %% Return smallest offset (first in table)
            lists:min(Offsets)
    end.

%% Calculate ref padding for field list (without offsets)
%% Ensure nested table's SOFFSET is 4-byte aligned (not just vtable start)
%% Position of soffset = Pos + VTableSize, which must be 4-byte aligned
calc_ref_padding_for_refs([], _Pos, _Defs) ->
    0;
calc_ref_padding_for_refs([{_, Type, Value} | _], Pos, Defs) when is_atom(Type), is_map(Value) ->
    case maps:get(Type, Defs, undefined) of
        {table, Fields} ->
            %% Calculate nested vtable size
            FieldValues = collect_fields(Value, Fields, Defs),
            {Scalars, Refs} = lists:partition(
                fun({_Id, T, _Val}) -> is_scalar_type(T) end,
                FieldValues
            ),
            VTableSize = calc_vtable_size(Scalars, Refs),
            %% Pad to make (Pos + VTableSize) 4-byte aligned
            SOffsetPos = Pos + VTableSize,
            (4 - (SOffsetPos rem 4)) rem 4;
        _ ->
            0
    end;
calc_ref_padding_for_refs(
    [{_, {union_value, UnionName}, #{type := MemberType, value := Value}} | _], Pos, Defs
) ->
    {union, _Members} = maps:get(UnionName, Defs),
    calc_ref_padding_for_refs([{0, MemberType, Value}], Pos, Defs);
calc_ref_padding_for_refs(_, _Pos, _Defs) ->
    0.

build_slots(Scalars, Refs, NumSlots, _MaxAlign) ->
    %% flatc builds table from END backward, placing largest fields at end
    %% 1. Sort by size descending, then ID descending
    %% 2. Calculate table end position
    %% 3. Place fields backward from end
    AllFields = lists:sort(fun field_layout_order/2, Scalars ++ Refs),

    %% Calculate table size (aligned to 4 bytes for soffset)
    TableDataSize = calc_backward_table_size(AllFields),
    TableSize = 4 + align_offset(TableDataSize, 4),

    %% Place fields backward from end of table
    Slots = place_fields_backward(AllFields, TableSize),

    [<<(maps:get(Id, Slots, 0)):16/little>> || Id <- lists:seq(0, NumSlots - 1)].

%% Sort by size descending, then by field ID descending
field_layout_order({IdA, TypeA, _}, {IdB, TypeB, _}) ->
    SizeA = field_inline_size(TypeA),
    SizeB = field_inline_size(TypeB),
    field_layout_order(SizeA, SizeB, IdA, IdB).

field_layout_order(SizeA, SizeB, _IdA, _IdB) when SizeA > SizeB -> true;
field_layout_order(SizeA, SizeB, _IdA, _IdB) when SizeA < SizeB -> false;
field_layout_order(_SizeA, _SizeB, IdA, IdB) -> IdA >= IdB.

%% Calculate raw data size for all fields
calc_backward_table_size(Fields) ->
    lists:sum([field_inline_size(Type) || {_, Type, _} <- Fields]).

%% Place fields backward from end of table
place_fields_backward(Fields, TableSize) ->
    {Slots, _} = lists:foldl(
        fun({Id, Type, _}, {Acc, EndPos}) ->
            Size = field_inline_size(Type),
            Align = min(Size, 4),
            %% Work backward: EndPos is where current field ends
            %% Field starts at EndPos - Size, but must be aligned
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, TableSize},
        Fields
    ),
    Slots.

%% Align offset DOWN to alignment boundary
align_down(Off, Align) ->
    Off - (Off rem Align).

%% Size of field as stored inline in table (refs are 4-byte uoffsets)
field_inline_size(string) -> 4;
field_inline_size({vector, _}) -> 4;
field_inline_size({union_value, _}) -> 4;
field_inline_size(Type) when is_atom(Type) -> type_size(Type);
field_inline_size({struct, _} = T) -> type_size(T);
field_inline_size({enum, _} = T) -> type_size(T);
field_inline_size({union_type, _}) -> 1;
%% Default for unknown refs
field_inline_size(_) -> 4.

%% Calculate total table data size (aligned to 4 bytes for soffset)
calc_table_data_size(Scalars, Refs) ->
    AllFields = Scalars ++ Refs,
    RawSize = calc_backward_table_size(AllFields),
    align_offset(RawSize, 4).

%% =============================================================================
%% Data Building
%% =============================================================================

%% Build table data with fields placed backward from end (like flatc)
build_table_data(Scalars, Refs, TablePos, Defs) ->
    AllFields = lists:sort(fun field_layout_order/2, Scalars ++ Refs),

    %% Calculate table size and field positions (backward placement)
    %% Table data must be 4-byte aligned for soffset
    RawSize = calc_backward_table_size(AllFields),
    TableDataSize = align_offset(RawSize, 4),
    BaseTableSize = 4 + TableDataSize,

    %% Build field layout with positions (using base table size for slot calculation)
    Slots = place_fields_backward(AllFields, BaseTableSize),
    FieldLayout = [{Id, Type, Value, maps:get(Id, Slots)} || {Id, Type, Value} <- AllFields],
    SortedLayout = lists:sort(fun({_, _, _, A}, {_, _, _, B}) -> A =< B end, FieldLayout),

    %% Extract refs in flatc order: IDs 1, 2, ..., N, then 0
    RefFields = [
        {Id, Type, Value, Off}
     || {Id, Type, Value, Off} <- SortedLayout,
        not is_scalar_type(Type)
    ],
    RefFieldsOrdered = sort_refs_flatc_order_4(RefFields),

    %% Calculate padding needed before ref data for nested table alignment
    RefPadding = calc_ref_padding(RefFieldsOrdered, TablePos + BaseTableSize, Defs),
    TableSize = BaseTableSize + RefPadding,
    RefDataStart = TablePos + TableSize,

    {RefDataBin, RefPositions} = build_ref_data(RefFieldsOrdered, RefDataStart, Defs),

    %% Build table inline data with padding
    TableData = build_inline_data(SortedLayout, TablePos, RefPositions),
    PadBin = <<0:(RefPadding * 8)>>,

    {<<TableData/binary, PadBin/binary>>, RefDataBin}.

%% Calculate padding needed before ref data
%% Ensure nested table's SOFFSET is 4-byte aligned (Pos + VTableSize must be 4-byte aligned)
calc_ref_padding([], _Pos, _Defs) ->
    0;
calc_ref_padding([{_, Type, Value, _} | _], Pos, Defs) when is_atom(Type), is_map(Value) ->
    case maps:get(Type, Defs, undefined) of
        {table, Fields} ->
            %% Calculate nested vtable size
            FieldValues = collect_fields(Value, Fields, Defs),
            {Scalars, Refs} = lists:partition(
                fun({_Id, T, _Val}) -> is_scalar_type(T) end,
                FieldValues
            ),
            VTableSize = calc_vtable_size(Scalars, Refs),
            %% Pad to make (Pos + VTableSize) 4-byte aligned
            SOffsetPos = Pos + VTableSize,
            (4 - (SOffsetPos rem 4)) rem 4;
        _ ->
            0
    end;
calc_ref_padding(
    [{_, {union_value, UnionName}, #{type := MemberType, value := Value}, _} | _], Pos, Defs
) ->
    {union, _Members} = maps:get(UnionName, Defs),
    calc_ref_padding([{0, MemberType, Value, 0}], Pos, Defs);
calc_ref_padding(_, _Pos, _Defs) ->
    0.

%% Collect all vtables that will be used in ref data (for vtable sharing detection)
collect_ref_vtables(Refs, Defs) ->
    lists:foldl(
        fun({_Id, Type, Value}, {VTAcc, Seen}) ->
            collect_vtables_from_ref(Type, Value, Defs, VTAcc, Seen)
        end,
        {[], #{}},
        Refs
    ).

collect_vtables_from_ref({vector, ElemType}, Values, Defs, VTAcc, Seen) when is_list(Values) ->
    %% For table vectors, collect vtables from elements
    case maps:get(ElemType, Defs, undefined) of
        {table, Fields} ->
            lists:foldl(
                fun(Value, {Acc, S}) ->
                    FieldValues = collect_fields(Value, Fields, Defs),
                    {Scalars, Refs} = lists:partition(
                        fun({_Id, T, _Val}) -> is_scalar_type(T) end,
                        FieldValues
                    ),
                    VT = build_vtable(Scalars, Refs, 4),
                    case maps:is_key(VT, S) of
                        true -> {Acc, S};
                        false -> {[VT | Acc], S#{VT => true}}
                    end
                end,
                {VTAcc, Seen},
                Values
            );
        _ ->
            {VTAcc, Seen}
    end;
collect_vtables_from_ref(Type, Value, Defs, VTAcc, Seen) when is_atom(Type), is_map(Value) ->
    %% Nested table
    case maps:get(Type, Defs, undefined) of
        {table, Fields} ->
            FieldValues = collect_fields(Value, Fields, Defs),
            {Scalars, Refs} = lists:partition(
                fun({_Id, T, _Val}) -> is_scalar_type(T) end,
                FieldValues
            ),
            VT = build_vtable(Scalars, Refs, 4),
            case maps:is_key(VT, Seen) of
                true -> {VTAcc, Seen};
                false -> {[VT | VTAcc], Seen#{VT => true}}
            end;
        _ ->
            {VTAcc, Seen}
    end;
collect_vtables_from_ref(_, _, _, VTAcc, Seen) ->
    {VTAcc, Seen}.

%% Build ref data with vtable sharing, returning the position of the shared vtable
%% RefFieldsOrdered: list of {Id, Type, Value, FieldOff} tuples
%% Returns: {RefDataBin, RefPositions, SharedVTablePos}
build_ref_data_with_vtable_sharing(RefFieldsOrdered, RefDataStart, SharedVTable, Defs) ->
    %% Encode refs and track shared vtable position and ref positions
    {DataBins, Positions, VTablePos, _} = lists:foldl(
        fun({_Id, Type, Value, FieldOff}, {DataAcc, PosAcc, VTPos, DataPos}) ->
            %% Add padding for 8-byte vector alignment if needed
            AlignPad = calc_vector_alignment_padding(Type, DataPos, Defs),
            PaddedPos = DataPos + AlignPad,

            DataBin = encode_ref(Type, Value, Defs),

            %% For nested tables, uoffset should point to soffset (after vtable)
            RefTargetPos =
                case is_nested_table_type(Type, Value, Defs) of
                    {true, VTableSize} -> PaddedPos + VTableSize;
                    false -> PaddedPos
                end,

            %% Check if this ref contains the shared vtable
            NewVTPos =
                case find_vtable_in_ref(DataBin, SharedVTable) of
                    {found, RelPos} -> PaddedPos + RelPos;
                    not_found -> VTPos
                end,

            PadBin = <<0:(AlignPad * 8)>>,
            {
                [DataBin, PadBin | DataAcc],
                PosAcc#{FieldOff => RefTargetPos},
                NewVTPos,
                PaddedPos + byte_size(DataBin)
            }
        end,
        {[], #{}, 0, RefDataStart},
        RefFieldsOrdered
    ),
    {iolist_to_binary(lists:reverse(DataBins)), Positions, VTablePos}.

%% Find the position of a vtable within a ref binary
find_vtable_in_ref(Bin, VTable) ->
    VTSize = byte_size(VTable),
    find_vtable_in_ref(Bin, VTable, VTSize, 0).

find_vtable_in_ref(Bin, VTable, VTSize, Offset) when byte_size(Bin) >= VTSize ->
    case Bin of
        <<VTable:VTSize/binary, _/binary>> ->
            {found, Offset};
        <<_, Rest/binary>> ->
            find_vtable_in_ref(Rest, VTable, VTSize, Offset + 1)
    end;
find_vtable_in_ref(_, _, _, _) ->
    not_found.

%% Build ref data and return map of field_offset -> data_position
%% For nested tables, position points to soffset, not vtable start
build_ref_data(RefFields, RefDataStart, Defs) ->
    {DataBins, Positions, _} = lists:foldl(
        fun({_Id, Type, Value, FieldOff}, {DataAcc, PosAcc, DataPos}) ->
            %% Add padding for 8-byte vector alignment if needed
            AlignPad = calc_vector_alignment_padding(Type, DataPos, Defs),
            PaddedPos = DataPos + AlignPad,

            DataBin = encode_ref(Type, Value, Defs),
            %% For nested tables, uoffset should point to soffset (after vtable)
            RefTargetPos =
                case is_nested_table_type(Type, Value, Defs) of
                    {true, VTableSize} -> PaddedPos + VTableSize;
                    false -> PaddedPos
                end,
            PadBin = <<0:(AlignPad * 8)>>,
            {
                [DataBin, PadBin | DataAcc],
                PosAcc#{FieldOff => RefTargetPos},
                PaddedPos + byte_size(DataBin)
            }
        end,
        {[], #{}, RefDataStart},
        RefFields
    ),
    {iolist_to_binary(lists:reverse(DataBins)), Positions}.

%% Calculate padding needed for vector 8-byte alignment
%% Vector data (after 4-byte length) must be 8-byte aligned for 8-byte elements
calc_vector_alignment_padding({vector, ElemType}, DataPos, Defs) ->
    ResolvedType = resolve_type(ElemType, Defs),
    case type_size(ResolvedType) of
        8 ->
            %% Need (DataPos + 4) % 8 == 0, so DataPos % 8 == 4
            (12 - (DataPos rem 8)) rem 8;
        _ ->
            0
    end;
calc_vector_alignment_padding(_, _, _) ->
    0.

%% Check if type is a nested table (vtable is at START, need offset to point to soffset)
%% Returns {true, VTableSize} or false
is_nested_table_type(Type, Value, Defs) when is_atom(Type), is_map(Value) ->
    case maps:get(Type, Defs, undefined) of
        {table, Fields} ->
            %% Calculate vtable size for this nested table
            FieldValues = collect_fields(Value, Fields, Defs),
            {Scalars, Refs} = lists:partition(
                fun({_Id, T, _Val}) -> is_scalar_type(T) end,
                FieldValues
            ),
            VTableSize = calc_vtable_size(Scalars, Refs),
            {true, VTableSize};
        _ ->
            false
    end;
is_nested_table_type({union_value, UnionName}, #{type := MemberType, value := Value}, Defs) ->
    %% Union value - check the member type
    {union, _Members} = maps:get(UnionName, Defs),
    is_nested_table_type(MemberType, Value, Defs);
is_nested_table_type(_, _, _) ->
    false.

%% Calculate vtable size for a table
calc_vtable_size(Scalars, Refs) ->
    AllFields = Scalars ++ Refs,
    case AllFields of
        [] ->
            4;
        _ ->
            MaxId = lists:max([Id || {Id, _, _} <- AllFields]),
            NumSlots = MaxId + 1,
            4 + (NumSlots * 2)
    end.

%% Build inline table data (scalars inline, refs as uoffsets)
build_inline_data(FieldLayout, TablePos, RefPositions) ->
    {Data, _} = lists:foldl(
        fun({_Id, Type, Value, FieldOff}, {Acc, CurOff}) ->
            %% Add padding if needed
            Pad = FieldOff - CurOff,
            PadBin = <<0:(Pad * 8)>>,

            %% Encode the field
            FieldBin =
                case is_scalar_type(Type) of
                    true ->
                        encode_scalar(Value, Type);
                    false ->
                        %% Reference - write uoffset to ref data
                        FieldAbsPos = TablePos + FieldOff,
                        RefDataPos = maps:get(FieldOff, RefPositions),
                        UOffset = RefDataPos - FieldAbsPos,
                        <<UOffset:32/little-signed>>
                end,
            {<<Acc/binary, PadBin/binary, FieldBin/binary>>, FieldOff + byte_size(FieldBin)}
        end,
        %% Start at offset 4
        {<<>>, 4},
        FieldLayout
    ),
    Data.

encode_string(Bin) ->
    Len = byte_size(Bin),
    TotalLen = 4 + Len + 1,
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    <<Len:32/little, Bin/binary, 0, 0:(PadLen * 8)>>.

encode_ref(string, Bin, _Defs) when is_binary(Bin) ->
    Len = byte_size(Bin),
    %% Pad string to 4-byte boundary

    %% length + data + null
    TotalLen = 4 + Len + 1,
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    <<Len:32/little, Bin/binary, 0, 0:(PadLen * 8)>>;
encode_ref({vector, ElemType}, Values, Defs) when is_list(Values) ->
    encode_vector(ElemType, Values, Defs);
encode_ref({union_value, UnionName}, #{type := MemberType, value := Value}, Defs) ->
    %% Union value - encode as the member table type
    {union, _Members} = maps:get(UnionName, Defs),
    encode_nested_table(MemberType, Value, Defs);
encode_ref(TableType, Map, Defs) when is_atom(TableType), is_map(Map) ->
    %% Nested table - build it inline
    %% Returns {Binary, TableEntryOffset} where TableEntryOffset is where soffset lives
    encode_nested_table(TableType, Map, Defs).

encode_vector(ElemType, Values, Defs) ->
    ResolvedType = resolve_type(ElemType, Defs),
    case is_scalar_type(ResolvedType) of
        true ->
            %% Scalar vector: length + inline elements
            Len = length(Values),
            Elements = [encode_scalar(V, ResolvedType) || V <- Values],
            ElementsBin = iolist_to_binary(Elements),
            %% Pad to 4-byte boundary
            TotalLen = 4 + byte_size(ElementsBin),
            PadLen = (4 - (TotalLen rem 4)) rem 4,
            <<Len:32/little, ElementsBin/binary, 0:(PadLen * 8)>>;
        false ->
            %% Reference vector (e.g., strings, tables): length + offsets, then data
            encode_ref_vector(ElemType, Values, Defs)
    end.

encode_ref_vector(ElemType, Values, Defs) ->
    case ElemType of
        string ->
            encode_string_vector_dedup(Values);
        _ ->
            encode_ref_vector_standard(ElemType, Values, Defs)
    end.

encode_string_vector_dedup(Values) ->
    %% String vector - flatc writes data in REVERSE order
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Encode strings in reverse order (like flatc does)
    ReversedValues = lists:reverse(Values),
    {DataBins, PosByValue, _} = lists:foldl(
        fun(Str, {DataAcc, PosMap, DataPos}) ->
            case maps:get(Str, PosMap, undefined) of
                undefined ->
                    DataBin = encode_string(Str),
                    {[DataBin | DataAcc], PosMap#{Str => DataPos}, DataPos + byte_size(DataBin)};
                _CachedPos ->
                    %% Already encoded, reuse position
                    {DataAcc, PosMap, DataPos}
            end
        end,
        {[], #{}, HeaderSize},
        ReversedValues
    ),

    %% Build offsets for original order, pointing to reverse-order data
    Offsets = lists:map(
        fun({Idx, Str}) ->
            OffsetPos = 4 + (Idx * 4),
            DataPos = maps:get(Str, PosByValue),
            UOffset = DataPos - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), Values)
    ),

    iolist_to_binary([
        <<Len:32/little>>,
        Offsets,
        lists:reverse(DataBins)
    ]).

encode_ref_vector_standard(ElemType, Values, Defs) ->
    %% Check if this is a table vector that needs vtable sharing
    case maps:get(ElemType, Defs, undefined) of
        {table, _Fields} ->
            encode_table_vector_with_sharing(ElemType, Values, Defs);
        _ ->
            encode_ref_vector_simple(ElemType, Values, Defs)
    end.

%% Simple ref vector encoding (non-table elements)
encode_ref_vector_simple(ElemType, Values, Defs) ->
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    ReversedValues = lists:reverse(Values),
    EncodedElems = [{encode_ref(ElemType, V, Defs), V} || V <- ReversedValues],

    {_, ElemPositions} = lists:foldl(
        fun({ElemBin, Value}, {DataPos, PosAcc}) ->
            VTableOffset =
                case is_nested_table_type(ElemType, Value, Defs) of
                    {true, VTableSize} -> VTableSize;
                    false -> 0
                end,
            {DataPos + byte_size(ElemBin), [{DataPos, VTableOffset, ElemBin} | PosAcc]}
        end,
        {HeaderSize, []},
        EncodedElems
    ),

    Offsets = lists:map(
        fun({Idx, {DataPos, VTableOffset, _}}) ->
            OffsetPos = 4 + (Idx * 4),
            UOffset = (DataPos + VTableOffset) - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), ElemPositions)
    ),

    iolist_to_binary([
        <<Len:32/little>>,
        Offsets,
        [Bin || {Bin, _} <- EncodedElems]
    ]).

%% Table vector encoding with vtable sharing (flatc compatible)
%% Layout: elements with vtable-after first, then shared vtable, then elements with vtable-before
%%
%% flatc processes elements in reverse order (last first), and for each unique vtable:
%% - The FIRST element processed (LAST in original order) gets vtable-after
%% - The LAST element processed (FIRST in original order) gets vtable-before
%% - The vtable is placed immediately before the vtable-before element
encode_table_vector_with_sharing(TableType, Values, Defs) ->
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Process in reverse order (last element first, like flatc)
    IndexedValues = lists:zip(lists:seq(0, Len - 1), Values),
    ReversedIndexedValues = lists:reverse(IndexedValues),

    %% Collect vtable info for each element
    ElemVTables = lists:map(
        fun({OrigIdx, Value}) ->
            {table, Fields} = maps:get(TableType, Defs),
            FieldValues = collect_fields(Value, Fields, Defs),
            {Scalars, Refs} = lists:partition(
                fun({_Id, Type, _Val}) -> is_scalar_type(Type) end,
                FieldValues
            ),
            VTable = build_vtable(Scalars, Refs, 4),
            {OrigIdx, VTable, Scalars, Refs}
        end,
        ReversedIndexedValues
    ),

    %% Identify vtable owners: FIRST element in original order (LAST in processing order)
    %% = element with LOWEST OrigIdx for each vtable
    VTableOwners = lists:foldl(
        fun({OrigIdx, VTable, _Scalars, _Refs}, Acc) ->
            case maps:get(VTable, Acc, undefined) of
                undefined -> Acc#{VTable => OrigIdx};
                ExistingIdx when OrigIdx < ExistingIdx -> Acc#{VTable => OrigIdx};
                _ -> Acc
            end
        end,
        #{},
        ElemVTables
    ),

    %% Encode all elements as body-only (soffset placeholder + data)
    %% Also track which element owns which vtable
    EncodedElems = lists:map(
        fun({OrigIdx, VTable, Scalars, Refs}) ->
            OwnerIdx = maps:get(VTable, VTableOwners),
            IsOwner = OrigIdx =:= OwnerIdx,
            %% Owner elements use normal padding (they're last and need trailing padding)
            %% Non-owner elements use minimal padding (followed by vtable which needs 2-byte align)
            TableBin =
                case IsOwner of
                    true -> encode_nested_table_body_only_padded(Scalars, Refs, Defs);
                    false -> encode_nested_table_body_only(Scalars, Refs, Defs)
                end,
            {OrigIdx, VTable, TableBin, IsOwner}
        end,
        ElemVTables
    ),

    %% Buffer order is same as ElemVTables order (reverse of original)
    %% But we need to insert vtables between vtable-after and vtable-before elements
    %%
    %% For each vtable group:
    %% - Non-owner elements come first (vtable-after, negative soffset)
    %% - Then the vtable
    %% - Then the owner element (vtable-before, positive soffset)

    %% Group elements by vtable and sort within each group
    VTableGroups = lists:foldl(
        fun({OrigIdx, VTable, TableBin, IsOwner}, Acc) ->
            Group = maps:get(VTable, Acc, []),
            Acc#{VTable => [{OrigIdx, TableBin, IsOwner} | Group]}
        end,
        #{},
        EncodedElems
    ),

    %% Build buffer: for each vtable group (in processing order), output non-owners, vtable, owner
    %% Sort vtable groups by the highest OrigIdx in each group (processing order)
    VTableGroupList = maps:to_list(VTableGroups),
    SortedVTableGroups = lists:sort(
        fun({_VT1, G1}, {_VT2, G2}) ->
            MaxIdx1 = lists:max([Idx || {Idx, _, _} <- G1]),
            MaxIdx2 = lists:max([Idx || {Idx, _, _} <- G2]),
            %% Descending by max index (first processed first)
            MaxIdx1 > MaxIdx2
        end,
        VTableGroupList
    ),
    SortedByVTable = lists:flatmap(
        fun({VTable, Group}) ->
            {Owners, NonOwners} = lists:partition(fun({_, _, IsOwner}) -> IsOwner end, Group),
            %% Non-owners in descending OrigIdx order (same as processing order)
            SortedNonOwners = lists:sort(
                fun({IdxA, _, _}, {IdxB, _, _}) -> IdxA > IdxB end,
                NonOwners
            ),
            %% Convert to buffer elements
            NonOwnerElems = [{vtable_after, Idx, Bin, VTable} || {Idx, Bin, _} <- SortedNonOwners],
            OwnerElems = [{vtable_before, Idx, Bin, VTable} || {Idx, Bin, _} <- Owners],
            %% Insert vtable before owner
            NonOwnerElems ++ [{vtable_insert, VTable}] ++ OwnerElems
        end,
        SortedVTableGroups
    ),

    %% Calculate positions with proper alignment
    %% Tables must be 4-byte aligned, so vtables need padding to ensure
    %% the following vtable_before element is 4-byte aligned
    {ElemInfos, VTablePositions, PaddingMap, _FinalPos} = lists:foldl(
        fun(Elem, {InfoAcc, VTPos, PadMap, DataPos}) ->
            case Elem of
                {vtable_insert, VTable} ->
                    VTSize = byte_size(VTable),
                    %% Calculate padding needed so that (DataPos + Pad + VTSize) is 4-byte aligned
                    %% This ensures the vtable_before element after the vtable is aligned
                    AlignPad = (4 - ((DataPos + VTSize) rem 4)) rem 4,
                    PaddedPos = DataPos + AlignPad,
                    {InfoAcc, VTPos#{VTable => PaddedPos}, PadMap#{VTable => AlignPad},
                        PaddedPos + VTSize};
                {vtable_after, OrigIdx, Bin, _VTable} ->
                    %% soffset is at start of body
                    SOffsetPos = DataPos,
                    {
                        [{OrigIdx, SOffsetPos, Bin} | InfoAcc],
                        VTPos,
                        PadMap,
                        DataPos + byte_size(Bin)
                    };
                {vtable_before, OrigIdx, Bin, _VTable} ->
                    %% soffset is at start of body
                    SOffsetPos = DataPos,
                    {
                        [{OrigIdx, SOffsetPos, Bin} | InfoAcc],
                        VTPos,
                        PadMap,
                        DataPos + byte_size(Bin)
                    }
            end
        end,
        {[], #{}, #{}, HeaderSize},
        SortedByVTable
    ),

    %% Sort by original index to build correct offset array
    ElemInfosByOrigIdx = lists:sort(fun({IdxA, _, _}, {IdxB, _, _}) -> IdxA =< IdxB end, ElemInfos),

    %% Build offsets array in original order
    Offsets = lists:map(
        fun({Idx, {_OrigIdx, SOffsetPos, _Bin}}) ->
            OffsetPos = 4 + (Idx * 4),
            UOffset = SOffsetPos - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), ElemInfosByOrigIdx)
    ),

    %% Build lookup map for soffset positions
    SOffsetPosMap = maps:from_list([{Idx, Pos} || {Idx, Pos, _Bin} <- ElemInfos]),

    %% Build data section with proper soffsets
    DataBins = lists:map(
        fun(Elem) ->
            case Elem of
                {vtable_insert, VTable} ->
                    %% Insert padding before vtable for alignment
                    PadLen = maps:get(VTable, PaddingMap, 0),
                    PadBin = <<0:(PadLen * 8)>>,
                    <<PadBin/binary, VTable/binary>>;
                {vtable_after, OrigIdx, Bin, VTable} ->
                    %% Negative soffset (vtable is after this element)
                    VTablePos = maps:get(VTable, VTablePositions),
                    SOffsetPos = maps:get(OrigIdx, SOffsetPosMap),
                    %% Negative
                    SOffset = SOffsetPos - VTablePos,
                    <<_:32, Rest/binary>> = Bin,
                    <<SOffset:32/little-signed, Rest/binary>>;
                {vtable_before, OrigIdx, Bin, VTable} ->
                    %% Positive soffset (vtable is before this element)
                    VTablePos = maps:get(VTable, VTablePositions),
                    SOffsetPos = maps:get(OrigIdx, SOffsetPosMap),
                    %% Positive
                    SOffset = SOffsetPos - VTablePos,
                    <<_:32, Rest/binary>> = Bin,
                    <<SOffset:32/little-signed, Rest/binary>>
            end
        end,
        SortedByVTable
    ),

    iolist_to_binary([
        <<Len:32/little>>,
        Offsets,
        DataBins
    ]).

%% Encode nested table as body only (soffset placeholder + inline data + refs)
%% Uses minimal string padding for use in table vectors with vtable sharing (non-owner elements)
encode_nested_table_body_only(Scalars, Refs, Defs) ->
    {TableData, RefDataBin} = build_table_data_minimal_padding(Scalars, Refs, 0, Defs),
    iolist_to_binary([
        %% Placeholder soffset
        <<0:32/little-signed>>,
        TableData,
        RefDataBin
    ]).

%% Encode nested table as body only with normal string padding (for owner elements)
encode_nested_table_body_only_padded(Scalars, Refs, Defs) ->
    {TableData, RefDataBin} = build_table_data(Scalars, Refs, 0, Defs),
    iolist_to_binary([
        %% Placeholder soffset
        <<0:32/little-signed>>,
        TableData,
        RefDataBin
    ]).

%% Build table data with minimal string padding (for table vectors with vtable sharing)
%% Strings are NOT padded to 4-byte boundary since vtables only need 2-byte alignment
build_table_data_minimal_padding(Scalars, Refs, TablePos, Defs) ->
    AllFields = lists:sort(fun field_layout_order/2, Scalars ++ Refs),
    RawSize = calc_backward_table_size(AllFields),
    TableDataSize = align_offset(RawSize, 4),
    BaseTableSize = 4 + TableDataSize,
    Slots = place_fields_backward(AllFields, BaseTableSize),
    FieldLayout = [{Id, Type, Value, maps:get(Id, Slots)} || {Id, Type, Value} <- AllFields],
    SortedLayout = lists:sort(fun({_, _, _, A}, {_, _, _, B}) -> A =< B end, FieldLayout),
    RefFields = [
        {Id, Type, Value, Off}
     || {Id, Type, Value, Off} <- SortedLayout,
        not is_scalar_type(Type)
    ],
    RefFieldsOrdered = sort_refs_flatc_order_4(RefFields),
    RefPadding = calc_ref_padding(RefFieldsOrdered, TablePos + BaseTableSize, Defs),
    TableSize = BaseTableSize + RefPadding,
    RefDataStart = TablePos + TableSize,
    {RefDataBin, RefPositions} = build_ref_data_minimal_padding(
        RefFieldsOrdered, RefDataStart, Defs
    ),
    TableData = build_inline_data(SortedLayout, TablePos, RefPositions),
    PadBin = <<0:(RefPadding * 8)>>,
    {<<TableData/binary, PadBin/binary>>, RefDataBin}.

%% Build ref data with minimal string padding
build_ref_data_minimal_padding(RefFields, RefDataStart, Defs) ->
    {DataBins, Positions, _} = lists:foldl(
        fun({_Id, Type, Value, FieldOff}, {DataAcc, PosAcc, DataPos}) ->
            AlignPad = calc_vector_alignment_padding(Type, DataPos, Defs),
            PaddedPos = DataPos + AlignPad,
            DataBin = encode_ref_minimal_padding(Type, Value, Defs),
            RefTargetPos =
                case is_nested_table_type(Type, Value, Defs) of
                    {true, VTableSize} -> PaddedPos + VTableSize;
                    false -> PaddedPos
                end,
            PadBin = <<0:(AlignPad * 8)>>,
            {
                [DataBin, PadBin | DataAcc],
                PosAcc#{FieldOff => RefTargetPos},
                PaddedPos + byte_size(DataBin)
            }
        end,
        {[], #{}, RefDataStart},
        RefFields
    ),
    {iolist_to_binary(lists:reverse(DataBins)), Positions}.

%% Encode ref with minimal string padding (no 4-byte alignment padding)
encode_ref_minimal_padding(string, Bin, _Defs) when is_binary(Bin) ->
    Len = byte_size(Bin),
    %% No padding - just length + data + null terminator
    <<Len:32/little, Bin/binary, 0>>;
encode_ref_minimal_padding(Type, Value, Defs) ->
    encode_ref(Type, Value, Defs).

encode_nested_table(TableType, Map, Defs) ->
    %% Build a nested table - vtable first, then table (positive soffset, like flatc)
    {table, Fields} = maps:get(TableType, Defs),
    FieldValues = collect_fields(Map, Fields, Defs),
    {Scalars, Refs} = lists:partition(
        fun({_Id, Type, _Val}) -> is_scalar_type(Type) end,
        FieldValues
    ),

    VTable = build_vtable(Scalars, Refs, 4),
    VTableSize = byte_size(VTable),

    %% Layout: vtable | soffset | table_data | ref_data
    %% soffset is positive, pointing back to vtable start
    {TableData, RefDataBin} = build_table_data(Scalars, Refs, VTableSize, Defs),

    %% Positive = backward to vtable at start
    SOffset = VTableSize,

    iolist_to_binary([
        VTable,
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ]).

%% =============================================================================
%% Scalar Encoding
%% =============================================================================

encode_scalar(Value, bool) ->
    <<
        (if
            Value -> 1;
            true -> 0
        end):8
    >>;
encode_scalar(Value, byte) ->
    <<Value:8/signed>>;
encode_scalar(Value, int8) ->
    <<Value:8/signed>>;
encode_scalar(Value, ubyte) ->
    <<Value:8/unsigned>>;
encode_scalar(Value, uint8) ->
    <<Value:8/unsigned>>;
encode_scalar(Value, short) ->
    <<Value:16/little-signed>>;
encode_scalar(Value, int16) ->
    <<Value:16/little-signed>>;
encode_scalar(Value, ushort) ->
    <<Value:16/little-unsigned>>;
encode_scalar(Value, uint16) ->
    <<Value:16/little-unsigned>>;
encode_scalar(Value, int) ->
    <<Value:32/little-signed>>;
encode_scalar(Value, int32) ->
    <<Value:32/little-signed>>;
encode_scalar(Value, uint) ->
    <<Value:32/little-unsigned>>;
encode_scalar(Value, uint32) ->
    <<Value:32/little-unsigned>>;
encode_scalar(Value, long) ->
    <<Value:64/little-signed>>;
encode_scalar(Value, int64) ->
    <<Value:64/little-signed>>;
encode_scalar(Value, ulong) ->
    <<Value:64/little-unsigned>>;
encode_scalar(Value, uint64) ->
    <<Value:64/little-unsigned>>;
encode_scalar(Value, float) ->
    <<Value:32/little-float>>;
encode_scalar(Value, float32) ->
    <<Value:32/little-float>>;
encode_scalar(Value, double) ->
    <<Value:64/little-float>>;
encode_scalar(Value, float64) ->
    <<Value:64/little-float>>;
encode_scalar(Value, {enum, Base}) ->
    encode_scalar(Value, Base);
encode_scalar(Map, {struct, Fields}) when is_map(Map) ->
    encode_struct(Map, Fields);
encode_scalar(TypeIndex, {union_type, _UnionName}) when is_integer(TypeIndex) ->
    <<TypeIndex:8/unsigned>>.

%% Encode struct as inline data
encode_struct(Map, Fields) ->
    {Bin, _} = lists:foldl(
        fun({Name, Type}, {Acc, Off}) ->
            Size = type_size(Type),
            AlignedOff = align_offset(Off, Size),
            Pad = AlignedOff - Off,
            Value = get_field_value(Map, Name),
            ValBin = encode_scalar(Value, Type),
            {<<Acc/binary, 0:(Pad * 8), ValBin/binary>>, AlignedOff + Size}
        end,
        {<<>>, 0},
        Fields
    ),
    %% Pad to struct alignment
    StructSize = calc_struct_size(Fields),
    CurrentSize = byte_size(Bin),
    TrailingPad = StructSize - CurrentSize,
    <<Bin/binary, 0:(TrailingPad * 8)>>.

%% =============================================================================
%% Helpers
%% =============================================================================

type_size(bool) -> 1;
type_size(byte) -> 1;
type_size(ubyte) -> 1;
type_size(int8) -> 1;
type_size(uint8) -> 1;
type_size(short) -> 2;
type_size(ushort) -> 2;
type_size(int16) -> 2;
type_size(uint16) -> 2;
type_size(int) -> 4;
type_size(uint) -> 4;
type_size(int32) -> 4;
type_size(uint32) -> 4;
type_size(long) -> 8;
type_size(ulong) -> 8;
type_size(int64) -> 8;
type_size(uint64) -> 8;
type_size(float) -> 4;
type_size(float32) -> 4;
type_size(double) -> 8;
type_size(float64) -> 8;
type_size({enum, Base}) -> type_size(Base);
type_size({struct, Fields}) -> calc_struct_size(Fields);
%% Union type is ubyte
type_size({union_type, _}) -> 1;
type_size(_) -> 4.

%% Calculate struct size with proper alignment
calc_struct_size(Fields) ->
    {_, EndOffset, MaxAlign} = lists:foldl(
        fun({_Name, Type}, {_, CurOffset, MaxAlignAcc}) ->
            Size = type_size(Type),
            Align = Size,
            AlignedOffset = align_offset(CurOffset, Align),
            {ok, AlignedOffset + Size, max(MaxAlignAcc, Align)}
        end,
        {ok, 0, 1},
        Fields
    ),
    align_offset(EndOffset, MaxAlign).

align_offset(Off, Align) ->
    case Off rem Align of
        0 -> Off;
        R -> Off + (Align - R)
    end.

parse_field_def({Name, Type, Attrs}) ->
    {Name, maps:get(id, Attrs), normalize_type(Type), extract_default(Type)};
parse_field_def({Name, Type}) ->
    {Name, 0, normalize_type(Type), extract_default(Type)}.

extract_default({_, D}) when is_number(D); is_boolean(D) -> D;
extract_default(_) -> undefined.

normalize_type({T, D}) when is_atom(T), is_number(D) -> T;
normalize_type({T, D}) when is_atom(T), is_boolean(D) -> T;
normalize_type(T) -> T.
