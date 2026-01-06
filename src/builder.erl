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
    HeaderSize = case FileId of
        no_file_id -> 4;
        _ -> 8
    end,

    %% Calculate table size including ref padding
    TableSizeWithPadding = calc_table_size_with_padding(Scalars, Refs, HeaderSize, Defs),

    %% Layout: header | [pre-vtable-pad] | vtable | table | ref_data
    VTable = build_vtable_with_size(Scalars, Refs, 4, TableSizeWithPadding),
    VTableSize = byte_size(VTable),

    %% Calculate table position (4-byte aligned, plus adjustment for 8-byte fields)
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos4 = align_offset(TablePosUnaligned, 4),

    %% If table has 8-byte fields, ensure they're 8-byte aligned in the buffer
    First8ByteOffset = find_first_8byte_field_offset(Scalars ++ Refs, TableSizeWithPadding),
    TablePos = case First8ByteOffset of
        none -> TablePos4;
        Offset ->
            case (TablePos4 + Offset) rem 8 of
                0 -> TablePos4;
                _ -> TablePos4 + 4
            end
    end,
    PreVTablePad = TablePos - VTableSize - HeaderSize,

    %% Build table data
    {TableData, RefDataBin} = build_table_data(Scalars, Refs, TablePos, Defs),

    %% soffset points back to vtable
    VTablePos = HeaderSize + PreVTablePad,
    SOffset = TablePos - VTablePos,

    iolist_to_binary([
        <<TablePos:32/little-unsigned>>,
        file_id_bin(FileId),
        <<0:(PreVTablePad*8)>>,
        VTable,
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ]).

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
                        undefined -> [];
                        MemberType when is_atom(MemberType) ->
                            {union, Members} = maps:get(UnionName, Defs),
                            TypeIndex = find_union_index(MemberType, Members, 1),
                            [{FieldId, {union_type, UnionName}, TypeIndex}];
                        MemberType when is_binary(MemberType) ->
                            {union, Members} = maps:get(UnionName, Defs),
                            TypeIndex = find_union_index(binary_to_atom(MemberType), Members, 1),
                            [{FieldId, {union_type, UnionName}, TypeIndex}];
                        _ -> []
                    end;
                {union_value, UnionName} ->
                    %% Union value field - the map is the table value directly
                    %% Get the type from the corresponding _type field
                    TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
                    case get_field_value(Map, Name) of
                        undefined -> [];
                        TableValue when is_map(TableValue) ->
                            MemberType = case get_field_value(Map, TypeFieldName) of
                                T when is_atom(T) -> T;
                                T when is_binary(T) -> binary_to_atom(T);
                                _ -> error({missing_union_type_field, TypeFieldName})
                            end,
                            [{FieldId, {union_value, UnionName}, #{type => MemberType, value => TableValue}}];
                        _ -> []
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
find_union_index(Type, [{Type, _Val} | _], Index) -> Index;  %% Handle enum with explicit value
find_union_index(Type, [_ | Rest], Index) -> find_union_index(Type, Rest, Index + 1);
find_union_index(_Type, [], _Index) -> 0.  %% NONE

%% Look up field by atom key or binary key
get_field_value(Map, Name) when is_atom(Name) ->
    case maps:get(Name, Map, undefined) of
        undefined -> maps:get(atom_to_binary(Name), Map, undefined);
        Value -> Value
    end.

is_scalar_type({enum, _}) -> true;
is_scalar_type({struct, _}) -> true;  %% Structs are inline fixed-size data
is_scalar_type({union_type, _}) -> true;  %% Union type field is ubyte
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
    NumSlots = case AllFields of
        [] -> 0;
        _ -> lists:max([Id || {Id, _, _} <- AllFields]) + 1
    end,
    VTableSize = 4 + (NumSlots * 2),
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos = align_offset(TablePosUnaligned, 4),

    %% Extract refs in reverse field ID order to find first ref
    RefFields = [{Id, Type, Value} || {Id, Type, Value} <- AllFields, not is_scalar_type(Type)],
    RefFieldsByIdDesc = lists:sort(fun({IdA,_,_}, {IdB,_,_}) -> IdA >= IdB end, RefFields),

    %% Calculate ref padding
    RefPadding = calc_ref_padding_for_refs(RefFieldsByIdDesc, TablePos + BaseTableSize, Defs),
    BaseTableSize + RefPadding.

%% Find the offset of the first 8-byte field using backward placement
find_first_8byte_field_offset(Fields, TableSize) ->
    AllFields = lists:sort(fun field_layout_order/2, Fields),
    Slots = place_fields_backward(AllFields, TableSize),
    %% Find 8-byte fields and get their offsets
    EightByteFields = [{Id, Type} || {Id, Type, _} <- AllFields, type_size(Type) =:= 8],
    case EightByteFields of
        [] -> none;
        _ ->
            Offsets = [maps:get(Id, Slots) || {Id, _} <- EightByteFields],
            lists:min(Offsets)  %% Return smallest offset (first in table)
    end.

%% Calculate ref padding for field list (without offsets)
%% Ensure nested table's SOFFSET is 4-byte aligned (not just vtable start)
%% Position of soffset = Pos + VTableSize, which must be 4-byte aligned
calc_ref_padding_for_refs([], _Pos, _Defs) -> 0;
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
        _ -> 0
    end;
calc_ref_padding_for_refs([{_, {union_value, UnionName}, #{type := MemberType, value := Value}} | _], Pos, Defs) ->
    {union, _Members} = maps:get(UnionName, Defs),
    calc_ref_padding_for_refs([{0, MemberType, Value}], Pos, Defs);
calc_ref_padding_for_refs(_, _Pos, _Defs) -> 0.

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

max_field_align([]) -> 1;
max_field_align(Fields) ->
    lists:max([min(field_inline_size(Type), 4) || {_, Type, _} <- Fields]).

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
field_inline_size(_) -> 4.  %% Default for unknown refs

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
    SortedLayout = lists:sort(fun({_,_,_,A}, {_,_,_,B}) -> A =< B end, FieldLayout),

    %% Extract refs in reverse field ID order (like flatc)
    RefFields = [{Id, Type, Value, Off} || {Id, Type, Value, Off} <- SortedLayout,
                                            not is_scalar_type(Type)],
    RefFieldsByIdDesc = lists:sort(fun({IdA,_,_,_}, {IdB,_,_,_}) -> IdA >= IdB end, RefFields),

    %% Calculate padding needed before ref data for nested table alignment
    RefPadding = calc_ref_padding(RefFieldsByIdDesc, TablePos + BaseTableSize, Defs),
    TableSize = BaseTableSize + RefPadding,
    RefDataStart = TablePos + TableSize,

    {RefDataBin, RefPositions} = build_ref_data(RefFieldsByIdDesc, RefDataStart, Defs),

    %% Build table inline data with padding
    TableData = build_inline_data(SortedLayout, TablePos, RefPositions),
    PadBin = <<0:(RefPadding*8)>>,

    {<<TableData/binary, PadBin/binary>>, RefDataBin}.

%% Calculate padding needed before ref data
%% Ensure nested table's SOFFSET is 4-byte aligned (Pos + VTableSize must be 4-byte aligned)
calc_ref_padding([], _Pos, _Defs) -> 0;
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
        _ -> 0
    end;
calc_ref_padding([{_, {union_value, UnionName}, #{type := MemberType, value := Value}, _} | _], Pos, Defs) ->
    {union, _Members} = maps:get(UnionName, Defs),
    calc_ref_padding([{0, MemberType, Value, 0}], Pos, Defs);
calc_ref_padding(_, _Pos, _Defs) -> 0.

%% Build ref data and return map of field_offset -> data_position
%% For nested tables, position points to soffset, not vtable start
build_ref_data(RefFields, RefDataStart, Defs) ->
    {DataBins, Positions, _} = lists:foldl(
        fun({_Id, Type, Value, FieldOff}, {DataAcc, PosAcc, DataPos}) ->
            DataBin = encode_ref(Type, Value, Defs),
            %% For nested tables, uoffset should point to soffset (after vtable)
            RefTargetPos = case is_nested_table_type(Type, Value, Defs) of
                {true, VTableSize} -> DataPos + VTableSize;
                false -> DataPos
            end,
            {[DataBin | DataAcc],
             PosAcc#{FieldOff => RefTargetPos},
             DataPos + byte_size(DataBin)}
        end,
        {[], #{}, RefDataStart},
        RefFields
    ),
    {iolist_to_binary(lists:reverse(DataBins)), Positions}.

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
        _ -> false
    end;
is_nested_table_type({union_value, UnionName}, #{type := MemberType, value := Value}, Defs) ->
    %% Union value - check the member type
    {union, _Members} = maps:get(UnionName, Defs),
    is_nested_table_type(MemberType, Value, Defs);
is_nested_table_type(_, _, _) -> false.

%% Calculate vtable size for a table
calc_vtable_size(Scalars, Refs) ->
    AllFields = Scalars ++ Refs,
    case AllFields of
        [] -> 4;
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
            PadBin = <<0:(Pad*8)>>,

            %% Encode the field
            FieldBin = case is_scalar_type(Type) of
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
        {<<>>, 4},  %% Start at offset 4
        FieldLayout
    ),
    Data.

%% Encode ref with string caching for deduplication
encode_ref_cached(string, Bin, _Defs, DataPos, Cache) when is_binary(Bin) ->
    case maps:get(Bin, Cache, undefined) of
        undefined ->
            DataBin = encode_string(Bin),
            {new, DataBin, Cache#{Bin => DataPos}};
        CachedPos ->
            {cached, CachedPos, Cache}
    end;
encode_ref_cached(Type, Value, Defs, _DataPos, Cache) ->
    %% Non-string refs don't use caching (yet)
    DataBin = encode_ref(Type, Value, Defs),
    {new, DataBin, Cache}.

encode_string(Bin) ->
    Len = byte_size(Bin),
    TotalLen = 4 + Len + 1,
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    <<Len:32/little, Bin/binary, 0, 0:(PadLen*8)>>.

encode_ref(string, Bin, _Defs) when is_binary(Bin) ->
    Len = byte_size(Bin),
    %% Pad string to 4-byte boundary
    TotalLen = 4 + Len + 1,  %% length + data + null
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    <<Len:32/little, Bin/binary, 0, 0:(PadLen*8)>>;

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
            <<Len:32/little, ElementsBin/binary, 0:(PadLen*8)>>;
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
    %% Vector format: length (4) + offsets (4 each) + data (in reverse order like flatc)
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Encode elements in reverse order (like flatc does)
    ReversedValues = lists:reverse(Values),
    EncodedElems = [{encode_ref(ElemType, V, Defs), V} || V <- ReversedValues],

    %% Calculate data positions and vtable offsets (reverse order)
    {_, ElemPositions} = lists:foldl(
        fun({ElemBin, Value}, {DataPos, PosAcc}) ->
            %% For table elements, uoffset should point to soffset (after vtable)
            VTableOffset = case is_nested_table_type(ElemType, Value, Defs) of
                {true, VTableSize} -> VTableSize;
                false -> 0
            end,
            {DataPos + byte_size(ElemBin), [{DataPos, VTableOffset, ElemBin} | PosAcc]}
        end,
        {HeaderSize, []},
        EncodedElems
    ),
    %% ElemPositions is now in original order (because we reversed twice)

    %% Build offsets - for tables, point to soffset not vtable
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

    SOffset = VTableSize,  %% Positive = backward to vtable at start

    iolist_to_binary([
        VTable,
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ]).

%% =============================================================================
%% Scalar Encoding
%% =============================================================================

encode_scalar(Value, bool) -> <<(if Value -> 1; true -> 0 end):8>>;
encode_scalar(Value, byte) -> <<Value:8/signed>>;
encode_scalar(Value, int8) -> <<Value:8/signed>>;
encode_scalar(Value, ubyte) -> <<Value:8/unsigned>>;
encode_scalar(Value, uint8) -> <<Value:8/unsigned>>;
encode_scalar(Value, short) -> <<Value:16/little-signed>>;
encode_scalar(Value, int16) -> <<Value:16/little-signed>>;
encode_scalar(Value, ushort) -> <<Value:16/little-unsigned>>;
encode_scalar(Value, uint16) -> <<Value:16/little-unsigned>>;
encode_scalar(Value, int) -> <<Value:32/little-signed>>;
encode_scalar(Value, int32) -> <<Value:32/little-signed>>;
encode_scalar(Value, uint) -> <<Value:32/little-unsigned>>;
encode_scalar(Value, uint32) -> <<Value:32/little-unsigned>>;
encode_scalar(Value, long) -> <<Value:64/little-signed>>;
encode_scalar(Value, int64) -> <<Value:64/little-signed>>;
encode_scalar(Value, ulong) -> <<Value:64/little-unsigned>>;
encode_scalar(Value, uint64) -> <<Value:64/little-unsigned>>;
encode_scalar(Value, float) -> <<Value:32/little-float>>;
encode_scalar(Value, float32) -> <<Value:32/little-float>>;
encode_scalar(Value, double) -> <<Value:64/little-float>>;
encode_scalar(Value, float64) -> <<Value:64/little-float>>;
encode_scalar(Value, {enum, Base}) -> encode_scalar(Value, Base);
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
            {<<Acc/binary, 0:(Pad*8), ValBin/binary>>, AlignedOff + Size}
        end,
        {<<>>, 0},
        Fields
    ),
    %% Pad to struct alignment
    StructSize = calc_struct_size(Fields),
    CurrentSize = byte_size(Bin),
    TrailingPad = StructSize - CurrentSize,
    <<Bin/binary, 0:(TrailingPad*8)>>.

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
type_size({union_type, _}) -> 1;  %% Union type is ubyte
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
