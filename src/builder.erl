-module(builder).

-export([from_map/3, from_map/4]).

-define(HEADER_SIZE, 8).

%% =============================================================================
%% Public API
%% =============================================================================

from_map(Map, Defs, RootType) ->
    from_map(Map, Defs, RootType, <<0,0,0,0>>).

from_map(Map, Defs, RootType, FileId) ->
    {table, Fields} = maps:get(RootType, Defs),
    FieldValues = collect_fields(Map, Fields, Defs),
    {Scalars, Refs} = lists:partition(
        fun({_Id, Type, _Val}) -> is_scalar_type(Type) end,
        FieldValues
    ),

    MaxAlign = case Scalars of
        [] -> 4;
        _ -> lists:max([type_size(T) || {_, T, _} <- Scalars])
    end,

    %% Layout: header | vtable | [pad] | table | ref_data
    %% Calculate sizes
    VTable = build_vtable(Scalars, Refs, MaxAlign),
    VTableSize = byte_size(VTable),

    TablePosUnaligned = ?HEADER_SIZE + VTableSize,
    TablePos = align_offset(TablePosUnaligned, max(MaxAlign, 4)),
    PreTablePad = TablePos - TablePosUnaligned,

    %% Table contains: soffset (4) + scalar fields + ref offsets
    ScalarDataSize = calc_scalar_data_size(Scalars, MaxAlign, Refs /= []),
    RefOffsetSize = length(Refs) * 4,
    TableDataSize = ScalarDataSize + RefOffsetSize,
    TableEnd = TablePos + 4 + TableDataSize,  %% +4 for soffset

    %% Reference data starts after table
    SortedRefs = lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Refs),

    %% Build scalar data
    ScalarData = build_scalar_data(Scalars, MaxAlign, Refs /= []),

    %% Calculate ref offsets and build ref data
    RefOffsetStart = TablePos + 4 + ScalarDataSize,  %% Where ref offsets are in buffer
    {RefOffsets, RefDataBin} = build_refs(SortedRefs, RefOffsetStart, TableEnd, Defs),

    %% Assemble
    SOffset = TablePos - ?HEADER_SIZE,
    FileIdBin = ensure_file_id(FileId),

    iolist_to_binary([
        <<TablePos:32/little-unsigned>>,  %% Root offset
        FileIdBin,
        VTable,
        <<0:(PreTablePad*8)>>,
        <<SOffset:32/little-signed>>,
        ScalarData,
        RefOffsets,
        RefDataBin
    ]).

%% =============================================================================
%% Field Collection
%% =============================================================================

collect_fields(Map, Fields, Defs) ->
    lists:filtermap(
        fun(FieldDef) ->
            {Name, FieldId, Type, Default} = parse_field_def(FieldDef),
            case get_field_value(Map, Name) of
                undefined -> false;
                Value when Value =:= Default -> false;
                Value -> {true, {FieldId, resolve_type(Type, Defs), Value}}
            end
        end,
        Fields
    ).

%% Look up field by atom key or binary key
get_field_value(Map, Name) when is_atom(Name) ->
    case maps:get(Name, Map, undefined) of
        undefined -> maps:get(atom_to_binary(Name), Map, undefined);
        Value -> Value
    end.

is_scalar_type({enum, _}) -> true;
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

%% Resolve type name to its definition (for enums)
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _Values} -> {enum, Base};
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

            ScalarDataSize = calc_scalar_data_size(Scalars, MaxAlign, Refs /= []),
            RefSize = length(Refs) * 4,
            TableSize = 4 + ScalarDataSize + RefSize,

            Slots = build_slots(Scalars, Refs, NumSlots, MaxAlign),

            iolist_to_binary([
                <<VTableSize:16/little, TableSize:16/little>>,
                Slots
            ])
    end.

build_slots(Scalars, Refs, NumSlots, MaxAlign) ->
    %% Scalars start after soffset (4 bytes), aligned
    FirstOffset = align_offset(4, MaxAlign),
    {ScalarSlots, ScalarEnd} = lists:foldl(
        fun({Id, Type, _}, {Acc, Off}) ->
            Size = type_size(Type),
            AlignedOff = align_offset(Off, Size),
            {Acc#{Id => AlignedOff}, AlignedOff + Size}
        end,
        {#{}, FirstOffset},
        lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Scalars)
    ),

    %% Refs come after scalars
    RefStart = align_offset(ScalarEnd, 4),
    {AllSlots, _} = lists:foldl(
        fun({Id, _, _}, {Acc, Off}) ->
            {Acc#{Id => Off}, Off + 4}
        end,
        {ScalarSlots, RefStart},
        lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Refs)
    ),

    [<<(maps:get(Id, AllSlots, 0)):16/little>> || Id <- lists:seq(0, NumSlots - 1)].

calc_scalar_data_size([], _, _HasRefs) -> 0;
calc_scalar_data_size(Scalars, MaxAlign, HasRefs) ->
    FirstOffset = align_offset(4, MaxAlign),
    EndOffset = lists:foldl(
        fun({_, Type, _}, Off) ->
            Size = type_size(Type),
            align_offset(Off, Size) + Size
        end,
        FirstOffset,
        lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Scalars)
    ),
    RawSize = EndOffset - 4,
    %% Pad to 4-byte boundary if there are refs following
    case HasRefs of
        true -> align_offset(RawSize + 4, 4) - 4;
        false -> RawSize
    end.

%% =============================================================================
%% Data Building
%% =============================================================================

build_scalar_data(Scalars, MaxAlign, HasRefs) ->
    FirstOffset = align_offset(4, MaxAlign),
    InitialPad = FirstOffset - 4,
    Sorted = lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Scalars),
    {Data, FinalOff} = lists:foldl(
        fun({_, Type, Value}, {Acc, Off}) ->
            Size = type_size(Type),
            AlignedOff = align_offset(Off, Size),
            Pad = AlignedOff - Off,
            ValBin = encode_scalar(Value, Type),
            {<<Acc/binary, 0:(Pad*8), ValBin/binary>>, AlignedOff + Size}
        end,
        {<<0:(InitialPad*8)>>, FirstOffset},
        Sorted
    ),
    %% Pad to 4-byte boundary if refs follow
    case HasRefs of
        true ->
            TrailingPad = align_offset(FinalOff, 4) - FinalOff,
            <<Data/binary, 0:(TrailingPad*8)>>;
        false ->
            Data
    end.

build_refs(SortedRefs, FirstRefOffsetPos, RefDataStart, Defs) ->
    %% For each ref, calculate offset from field position to data position
    {OffsetBins, DataBins, _} = lists:foldl(
        fun({_, Type, Value}, {OffAcc, DataAcc, DataPos}) ->
            FieldPos = FirstRefOffsetPos + (length(OffAcc) * 4),
            DataBin = encode_ref(Type, Value, Defs),
            UOffset = DataPos - FieldPos,
            {[<<UOffset:32/little-signed>> | OffAcc],
             [DataBin | DataAcc],
             DataPos + byte_size(DataBin)}
        end,
        {[], [], RefDataStart},
        SortedRefs
    ),
    {iolist_to_binary(lists:reverse(OffsetBins)),
     iolist_to_binary(lists:reverse(DataBins))}.

encode_ref(string, Bin, _Defs) when is_binary(Bin) ->
    Len = byte_size(Bin),
    %% Pad string to 4-byte boundary
    TotalLen = 4 + Len + 1,  %% length + data + null
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    <<Len:32/little, Bin/binary, 0, 0:(PadLen*8)>>;

encode_ref({vector, ElemType}, Values, Defs) when is_list(Values) ->
    encode_vector(ElemType, Values, Defs);

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
    %% First encode all the data elements
    EncodedElems = [encode_ref(ElemType, V, Defs) || V <- Values],

    %% Vector format: length (4) + offsets (4 each) + data
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Calculate offset for each element
    {Offsets, _} = lists:foldl(
        fun(ElemBin, {OffAcc, DataPos}) ->
            %% Offset from current offset position to data
            OffsetPos = 4 + (length(OffAcc) * 4),
            UOffset = DataPos - OffsetPos,
            {[<<UOffset:32/little-signed>> | OffAcc], DataPos + byte_size(ElemBin)}
        end,
        {[], HeaderSize},
        EncodedElems
    ),

    iolist_to_binary([
        <<Len:32/little>>,
        lists:reverse(Offsets),
        EncodedElems
    ]).

encode_nested_table(TableType, Map, Defs) ->
    %% Build a nested table - table first, then vtable (forward-pointing soffset)
    {table, Fields} = maps:get(TableType, Defs),
    FieldValues = collect_fields(Map, Fields, Defs),
    {Scalars, Refs} = lists:partition(
        fun({_Id, Type, _Val}) -> is_scalar_type(Type) end,
        FieldValues
    ),

    MaxAlign = case Scalars of
        [] -> 4;
        _ -> lists:max([type_size(T) || {_, T, _} <- Scalars])
    end,

    VTable = build_vtable(Scalars, Refs, MaxAlign),
    ScalarDataSize = calc_scalar_data_size(Scalars, MaxAlign, Refs /= []),
    ScalarData = build_scalar_data(Scalars, MaxAlign, Refs /= []),

    %% Layout: table | ref_data | vtable
    %% Table entry at position 0
    TableDataSize = 4 + ScalarDataSize + (length(Refs) * 4),

    SortedRefs = lists:sort(fun({A,_,_}, {B,_,_}) -> A =< B end, Refs),
    RefOffsetStart = 4 + ScalarDataSize,  %% Relative to table start
    RefDataStart = TableDataSize,  %% Ref data starts after table inline data

    %% Build ref data (includes nested tables recursively)
    {RefOffsets, RefDataBin} = build_refs(SortedRefs, RefOffsetStart, RefDataStart, Defs),
    RefDataSize = byte_size(RefDataBin),

    %% VTable comes after ref data
    VTablePos = TableDataSize + RefDataSize,
    SOffset = -VTablePos,  %% Negative = forward to vtable

    iolist_to_binary([
        <<SOffset:32/little-signed>>,
        ScalarData,
        RefOffsets,
        RefDataBin,
        VTable
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
encode_scalar(Value, {enum, Base}) -> encode_scalar(Value, Base).

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
type_size(_) -> 4.

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

ensure_file_id(undefined) -> <<0,0,0,0>>;
ensure_file_id(B) when byte_size(B) =:= 4 -> B;
ensure_file_id(_) -> error(invalid_file_id).
