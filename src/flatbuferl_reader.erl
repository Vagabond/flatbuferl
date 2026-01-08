%% @private
-module(flatbuferl_reader).
-include("flatbuferl_records.hrl").
-export([
    get_root/1,
    get_file_id/1,
    get_field/4,
    get_field_offset/3,
    get_vector_info/4,
    get_vector_element_at/3,
    element_size/1,
    %% Fast path - read vtable once, then read fields
    read_vtable/1,
    read_field/4
]).

-type buffer() :: binary().
-type table_ref() :: {table, Offset :: non_neg_integer(), buffer()}.
-type field_id() :: non_neg_integer().

-export_type([table_ref/0]).

-spec get_root(buffer()) -> table_ref().
get_root(<<RootOffset:32/little-unsigned, _/binary>> = Buffer) ->
    {table, RootOffset, Buffer}.

-spec get_file_id(buffer()) -> binary().
get_file_id(Buffer) ->
    <<_RootOffset:32, FileId:4/binary, _/binary>> = Buffer,
    FileId.

%% Read vtable once for a table - returns {TableOffset, VTableSize, VTableData, Buffer}
%% VTableData is the vtable bytes starting after the 4-byte header
-type vtable() :: {TableOffset :: non_neg_integer(), VTableSize :: non_neg_integer(),
                   VTableData :: binary(), Buffer :: binary()}.
-spec read_vtable(table_ref()) -> vtable().
read_vtable({table, TableOffset, Buffer}) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _TableSize:16,
      VTableData/binary>> = Buffer,
    {TableOffset, VTableSize, VTableData, Buffer}.

%% Read a field using pre-read vtable - avoids re-reading vtable per field
-spec read_field(vtable(), field_id(), atom() | tuple(), buffer()) ->
    {ok, term()} | missing.
read_field({TableOffset, VTableSize, VTableData, Buffer}, FieldId, FieldType, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInVTable = FieldOffsetPos - 4,
            <<_:FieldOffsetInVTable/binary, FieldOffset:16/little-unsigned, _/binary>> = VTableData,
            case FieldOffset of
                0 -> missing;
                _ -> read_value(Buffer, TableOffset + FieldOffset, FieldType)
            end;
        false ->
            missing
    end.

%% Low-level field access by ID and type
-spec get_field(table_ref(), field_id(), atom() | tuple(), buffer()) ->
    {ok, term()} | missing | {error, term()}.
get_field({table, TableOffset, Buffer}, FieldId, FieldType, _) ->
    %% Read soffset to vtable
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,

    %% Read vtable header
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _:16/little-unsigned,
        VTableRest/binary>> = Buffer,

    %% Calculate field offset position in vtable

    %% 4 bytes header + 2 bytes per field
    FieldOffsetPos = 4 + (FieldId * 2),

    case FieldOffsetPos < VTableSize of
        true ->
            %% Read field offset from vtable

            %% Offset into VTableRest
            FieldOffsetInVTable = FieldOffsetPos - 4,
            <<_:FieldOffsetInVTable/binary, FieldOffset:16/little-unsigned, _/binary>> = VTableRest,

            case FieldOffset of
                0 ->
                    %% Field not present, use default
                    missing;
                _ ->
                    %% Field is present at TableOffset + FieldOffset
                    FieldPos = TableOffset + FieldOffset,
                    read_value(Buffer, FieldPos, FieldType)
            end;
        false ->
            %% Field ID beyond vtable - field not present
            missing
    end.

%% Get the absolute byte offset of a field in the buffer (for mutation).
%% Returns {ok, ByteOffset} or missing if field not present.
-spec get_field_offset(table_ref(), field_id(), buffer()) ->
    {ok, non_neg_integer()} | missing.
get_field_offset({table, TableOffset, Buffer}, FieldId, _) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _TableSize:16/little-unsigned,
        VTableRest/binary>> = Buffer,
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInVTable = FieldOffsetPos - 4,
            <<_:FieldOffsetInVTable/binary, FieldOffset:16/little-unsigned, _/binary>> = VTableRest,
            case FieldOffset of
                0 -> missing;
                _ -> {ok, TableOffset + FieldOffset}
            end;
        false ->
            missing
    end.

%% Get vector metadata without reading elements.
%% Returns {ok, {Length, ElementsStart, ElementType}} or missing.
-spec get_vector_info(table_ref(), field_id(), tuple(), buffer()) ->
    {ok, {non_neg_integer(), non_neg_integer(), term()}} | missing.
get_vector_info({table, TableOffset, Buffer}, FieldId, {vector, ElementType}, _) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _TableSize:16/little-unsigned,
        VTableRest/binary>> = Buffer,
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInVTable = FieldOffsetPos - 4,
            <<_:FieldOffsetInVTable/binary, FieldOffset:16/little-unsigned, _/binary>> = VTableRest,
            case FieldOffset of
                0 ->
                    missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, VectorOffset:32/little-unsigned, _/binary>> = Buffer,
                    VectorPos = FieldPos + VectorOffset,
                    <<_:VectorPos/binary, Length:32/little-unsigned, _/binary>> = Buffer,
                    ElementsStart = VectorPos + 4,
                    {ok, {Length, ElementsStart, ElementType}}
            end;
        false ->
            missing
    end.

%% Get a single vector element by index.
%% VectorInfo is {Length, ElementsStart, ElementType} from get_vector_info.
-spec get_vector_element_at({non_neg_integer(), non_neg_integer(), term()}, integer(), buffer()) ->
    {ok, term()} | missing.
get_vector_element_at({Length, ElementsStart, ElementType}, Index, Buffer) ->
    ActualIndex =
        case Index < 0 of
            true -> Length + Index;
            false -> Index
        end,
    case ActualIndex >= 0 andalso ActualIndex < Length of
        true ->
            ElemSize = element_size(ElementType),
            ElemPos = ElementsStart + (ActualIndex * ElemSize),
            {_Size, Value} = read_vector_element(Buffer, ElemPos, ElementType),
            {ok, Value};
        false ->
            missing
    end.

%% Read a value of given type at position
%% Types are normalized to canonical forms at schema parse time for fast matching
%% Canonical types are listed first, aliases at end for backward compatibility
%%
%% === Canonical scalar types (used by parsed schemas) ===
read_value(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value =/= 0};
read_value(Buffer, Pos, int8) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, uint8) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, int16) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, uint16) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, int32) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, uint32) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, int64) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, uint64) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, float32) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, float64) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
%% String (offset to length-prefixed UTF-8)
read_value(Buffer, Pos, string) ->
    <<_:Pos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
    StringPos = Pos + StringOffset,
    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
    {ok, StringData};
%% === Compound types ===
%% Vector (offset to length-prefixed array)
read_value(Buffer, Pos, {vector, ElementType}) ->
    <<_:Pos/binary, VectorOffset:32/little-unsigned, _/binary>> = Buffer,
    VectorPos = Pos + VectorOffset,
    <<_:VectorPos/binary, Length:32/little-unsigned, _/binary>> = Buffer,
    ElementsStart = VectorPos + 4,
    read_vector_elements(Buffer, ElementsStart, Length, ElementType, []);
%% Enum - read as underlying type, return integer value
read_value(Buffer, Pos, {enum, UnderlyingType, _IndexMap}) ->
    read_value(Buffer, Pos, UnderlyingType);
read_value(Buffer, Pos, {enum, UnderlyingType}) ->
    read_value(Buffer, Pos, UnderlyingType);
%% Struct - read inline fixed-size data (enriched record format)
read_value(Buffer, Pos, #struct_def{fields = Fields}) ->
    StructMap = read_struct_fields_fast(Buffer, Pos, Fields, #{}),
    {ok, StructMap};
%% Struct - read inline fixed-size data (raw tuple format for tests)
read_value(Buffer, Pos, {struct, Fields}) ->
    {StructMap, _Size} = read_struct_fields(Buffer, Pos, Fields, #{}),
    {ok, StructMap};
%% Array - read inline fixed-size array
read_value(Buffer, Pos, {array, ElemType, Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements};
%% Union type - read the discriminator byte
read_value(Buffer, Pos, {union_type, _UnionName}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    {ok, TypeIndex};
%% Union value - read offset to table (returns table ref, caller resolves type)
read_value(Buffer, Pos, {union_value, _UnionName}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {ok, {table, NestedTablePos, Buffer}};
%% Type with default value - extract just the type (used internally)
read_value(Buffer, Pos, {Type, Default}) when is_atom(Type), is_number(Default) ->
    read_value(Buffer, Pos, Type);
read_value(Buffer, Pos, {Type, Default}) when is_atom(Type), is_boolean(Default) ->
    read_value(Buffer, Pos, Type);
%% === Aliases (for backward compatibility with direct API usage) ===
%% Must come before the catch-all atom clause below
read_value(Buffer, Pos, byte) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, ubyte) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, short) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, ushort) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, int) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, uint) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, long) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, ulong) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, float) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, double) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
%% Nested table - return table reference for lazy access (catch-all for atoms)
read_value(Buffer, Pos, TableName) when is_atom(TableName) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {ok, {table, NestedTablePos, Buffer}};
%% Unsupported type
read_value(_Buffer, _Pos, Type) ->
    {error, {unsupported_type, Type}}.

%% Read vector elements
read_vector_elements(_Buffer, _Pos, 0, _ElementType, Acc) ->
    {ok, lists:reverse(Acc)};
read_vector_elements(Buffer, Pos, Count, ElementType, Acc) ->
    {ElementSize, Value} = read_vector_element(Buffer, Pos, ElementType),
    read_vector_elements(Buffer, Pos + ElementSize, Count - 1, ElementType, [Value | Acc]).

%% 8-bit elements (canonical types first)
read_vector_element(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {1, Value =/= 0};
read_vector_element(Buffer, Pos, int8) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {1, Value};
read_vector_element(Buffer, Pos, uint8) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {1, Value};
%% 16-bit elements (canonical types first)
read_vector_element(Buffer, Pos, int16) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {2, Value};
read_vector_element(Buffer, Pos, uint16) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {2, Value};
%% 32-bit elements (canonical types first)
read_vector_element(Buffer, Pos, int32) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {4, Value};
read_vector_element(Buffer, Pos, uint32) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {4, Value};
read_vector_element(Buffer, Pos, float32) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {4, Value};
%% 64-bit elements (canonical types first)
read_vector_element(Buffer, Pos, int64) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {8, Value};
read_vector_element(Buffer, Pos, uint64) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {8, Value};
read_vector_element(Buffer, Pos, float64) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {8, Value};
%% Enum in vector
read_vector_element(Buffer, Pos, {enum, UnderlyingType}) ->
    read_vector_element(Buffer, Pos, UnderlyingType);
read_vector_element(Buffer, Pos, {enum, UnderlyingType, _Values}) ->
    read_vector_element(Buffer, Pos, UnderlyingType);
%% Type with default value in vector - extract just the type
read_vector_element(Buffer, Pos, {Type, Default}) when
    is_atom(Type), is_number(Default);
    is_atom(Type), is_boolean(Default)
->
    read_vector_element(Buffer, Pos, Type);
%% String in vector (offset to length-prefixed data)
read_vector_element(Buffer, Pos, string) ->
    <<_:Pos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
    StringPos = Pos + StringOffset,
    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
    {4, StringData};
%% Struct in vector (inline data - enriched record format)
read_vector_element(Buffer, Pos, #struct_def{fields = Fields, total_size = TotalSize}) ->
    StructMap = read_struct_fields_fast(Buffer, Pos, Fields, #{}),
    {TotalSize, StructMap};
%% Struct in vector (inline data - raw tuple format)
read_vector_element(Buffer, Pos, {struct, Fields}) ->
    {StructMap, Size} = read_struct_fields(Buffer, Pos, Fields, #{}),
    {Size, StructMap};
%% Array in vector (inline data)
read_vector_element(Buffer, Pos, {array, ElemType, Count}) ->
    {Elements, Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {Size, Elements};
%% Generic table reference in vector (for union vectors where type is determined separately)
read_vector_element(Buffer, Pos, table) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {4, {table, NestedTablePos, Buffer}};
%% Union type in vector (1-byte discriminator)
read_vector_element(Buffer, Pos, {union_type, _UnionName}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    {1, TypeIndex};
%% Union value in vector (offset to table)
read_vector_element(Buffer, Pos, {union_value, _UnionName}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {4, {table, NestedTablePos, Buffer}};
%% Non-canonical aliases (for tests that bypass schema parser)
read_vector_element(Buffer, Pos, byte) ->
    read_vector_element(Buffer, Pos, int8);
read_vector_element(Buffer, Pos, ubyte) ->
    read_vector_element(Buffer, Pos, uint8);
read_vector_element(Buffer, Pos, short) ->
    read_vector_element(Buffer, Pos, int16);
read_vector_element(Buffer, Pos, ushort) ->
    read_vector_element(Buffer, Pos, uint16);
read_vector_element(Buffer, Pos, int) ->
    read_vector_element(Buffer, Pos, int32);
read_vector_element(Buffer, Pos, uint) ->
    read_vector_element(Buffer, Pos, uint32);
read_vector_element(Buffer, Pos, float) ->
    read_vector_element(Buffer, Pos, float32);
read_vector_element(Buffer, Pos, long) ->
    read_vector_element(Buffer, Pos, int64);
read_vector_element(Buffer, Pos, ulong) ->
    read_vector_element(Buffer, Pos, uint64);
read_vector_element(Buffer, Pos, double) ->
    read_vector_element(Buffer, Pos, float64);
%% Table in vector (offset to table)
read_vector_element(Buffer, Pos, TableName) when is_atom(TableName) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {4, {table, NestedTablePos, Buffer}}.

%% =============================================================================
%% Struct Reading
%% =============================================================================

%% Read struct fields inline - fast path using precomputed offsets from #struct_def{}
read_struct_fields_fast(_Buffer, _Pos, [], Acc) ->
    Acc;
read_struct_fields_fast(Buffer, Pos, [#{name := Name, type := Type, offset := Offset} | Rest], Acc) ->
    {ok, Value} = read_struct_value(Buffer, Pos + Offset, Type),
    read_struct_fields_fast(Buffer, Pos, Rest, Acc#{Name => Value}).

%% Read struct fields inline - structs are fixed-size, no vtable
%% Handles both enriched format (maps) and raw tuple format
read_struct_fields(_Buffer, _Pos, [], Acc) ->
    {Acc, 0};
read_struct_fields(Buffer, Pos, Fields, Acc) ->
    %% Calculate struct layout with alignment
    {FieldOffsets, TotalSize} = calc_struct_layout(Fields),
    %% Read each field
    FinalAcc = lists:foldl(
        fun
            ({#{name := Name, type := Type}, Offset}, AccIn) ->
                %% Enriched format
                {ok, Value} = read_struct_value(Buffer, Pos + Offset, Type),
                AccIn#{Name => Value};
            ({{Name, Type}, Offset}, AccIn) ->
                %% Raw tuple format
                {ok, Value} = read_struct_value(Buffer, Pos + Offset, Type),
                AccIn#{Name => Value}
        end,
        Acc,
        lists:zip(Fields, FieldOffsets)
    ),
    {FinalAcc, TotalSize}.

%% Calculate field offsets in struct with proper alignment
%% Handles both enriched format (maps with precomputed offsets) and raw tuple format
calc_struct_layout(Fields) ->
    %% Check if fields are enriched (first field is a map)
    case Fields of
        [#{offset := _} | _] ->
            %% Enriched format - use precomputed offsets
            Offsets = [Offset || #{offset := Offset} <- Fields],
            LastField = lists:last(Fields),
            #{offset := LastOffset, size := LastSize} = LastField,
            MaxAlign = lists:max([Size || #{size := Size} <- Fields]),
            RawSize = LastOffset + LastSize,
            TotalSize = align_offset(RawSize, MaxAlign),
            {Offsets, TotalSize};
        _ ->
            %% Raw tuple format
            {OffsetsRev, _, MaxAlign} = lists:foldl(
                fun({_Name, Type}, {Acc, CurOffset, MaxAlignAcc}) ->
                    Size = element_size(Type),
                    Align = Size,
                    AlignedOffset = align_offset(CurOffset, Align),
                    {[AlignedOffset | Acc], AlignedOffset + Size, max(MaxAlignAcc, Align)}
                end,
                {[], 0, 1},
                Fields
            ),
            ReversedOffsets = lists:reverse(OffsetsRev),
            LastOffset = lists:last(ReversedOffsets),
            {_Name, LastType} = lists:last(Fields),
            RawSize = LastOffset + element_size(LastType),
            TotalSize = align_offset(RawSize, MaxAlign),
            {ReversedOffsets, TotalSize}
    end.

align_offset(Offset, Align) ->
    case Offset rem Align of
        0 -> Offset;
        Rem -> Offset + (Align - Rem)
    end.

element_size(bool) ->
    1;
element_size(byte) ->
    1;
element_size(ubyte) ->
    1;
element_size(int8) ->
    1;
element_size(uint8) ->
    1;
element_size(short) ->
    2;
element_size(ushort) ->
    2;
element_size(int16) ->
    2;
element_size(uint16) ->
    2;
element_size(int) ->
    4;
element_size(uint) ->
    4;
element_size(int32) ->
    4;
element_size(uint32) ->
    4;
element_size(float) ->
    4;
element_size(float32) ->
    4;
% offset
element_size(string) ->
    4;
element_size(long) ->
    8;
element_size(ulong) ->
    8;
element_size(int64) ->
    8;
element_size(uint64) ->
    8;
element_size(double) ->
    8;
element_size(float64) ->
    8;
element_size({enum, UnderlyingType}) ->
    element_size(UnderlyingType);
element_size({enum, UnderlyingType, _Values}) ->
    element_size(UnderlyingType);
element_size({array, ElemType, Count}) ->
    element_size(ElemType) * Count;
element_size(#struct_def{total_size = TotalSize}) ->
    TotalSize;
element_size({struct, Fields}) ->
    {_Offsets, Size} = calc_struct_layout(Fields),
    Size;
% offset
element_size({union_type, _}) ->
    1;
element_size({union_value, _}) ->
    4;
element_size(TableName) when is_atom(TableName) -> 4.

%% Read a scalar value from struct (no offset indirection)
%% Canonical types first for fast matching
read_struct_value(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value =/= 0};
read_struct_value(Buffer, Pos, int8) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, uint8) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, int16) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, uint16) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, int32) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, uint32) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, float32) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, int64) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, uint64) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, float64) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
%% Compound types
read_struct_value(Buffer, Pos, {array, ElemType, Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements};
%% Non-canonical aliases (for tests that bypass schema parser)
read_struct_value(Buffer, Pos, byte) -> read_struct_value(Buffer, Pos, int8);
read_struct_value(Buffer, Pos, ubyte) -> read_struct_value(Buffer, Pos, uint8);
read_struct_value(Buffer, Pos, short) -> read_struct_value(Buffer, Pos, int16);
read_struct_value(Buffer, Pos, ushort) -> read_struct_value(Buffer, Pos, uint16);
read_struct_value(Buffer, Pos, int) -> read_struct_value(Buffer, Pos, int32);
read_struct_value(Buffer, Pos, uint) -> read_struct_value(Buffer, Pos, uint32);
read_struct_value(Buffer, Pos, float) -> read_struct_value(Buffer, Pos, float32);
read_struct_value(Buffer, Pos, long) -> read_struct_value(Buffer, Pos, int64);
read_struct_value(Buffer, Pos, ulong) -> read_struct_value(Buffer, Pos, uint64);
read_struct_value(Buffer, Pos, double) -> read_struct_value(Buffer, Pos, float64).

%% =============================================================================
%% Array Reading
%% =============================================================================

%% Read fixed-size array elements inline
read_array_elements(Buffer, Pos, ElemType, Count) ->
    ElemSize = element_size(ElemType),
    Elements = read_array_elements_loop(Buffer, Pos, ElemType, ElemSize, Count, []),
    {Elements, ElemSize * Count}.

read_array_elements_loop(_Buffer, _Pos, _ElemType, _ElemSize, 0, Acc) ->
    lists:reverse(Acc);
read_array_elements_loop(Buffer, Pos, ElemType, ElemSize, Count, Acc) ->
    {ok, Value} = read_struct_value(Buffer, Pos, ElemType),
    read_array_elements_loop(Buffer, Pos + ElemSize, ElemType, ElemSize, Count - 1, [Value | Acc]).
