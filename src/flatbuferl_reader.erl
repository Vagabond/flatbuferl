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
    read_field/4,
    read_scalar_field/4,
    read_ref_field/4,
    read_string_field/3,
    read_struct_field/4,
    read_union_type_field/3,
    read_union_value_field/3
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

%% Read vtable once for a table - returns {TableOffset, VTableSize, VTableStart, Buffer}
%% VTableStart is the absolute position in Buffer where field offsets begin (after 4-byte header)
-type vtable() :: {
    TableOffset :: non_neg_integer(),
    VTableSize :: non_neg_integer(),
    VTableStart :: non_neg_integer(),
    Buffer :: binary()
}.
-spec read_vtable(table_ref()) -> vtable().
read_vtable({table, TableOffset, Buffer}) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _/binary>> = Buffer,
    {TableOffset, VTableSize, VTableOffset + 4, Buffer}.

%% Read a non-primitive field using pre-read vtable
%% Caller (decode_fields) already dispatched primitives to read_scalar_field
-spec read_field(vtable(), field_id(), atom() | tuple(), buffer()) ->
    {ok, term()} | missing.
read_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, FieldType, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ -> read_value(Buffer, TableOffset + FieldOffset, FieldType)
            end;
        false ->
            missing
    end.

%% Fast path for scalar fields - only 11 canonical types
-spec read_scalar_field(vtable(), field_id(), atom(), buffer()) ->
    {ok, term()} | missing.
read_scalar_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, ScalarType, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ -> read_scalar(Buffer, TableOffset + FieldOffset, ScalarType)
            end;
        false ->
            missing
    end.

%% Fast path for reference fields (nested tables) - skip primitive check
-spec read_ref_field(vtable(), field_id(), atom(), buffer()) ->
    {ok, {table, non_neg_integer(), buffer()}} | missing.
read_ref_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, _Type, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, NestedOffset:32/little-unsigned, _/binary>> = Buffer,
                    {ok, {table, FieldPos + NestedOffset, Buffer}}
            end;
        false ->
            missing
    end.

%% Fast path for union type field (uint8 discriminator)
-spec read_union_type_field(vtable(), field_id(), buffer()) ->
    {ok, non_neg_integer()} | missing.
read_union_type_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
                    {ok, TypeIndex}
            end;
        false ->
            missing
    end.

%% Fast path for union value field (table offset)
-spec read_union_value_field(vtable(), field_id(), buffer()) ->
    {ok, {table, non_neg_integer(), buffer()}} | missing.
read_union_value_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, NestedOffset:32/little-unsigned, _/binary>> = Buffer,
                    {ok, {table, FieldPos + NestedOffset, Buffer}}
            end;
        false ->
            missing
    end.

%% Fast path for string fields
-spec read_string_field(vtable(), field_id(), buffer()) ->
    {ok, binary()} | missing.
read_string_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
                    StringPos = FieldPos + StringOffset,
                    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
                    {ok, StringData}
            end;
        false ->
            missing
    end.

%% Fast path for inline struct fields - reads struct data directly
-spec read_struct_field(vtable(), field_id(), #struct_def{}, buffer()) ->
    {ok, map()} | missing.
read_struct_field({TableOffset, VTableSize, VTableStart, Buffer}, FieldId, #struct_def{fields = Fields}, _) ->
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableStart + (FieldId * 2),
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 -> missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    StructMap = read_struct_fields_fast(Buffer, FieldPos, Fields, #{}),
                    {ok, StructMap}
            end;
        false ->
            missing
    end.

%% Read scalar value - only canonical types (11 clauses)
read_scalar(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value =/= 0};
read_scalar(Buffer, Pos, int8) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, uint8) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, int16) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, uint16) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, int32) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, uint32) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, int64) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, uint64) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, float32) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_scalar(Buffer, Pos, float64) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
%% Enum - delegate to underlying type
read_scalar(Buffer, Pos, #enum_resolved{base_type = BaseType}) ->
    read_scalar(Buffer, Pos, BaseType);
%% Union type - ubyte discriminator
read_scalar(Buffer, Pos, #union_type_def{}) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value}.

%% Low-level field access by ID and type
-spec get_field(table_ref(), field_id(), atom() | tuple(), buffer()) ->
    {ok, term()} | missing | {error, term()}.
get_field({table, TableOffset, Buffer}, FieldId, FieldType, _) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _/binary>> = Buffer,
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableOffset + FieldOffsetPos,
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
            case FieldOffset of
                0 ->
                    missing;
                _ ->
                    FieldPos = TableOffset + FieldOffset,
                    case is_primitive_element(FieldType) of
                        true -> read_scalar(Buffer, FieldPos, FieldType);
                        false -> read_value(Buffer, FieldPos, FieldType)
                    end
            end;
        false ->
            missing
    end.

%% Get the absolute byte offset of a field in the buffer (for mutation).
%% Returns {ok, ByteOffset} or missing if field not present.
-spec get_field_offset(table_ref(), field_id(), buffer()) ->
    {ok, non_neg_integer()} | missing.
get_field_offset({table, TableOffset, Buffer}, FieldId, _) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _/binary>> = Buffer,
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableOffset + FieldOffsetPos,
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
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
get_vector_info(TableRef, FieldId, #vector_def{element_type = ElementType}, Buffer) ->
    get_vector_info(TableRef, FieldId, {vector, ElementType}, Buffer);
get_vector_info({table, TableOffset, Buffer}, FieldId, {vector, ElementType}, _) ->
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _/binary>> = Buffer,
    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            FieldOffsetInBuffer = VTableOffset + FieldOffsetPos,
            <<_:FieldOffsetInBuffer/binary, FieldOffset:16/little-unsigned, _/binary>> = Buffer,
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
            read_value(Buffer, ElemPos, ElementType);
        false ->
            missing
    end.

%% Read a value of given type at position
%% Dispatches based on type class

%% String (offset to length-prefixed UTF-8)
read_value(Buffer, Pos, string) ->
    <<_:Pos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
    StringPos = Pos + StringOffset,
    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
    {ok, StringData};
%% === Compound types ===
%% Vector with enriched record (from schema)
read_value(Buffer, Pos, #vector_def{
    element_type = ElemType, element_size = ElemSize, is_primitive = true
}) ->
    <<_:Pos/binary, VectorOffset:32/little-unsigned, _/binary>> = Buffer,
    VectorPos = Pos + VectorOffset,
    <<_:VectorPos/binary, Length:32/little-unsigned, _/binary>> = Buffer,
    read_scalar_elements(Buffer, VectorPos + 4, Length, ElemType, ElemSize, []);
read_value(Buffer, Pos, #vector_def{
    element_type = ElemType, element_size = ElemSize, is_primitive = false
}) ->
    <<_:Pos/binary, VectorOffset:32/little-unsigned, _/binary>> = Buffer,
    VectorPos = Pos + VectorOffset,
    <<_:VectorPos/binary, Length:32/little-unsigned, _/binary>> = Buffer,
    read_compound_elements(Buffer, VectorPos + 4, Length, ElemType, ElemSize, []);
%% Enum - read underlying scalar directly
read_value(Buffer, Pos, #enum_resolved{base_type = BaseType}) ->
    read_scalar(Buffer, Pos, BaseType);
%% Struct - read inline fixed-size data (enriched record format)
read_value(Buffer, Pos, #struct_def{fields = Fields}) ->
    StructMap = read_struct_fields_fast(Buffer, Pos, Fields, #{}),
    {ok, StructMap};
%% Struct - read inline fixed-size data (raw tuple format for tests)
read_value(Buffer, Pos, {struct, Fields}) ->
    {StructMap, _Size} = read_struct_fields(Buffer, Pos, Fields, #{}),
    {ok, StructMap};
%% Array - read inline fixed-size array
read_value(Buffer, Pos, #array_def{total_size = TotalSize, as_binary = true}) ->
    <<_:Pos/binary, Bin:TotalSize/binary, _/binary>> = Buffer,
    {ok, Bin};
read_value(Buffer, Pos, #array_def{element_type = ElemType, count = Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements};
%% Union type - read the discriminator byte
read_value(Buffer, Pos, {union_type, _UnionName}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    {ok, TypeIndex};
read_value(Buffer, Pos, #union_type_def{}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    {ok, TypeIndex};
%% Union value - read offset to table (returns table ref, caller resolves type)
read_value(Buffer, Pos, {union_value, _UnionName}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {ok, {table, NestedTablePos, Buffer}};
read_value(Buffer, Pos, #union_value_def{}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {ok, {table, NestedTablePos, Buffer}};
%% Type with default value - extract just the type (used internally)
read_value(Buffer, Pos, {Type, Default}) when is_atom(Type), is_number(Default) ->
    read_scalar(Buffer, Pos, Type);
read_value(Buffer, Pos, {Type, Default}) when is_atom(Type), is_boolean(Default) ->
    read_scalar(Buffer, Pos, Type);
%% Atom types - primitives or nested tables
read_value(Buffer, Pos, Type) when is_atom(Type) ->
    case is_primitive_element(Type) of
        true ->
            read_scalar(Buffer, Pos, Type);
        false ->
            %% Nested table - return table reference for lazy access
            <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
            NestedTablePos = Pos + TableOffset,
            {ok, {table, NestedTablePos, Buffer}}
    end;
%% Unsupported type
read_value(_Buffer, _Pos, Type) ->
    {error, {unsupported_type, Type}}.

%% Fast path for primitive scalar elements - element size from schema
read_scalar_elements(_Buffer, _Pos, 0, _ElemType, _ElemSize, Acc) ->
    {ok, lists:reverse(Acc)};
read_scalar_elements(Buffer, Pos, Count, ElemType, ElemSize, Acc) ->
    Value = read_scalar_value(Buffer, Pos, ElemType),
    read_scalar_elements(Buffer, Pos + ElemSize, Count - 1, ElemType, ElemSize, [Value | Acc]).

%% Compound elements (strings, tables, structs, etc.) - element size from schema
read_compound_elements(_Buffer, _Pos, 0, _ElemType, _ElemSize, Acc) ->
    {ok, lists:reverse(Acc)};
read_compound_elements(Buffer, Pos, Count, ElemType, ElemSize, Acc) ->
    Value = read_compound_value(Buffer, Pos, ElemType),
    read_compound_elements(Buffer, Pos + ElemSize, Count - 1, ElemType, ElemSize, [Value | Acc]).

%% Read scalar vector element value - 11 canonical types only
read_scalar_value(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    Value =/= 0;
read_scalar_value(Buffer, Pos, int8) ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, uint8) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, int16) ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, uint16) ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, int32) ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, uint32) ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, int64) ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, uint64) ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, float32) ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    Value;
read_scalar_value(Buffer, Pos, float64) ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    Value;
%% Enum - delegate to underlying type
read_scalar_value(Buffer, Pos, #enum_resolved{base_type = BaseType}) ->
    read_scalar_value(Buffer, Pos, BaseType);
%% Union type - stored as uint8
read_scalar_value(Buffer, Pos, #union_type_def{}) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    Value.

%% Read compound vector element value
read_compound_value(Buffer, Pos, string) ->
    <<_:Pos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
    StringPos = Pos + StringOffset,
    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
    StringData;
read_compound_value(Buffer, Pos, #struct_def{fields = Fields}) ->
    read_struct_fields_fast(Buffer, Pos, Fields, #{});
read_compound_value(Buffer, Pos, {union_type, _UnionName}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    TypeIndex;
read_compound_value(Buffer, Pos, #union_type_def{}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    TypeIndex;
read_compound_value(Buffer, Pos, {union_value, _UnionName}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    {table, Pos + TableOffset, Buffer};
read_compound_value(Buffer, Pos, #union_value_def{}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    {table, Pos + TableOffset, Buffer};
read_compound_value(Buffer, Pos, TableName) when is_atom(TableName) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    {table, Pos + TableOffset, Buffer}.

%% Check if type is primitive scalar (canonical types + enums)
is_primitive_element(bool) -> true;
is_primitive_element(int8) -> true;
is_primitive_element(uint8) -> true;
is_primitive_element(int16) -> true;
is_primitive_element(uint16) -> true;
is_primitive_element(int32) -> true;
is_primitive_element(uint32) -> true;
is_primitive_element(int64) -> true;
is_primitive_element(uint64) -> true;
is_primitive_element(float32) -> true;
is_primitive_element(float64) -> true;
is_primitive_element(#enum_resolved{}) -> true;
is_primitive_element(_) -> false.

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
element_size(#enum_resolved{base_type = BaseType}) ->
    element_size(BaseType);
element_size(#array_def{total_size = TotalSize}) ->
    TotalSize;
element_size(#struct_def{total_size = TotalSize}) ->
    TotalSize;
element_size({struct, Fields}) ->
    {_Offsets, Size} = calc_struct_layout(Fields),
    Size;
% Union types (both record and tuple forms for compatibility with fetch/update modules)
element_size({union_type, _}) ->
    1;
element_size(#union_type_def{}) ->
    1;
element_size({union_value, _}) ->
    4;
element_size(#union_value_def{}) ->
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
read_struct_value(Buffer, Pos, #array_def{total_size = TotalSize, as_binary = true}) ->
    <<_:Pos/binary, Bin:TotalSize/binary, _/binary>> = Buffer,
    {ok, Bin};
read_struct_value(Buffer, Pos, #array_def{element_type = ElemType, count = Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements};
%% Non-canonical aliases (for tests that bypass schema parser)
read_struct_value(Buffer, Pos, byte) ->
    read_struct_value(Buffer, Pos, int8);
read_struct_value(Buffer, Pos, ubyte) ->
    read_struct_value(Buffer, Pos, uint8);
read_struct_value(Buffer, Pos, short) ->
    read_struct_value(Buffer, Pos, int16);
read_struct_value(Buffer, Pos, ushort) ->
    read_struct_value(Buffer, Pos, uint16);
read_struct_value(Buffer, Pos, int) ->
    read_struct_value(Buffer, Pos, int32);
read_struct_value(Buffer, Pos, uint) ->
    read_struct_value(Buffer, Pos, uint32);
read_struct_value(Buffer, Pos, float) ->
    read_struct_value(Buffer, Pos, float32);
read_struct_value(Buffer, Pos, long) ->
    read_struct_value(Buffer, Pos, int64);
read_struct_value(Buffer, Pos, ulong) ->
    read_struct_value(Buffer, Pos, uint64);
read_struct_value(Buffer, Pos, double) ->
    read_struct_value(Buffer, Pos, float64).

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
