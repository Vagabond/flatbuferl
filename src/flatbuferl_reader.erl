%% @private
-module(flatbuferl_reader).
-export([
    get_root/1,
    get_file_id/1,
    get_field/4,
    get_vector_info/4,
    get_vector_element_at/3,
    element_size/1
]).

-type buffer() :: binary().
-type table_ref() :: {table, Offset :: non_neg_integer(), buffer()}.
-type field_id() :: non_neg_integer().

-spec get_root(buffer()) -> table_ref().
get_root(Buffer) ->
    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    {table, RootOffset, Buffer}.

-spec get_file_id(buffer()) -> binary().
get_file_id(Buffer) ->
    <<_RootOffset:32, FileId:4/binary, _/binary>> = Buffer,
    FileId.

%% Low-level field access by ID and type
-spec get_field(table_ref(), field_id(), atom() | tuple(), buffer()) ->
    {ok, term()} | missing | {error, term()}.
get_field({table, TableOffset, Buffer}, FieldId, FieldType, _) ->
    %% Read soffset to vtable
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,

    %% Read vtable header
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _TableSize:16/little-unsigned,
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
%% 8-bit integers
read_value(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value =/= 0};
read_value(Buffer, Pos, Type) when Type == byte; Type == int8 ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, Type) when Type == ubyte; Type == uint8 ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
%% 16-bit integers
read_value(Buffer, Pos, Type) when Type == short; Type == int16 ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, Type) when Type == ushort; Type == uint16 ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
%% 32-bit integers
read_value(Buffer, Pos, Type) when Type == int; Type == int32 ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, Type) when Type == uint; Type == uint32 ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
%% 64-bit integers
read_value(Buffer, Pos, Type) when Type == long; Type == int64 ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, Type) when Type == ulong; Type == uint64 ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
%% Floating point
read_value(Buffer, Pos, Type) when Type == float; Type == float32 ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_value(Buffer, Pos, Type) when Type == double; Type == float64 ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
%% String (offset to length-prefixed UTF-8)
read_value(Buffer, Pos, string) ->
    <<_:Pos/binary, StringOffset:32/little-unsigned, _/binary>> = Buffer,
    StringPos = Pos + StringOffset,
    <<_:StringPos/binary, Length:32/little-unsigned, StringData:Length/binary, _/binary>> = Buffer,
    {ok, StringData};
%% Vector (offset to length-prefixed array)
read_value(Buffer, Pos, {vector, ElementType}) ->
    <<_:Pos/binary, VectorOffset:32/little-unsigned, _/binary>> = Buffer,
    VectorPos = Pos + VectorOffset,
    <<_:VectorPos/binary, Length:32/little-unsigned, _/binary>> = Buffer,
    ElementsStart = VectorPos + 4,
    read_vector_elements(Buffer, ElementsStart, Length, ElementType, []);
%% Enum - read as underlying type, return integer value
read_value(Buffer, Pos, {enum, UnderlyingType}) ->
    read_value(Buffer, Pos, UnderlyingType);
%% Type with default value - extract just the type
%% Defaults are only for scalar types, matched after enum
read_value(Buffer, Pos, {Type, Default}) when
    is_atom(Type), is_number(Default);
    is_atom(Type), is_boolean(Default)
->
    read_value(Buffer, Pos, Type);
%% Union type - read the discriminator byte
read_value(Buffer, Pos, {union_type, _UnionName}) ->
    <<_:Pos/binary, TypeIndex:8/little-unsigned, _/binary>> = Buffer,
    {ok, TypeIndex};
%% Union value - read offset to table (returns table ref, caller resolves type)
read_value(Buffer, Pos, {union_value, _UnionName}) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {ok, {table, NestedTablePos, Buffer}};
%% Struct - read inline fixed-size data
read_value(Buffer, Pos, {struct, Fields}) ->
    {StructMap, _Size} = read_struct_fields(Buffer, Pos, Fields, #{}),
    {ok, StructMap};
%% Array - read inline fixed-size array
read_value(Buffer, Pos, {array, ElemType, Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements};
%% Nested table - return table reference for lazy access
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

%% 8-bit elements
read_vector_element(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {1, Value =/= 0};
read_vector_element(Buffer, Pos, Type) when Type == byte; Type == int8 ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {1, Value};
read_vector_element(Buffer, Pos, Type) when Type == ubyte; Type == uint8 ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {1, Value};
%% 16-bit elements
read_vector_element(Buffer, Pos, Type) when Type == short; Type == int16 ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {2, Value};
read_vector_element(Buffer, Pos, Type) when Type == ushort; Type == uint16 ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {2, Value};
%% 32-bit elements
read_vector_element(Buffer, Pos, Type) when Type == int; Type == int32 ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {4, Value};
read_vector_element(Buffer, Pos, Type) when Type == uint; Type == uint32 ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {4, Value};
read_vector_element(Buffer, Pos, Type) when Type == float; Type == float32 ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {4, Value};
%% 64-bit elements
read_vector_element(Buffer, Pos, Type) when Type == long; Type == int64 ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {8, Value};
read_vector_element(Buffer, Pos, Type) when Type == ulong; Type == uint64 ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {8, Value};
read_vector_element(Buffer, Pos, Type) when Type == double; Type == float64 ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {8, Value};
%% Enum in vector
read_vector_element(Buffer, Pos, {enum, UnderlyingType}) ->
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
%% Struct in vector (inline data)
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
%% Table in vector (offset to table)
read_vector_element(Buffer, Pos, TableName) when is_atom(TableName) ->
    <<_:Pos/binary, TableOffset:32/little-unsigned, _/binary>> = Buffer,
    NestedTablePos = Pos + TableOffset,
    {4, {table, NestedTablePos, Buffer}}.

%% =============================================================================
%% Struct Reading
%% =============================================================================

%% Read struct fields inline - structs are fixed-size, no vtable
read_struct_fields(_Buffer, _Pos, [], Acc) ->
    {Acc, 0};
read_struct_fields(Buffer, Pos, Fields, Acc) ->
    %% Calculate struct layout with alignment
    {FieldOffsets, TotalSize} = calc_struct_layout(Fields),
    %% Read each field
    FinalAcc = lists:foldl(
        fun({{Name, Type}, Offset}, AccIn) ->
            {ok, Value} = read_struct_value(Buffer, Pos + Offset, Type),
            AccIn#{Name => Value}
        end,
        Acc,
        lists:zip(Fields, FieldOffsets)
    ),
    {FinalAcc, TotalSize}.

%% Calculate field offsets in struct with proper alignment
calc_struct_layout(Fields) ->
    {Offsets, _, MaxAlign} = lists:foldl(
        fun({_Name, Type}, {Acc, CurOffset, MaxAlignAcc}) ->
            Size = element_size(Type),
            %% In FlatBuffers, alignment equals size for scalars
            Align = Size,
            AlignedOffset = align_offset(CurOffset, Align),
            {[AlignedOffset | Acc], AlignedOffset + Size, max(MaxAlignAcc, Align)}
        end,
        {[], 0, 1},
        Fields
    ),
    ReversedOffsets = lists:reverse(Offsets),
    %% Total size aligned to max alignment
    LastOffset = lists:last(ReversedOffsets),
    {_Name, LastType} = lists:last(Fields),
    RawSize = LastOffset + element_size(LastType),
    TotalSize = align_offset(RawSize, MaxAlign),
    {ReversedOffsets, TotalSize}.

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
element_size({array, ElemType, Count}) ->
    element_size(ElemType) * Count;
element_size({struct, Fields}) ->
    {_Offsets, Size} = calc_struct_layout(Fields),
    Size;
% offset
element_size({union_type, _}) -> 1;
element_size({union_value, _}) -> 4;
element_size(TableName) when is_atom(TableName) -> 4.

%% Read a scalar value from struct (no offset indirection)
read_struct_value(Buffer, Pos, bool) ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value =/= 0};
read_struct_value(Buffer, Pos, Type) when Type == byte; Type == int8 ->
    <<_:Pos/binary, Value:8/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == ubyte; Type == uint8 ->
    <<_:Pos/binary, Value:8/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == short; Type == int16 ->
    <<_:Pos/binary, Value:16/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == ushort; Type == uint16 ->
    <<_:Pos/binary, Value:16/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == int; Type == int32 ->
    <<_:Pos/binary, Value:32/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == uint; Type == uint32 ->
    <<_:Pos/binary, Value:32/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == float; Type == float32 ->
    <<_:Pos/binary, Value:32/little-float, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == long; Type == int64 ->
    <<_:Pos/binary, Value:64/little-signed, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == ulong; Type == uint64 ->
    <<_:Pos/binary, Value:64/little-unsigned, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, Type) when Type == double; Type == float64 ->
    <<_:Pos/binary, Value:64/little-float, _/binary>> = Buffer,
    {ok, Value};
read_struct_value(Buffer, Pos, {array, ElemType, Count}) ->
    {Elements, _Size} = read_array_elements(Buffer, Pos, ElemType, Count),
    {ok, Elements}.

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
