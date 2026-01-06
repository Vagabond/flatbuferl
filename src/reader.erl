-module(reader).
-export([
    get_root/1,
    get_file_id/1,
    get/3,
    get_field/4
]).

%% Type definitions for clarity
-type buffer() :: binary().
-type table_ref() :: {table, Offset :: non_neg_integer(), buffer()}.
-type schema() :: map().
-type field_id() :: non_neg_integer().
-type path() :: [atom()].

%% Get root table reference from buffer
-spec get_root(buffer()) -> table_ref().
get_root(Buffer) ->
    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    {table, RootOffset, Buffer}.

%% Get file identifier (4 bytes after root offset)
-spec get_file_id(buffer()) -> binary().
get_file_id(Buffer) ->
    <<_RootOffset:32, FileId:4/binary, _/binary>> = Buffer,
    FileId.

%% High-level path-based access
-spec get(table_ref(), schema(), path()) -> {ok, term()} | missing | {error, term()}.
get(TableRef, Schema, [FieldName]) ->
    get_field_by_name(TableRef, Schema, FieldName);
get(TableRef, Schema, [FieldName | Rest]) ->
    case get_field_by_name(TableRef, Schema, FieldName) of
        {ok, NestedTableRef} when element(1, NestedTableRef) == table ->
            %% Nested path access not supported via reader:get/3
            %% Use flatbuferl:get/2 for nested paths

            %% Suppress unused warnings
            _ = {NestedTableRef, Rest},
            {error, {unknown_nested_type, FieldName}};
        {ok, _Other} ->
            {error, {not_a_table, FieldName}};
        missing ->
            missing;
        {error, _} = Err ->
            Err
    end.

%% Get field by name using schema
get_field_by_name({table, TableOffset, Buffer}, Schema, FieldName) ->
    case find_field_in_schema(Schema, FieldName) of
        {ok, FieldId, FieldType} ->
            get_field({table, TableOffset, Buffer}, FieldId, FieldType, Buffer);
        error ->
            {error, {unknown_field, FieldName}}
    end.

find_field_in_schema({table, Fields}, FieldName) ->
    find_field_in_list(Fields, FieldName);
find_field_in_schema(#{} = Defs, FieldName) ->
    %% Schema is full definitions map, need table name context
    %% For now just search all tables
    maps:fold(
        fun
            (_Name, {table, Fields}, error) ->
                find_field_in_list(Fields, FieldName);
            (_Name, _Def, Acc) ->
                Acc
        end,
        error,
        Defs
    ).

find_field_in_list([], _FieldName) ->
    error;
find_field_in_list([{Name, Type, Attrs} | _Rest], Name) ->
    {ok, maps:get(id, Attrs), Type};
find_field_in_list([{Name, Type} | _Rest], Name) ->
    %% No attrs, shouldn't happen after processing
    {ok, 0, Type};
find_field_in_list([_ | Rest], FieldName) ->
    find_field_in_list(Rest, FieldName).

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
%% Generic table reference in vector (for union vectors where type is determined separately)
read_vector_element(Buffer, Pos, table) ->
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
            Size = scalar_size(Type),
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
    RawSize = LastOffset + scalar_size(LastType),
    TotalSize = align_offset(RawSize, MaxAlign),
    {ReversedOffsets, TotalSize}.

align_offset(Offset, Align) ->
    case Offset rem Align of
        0 -> Offset;
        Rem -> Offset + (Align - Rem)
    end.

scalar_size(bool) -> 1;
scalar_size(byte) -> 1;
scalar_size(ubyte) -> 1;
scalar_size(int8) -> 1;
scalar_size(uint8) -> 1;
scalar_size(short) -> 2;
scalar_size(ushort) -> 2;
scalar_size(int16) -> 2;
scalar_size(uint16) -> 2;
scalar_size(int) -> 4;
scalar_size(uint) -> 4;
scalar_size(int32) -> 4;
scalar_size(uint32) -> 4;
scalar_size(float) -> 4;
scalar_size(float32) -> 4;
scalar_size(long) -> 8;
scalar_size(ulong) -> 8;
scalar_size(int64) -> 8;
scalar_size(uint64) -> 8;
scalar_size(double) -> 8;
scalar_size(float64) -> 8.

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
    {ok, Value}.
