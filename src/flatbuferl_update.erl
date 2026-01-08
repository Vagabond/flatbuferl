%% @doc Buffer update operations for FlatBuffers.
%%
%% Provides efficient in-place updates for scalar fields when possible,
%% falling back to full re-encoding when necessary.
-module(flatbuferl_update).

-export([preflight/2, update/2]).

%% @doc Update fields in a FlatBuffer.
%%
%% Changes is a (possibly nested) map mirroring the structure from to_map.
%% For scalar/struct fields that exist in the buffer, performs an efficient
%% splice update. For variable-length fields or missing fields, falls back
%% to full re-encoding via to_map/from_map.
%%
%% Examples:
%%   update(Ctx, #{hp => 150})              - update single field
%%   update(Ctx, #{pos => #{x => 5.0}})     - update nested struct field
%%   update(Ctx, #{hp => 200, level => 10}) - update multiple fields
%%
%% Returns an iolist (for splice) or iodata (for re-encode).
-spec update(flatbuferl:ctx(), map()) -> iodata().
update(Ctx, Changes) ->
    Buffer = flatbuferl:ctx_buffer(Ctx),
    Schema = flatbuferl:ctx_schema(Ctx),
    case preflight(Ctx, Changes) of
        {simple, Updates} ->
            splice(Buffer, Updates);
        {complex, MergedChanges} ->
            Map = flatbuferl:to_map(Ctx),
            NewMap = deep_merge(Map, MergedChanges),
            flatbuferl:from_map(NewMap, Schema);
        {error, Reason} ->
            error(Reason)
    end.

%% Splice updates into buffer
splice(Buffer, Updates) ->
    Sorted = lists:keysort(1, Updates),
    splice(Buffer, Sorted, 0, []).

splice(Buffer, [], Pos, Acc) ->
    <<_:Pos/binary, Rest/binary>> = Buffer,
    lists:reverse([Rest | Acc]);
splice(Buffer, [{Offset, Size, Type, _Path, Value} | Rest], Pos, Acc) ->
    Len = Offset - Pos,
    <<_:Pos/binary, Chunk:Len/binary, _:Size/binary, _/binary>> = Buffer,
    Encoded = encode_update(Type, Value, Size),
    splice(Buffer, Rest, Offset + Size, [Encoded, Chunk | Acc]).

%% Encode an update value based on type
encode_update({string_shrink, OldAllocated}, NewValue, _Size) ->
    encode_shrunk_string(NewValue, OldAllocated);
encode_update({byte_vector_shrink, OldAllocated}, NewValue, _Size) ->
    encode_shrunk_byte_vector(NewValue, OldAllocated);
encode_update({vector_shrink, ElemType, ElemSize, OldAllocated}, NewValue, _Size) ->
    encode_shrunk_vector(NewValue, ElemType, ElemSize, OldAllocated);
encode_update(Type, Value, Size) ->
    BaseType = base_type(Type),
    encode_scalar(Value, BaseType, Size).

base_type({enum, Base, _EnumName}) -> Base;
base_type(Type) -> Type.

%% Encode a string to fit in OldAllocated bytes
encode_shrunk_string(NewValue, OldAllocated) ->
    NewLen = byte_size(NewValue),
    %% New data: length + string + null terminator
    NewData = <<NewLen:32/little, NewValue/binary, 0>>,
    NewDataSize = byte_size(NewData),
    %% Pad to match original allocated size
    PadSize = OldAllocated - NewDataSize,
    <<NewData/binary, 0:(PadSize * 8)>>.

%% Encode a byte vector (binary) to fit in OldAllocated bytes
encode_shrunk_byte_vector(NewValue, OldAllocated) ->
    NewLen = byte_size(NewValue),
    NewData = <<NewLen:32/little, NewValue/binary>>,
    NewDataSize = byte_size(NewData),
    PadSize = OldAllocated - NewDataSize,
    <<NewData/binary, 0:(PadSize * 8)>>.

%% Encode a scalar vector to fit in OldAllocated bytes
encode_shrunk_vector(NewValues, ElemType, ElemSize, OldAllocated) ->
    NewLen = length(NewValues),
    Elements = [encode_scalar(V, ElemType, ElemSize) || V <- NewValues],
    NewData = iolist_to_binary([<<NewLen:32/little>> | Elements]),
    NewDataSize = byte_size(NewData),
    PadSize = OldAllocated - NewDataSize,
    <<NewData/binary, 0:(PadSize * 8)>>.

%% Encode a scalar value to binary
-spec encode_scalar(term(), atom(), pos_integer()) -> binary().
encode_scalar(Value, bool, 1) ->
    case Value of
        true -> <<1:8>>;
        false -> <<0:8>>
    end;
encode_scalar(Value, Type, 1) when Type == byte; Type == int8 ->
    <<Value:8/signed>>;
encode_scalar(Value, Type, 1) when Type == ubyte; Type == uint8 ->
    <<Value:8/unsigned>>;
encode_scalar(Value, Type, 2) when Type == short; Type == int16 ->
    <<Value:16/little-signed>>;
encode_scalar(Value, Type, 2) when Type == ushort; Type == uint16 ->
    <<Value:16/little-unsigned>>;
encode_scalar(Value, Type, 4) when Type == int; Type == int32 ->
    <<Value:32/little-signed>>;
encode_scalar(Value, Type, 4) when Type == uint; Type == uint32 ->
    <<Value:32/little-unsigned>>;
encode_scalar(Value, Type, 4) when Type == float; Type == float32 ->
    <<Value:32/little-float>>;
encode_scalar(Value, Type, 8) when Type == long; Type == int64 ->
    <<Value:64/little-signed>>;
encode_scalar(Value, Type, 8) when Type == ulong; Type == uint64 ->
    <<Value:64/little-unsigned>>;
encode_scalar(Value, Type, 8) when Type == double; Type == float64 ->
    <<Value:64/little-float>>;
encode_scalar(Value, Type, Size) when is_atom(Type) ->
    %% Named type - enum base types are extracted by base_type/1
    encode_scalar(Value, ubyte, Size).

%% Convert enum atom to integer using the type definition
enum_to_integer(Value, {enum, _Base, EnumName}, Defs) when is_atom(Value) ->
    {{enum, _}, Values, _} = maps:get(EnumName, Defs),
    enum_index(Value, Values, 0);
enum_to_integer(Value, TypeName, Defs) when is_atom(TypeName), is_atom(Value) ->
    case maps:get(TypeName, Defs, undefined) of
        {{enum, _Base}, Values, _} ->
            enum_index(Value, Values, 0);
        _ ->
            Value
    end;
enum_to_integer(Value, _Type, _Defs) ->
    Value.

enum_index(Value, [Value | _], Index) -> Index;
enum_index(Value, [_ | Rest], Index) -> enum_index(Value, Rest, Index + 1).

%% Deep merge nested maps
deep_merge(Base, Changes) when is_map(Base), is_map(Changes) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            case maps:find(Key, Acc) of
                {ok, Existing} when is_map(Existing), is_map(Value) ->
                    maps:put(Key, deep_merge(Existing, Value), Acc);
                _ ->
                    maps:put(Key, Value, Acc)
            end
        end,
        Base,
        Changes
    );
deep_merge(_Base, Changes) ->
    Changes.

%% @doc Pre-flight check for update operations.
%%
%% Validates the proposed changes against the schema and buffer, determining
%% whether a simple splice update is possible or if full re-encoding is needed.
%%
%% Changes is a (possibly nested) map mirroring the structure from to_map.
%%
%% Returns:
%% - `{simple, Updates}' - all changes can be spliced, Updates contains offsets
%% - `{complex, Changes}' - at least one change requires re-encoding
%% - `{error, Reason}' - validation failed (bad field, type mismatch, etc.)
-type field_type() :: atom() | {enum, atom(), atom()}.
-type update_info() :: {
    Offset :: non_neg_integer(),
    Size :: pos_integer(),
    Type :: field_type(),
    Path :: [atom()],
    Value :: term()
}.
-spec preflight(flatbuferl:ctx(), map()) ->
    {simple, [update_info()]}
    | {complex, map()}
    | {error, term()}.
preflight(Ctx, Changes) ->
    Buffer = flatbuferl:ctx_buffer(Ctx),
    {Defs, RootType, Root} = flatbuferl:ctx_root(Ctx),
    %% Flatten nested map to list of {Path, Value} pairs
    PathValues = flatten_changes(Changes),
    preflight_changes(PathValues, Changes, Buffer, Defs, RootType, Root, []).

%% Flatten nested map to list of {Path, Value} pairs
flatten_changes(Map) ->
    flatten_changes(Map, [], []).

flatten_changes(Map, Path, Acc) when is_map(Map), map_size(Map) > 0 ->
    maps:fold(
        fun(Key, Value, A) ->
            flatten_changes(Value, Path ++ [Key], A)
        end,
        Acc,
        Map
    );
flatten_changes(Value, Path, Acc) ->
    [{Path, Value} | Acc].

preflight_changes([], _OrigChanges, _Buffer, _Defs, _RootType, _Root, Acc) ->
    {simple, lists:reverse(Acc)};
preflight_changes([{Path, Value} | Rest], OrigChanges, Buffer, Defs, RootType, Root, Acc) ->
    case preflight_path(Path, Value, Buffer, Defs, RootType, Root) of
        {ok, Update} ->
            preflight_changes(Rest, OrigChanges, Buffer, Defs, RootType, Root, [Update | Acc]);
        complex ->
            %% Return original nested map for deep merge fallback
            {complex, OrigChanges};
        {error, _} = Err ->
            Err
    end.

%% Check a single path and value
preflight_path(Path, Value, Buffer, Defs, RootType, Root) ->
    case traverse_for_update(Path, Buffer, Defs, RootType, Root) of
        {ok, Offset, Type} ->
            case is_fixed_size_type(Type, Defs) of
                {true, Size} ->
                    case validate_value(Value, Type, Defs) of
                        ok ->
                            %% Convert enum atoms to integers for encoding
                            EncodableValue = enum_to_integer(Value, Type, Defs),
                            {ok, {Offset, Size, Type, Path, EncodableValue}};
                        {error, _} = Err ->
                            Err
                    end;
                false ->
                    %% Check if we can shrink-update a string or vector
                    try_shrink_update(Type, Offset, Value, Buffer, Path, Defs)
            end;
        missing ->
            %% Field not present in buffer, need to re-encode to add it
            complex;
        {error, _} = Err ->
            Err
    end.

%% Try to do an in-place shrink update for strings/vectors
try_shrink_update(string, FieldOffset, NewValue, Buffer, Path, _Defs) when is_binary(NewValue) ->
    %% Field offset points to a 32-bit relative offset to string data
    <<_:FieldOffset/binary, RelOffset:32/little-signed, _/binary>> = Buffer,
    StringDataOffset = FieldOffset + RelOffset,
    <<_:StringDataOffset/binary, OldLen:32/little, _/binary>> = Buffer,
    NewLen = byte_size(NewValue),
    case NewLen =< OldLen of
        true ->
            %% Calculate total allocated space (length + data + null + padding)
            OldAllocated = 4 + OldLen + 1 + ((4 - ((OldLen + 1) rem 4)) rem 4),
            {ok, {StringDataOffset, OldAllocated, {string_shrink, OldAllocated}, Path, NewValue}};
        false ->
            complex
    end;
try_shrink_update({vector, ElemType}, FieldOffset, NewValue, Buffer, Path, Defs) ->
    case is_fixed_size_type(ElemType, Defs) of
        {true, ElemSize} when is_list(NewValue) ->
            %% Vector of fixed-size scalars
            <<_:FieldOffset/binary, RelOffset:32/little-signed, _/binary>> = Buffer,
            VecDataOffset = FieldOffset + RelOffset,
            <<_:VecDataOffset/binary, OldLen:32/little, _/binary>> = Buffer,
            NewLen = length(NewValue),
            case NewLen =< OldLen of
                true ->
                    OldDataSize = OldLen * ElemSize,
                    OldAllocated = 4 + OldDataSize + ((4 - (OldDataSize rem 4)) rem 4),
                    {ok,
                        {VecDataOffset, OldAllocated,
                            {vector_shrink, ElemType, ElemSize, OldAllocated}, Path, NewValue}};
                false ->
                    complex
            end;
        {true, _ElemSize} when
            is_binary(NewValue),
            (ElemType == ubyte orelse ElemType == byte orelse ElemType == int8 orelse
                ElemType == uint8)
        ->
            %% Binary for byte vector
            <<_:FieldOffset/binary, RelOffset:32/little-signed, _/binary>> = Buffer,
            VecDataOffset = FieldOffset + RelOffset,
            <<_:VecDataOffset/binary, OldLen:32/little, _/binary>> = Buffer,
            NewLen = byte_size(NewValue),
            case NewLen =< OldLen of
                true ->
                    OldAllocated = 4 + OldLen + ((4 - (OldLen rem 4)) rem 4),
                    {ok,
                        {VecDataOffset, OldAllocated, {byte_vector_shrink, OldAllocated}, Path,
                            NewValue}};
                false ->
                    complex
            end;
        _ ->
            complex
    end;
try_shrink_update(_Type, _Offset, _Value, _Buffer, _Path, _Defs) ->
    complex.

%% Traverse path to find field offset, handling unions
traverse_for_update([FieldName], Buffer, Defs, TableType, TableRef) ->
    lookup_field_offset(TableRef, Buffer, Defs, TableType, FieldName);
traverse_for_update([FieldName | Rest], Buffer, Defs, TableType, TableRef) ->
    case lookup_field_info(Defs, TableType, FieldName) of
        {ok, FieldId, {union_value, UnionName}, _Default} ->
            %% Union field - need to resolve actual type from buffer
            TypeFieldId = FieldId - 1,
            case
                flatbuferl_reader:get_field(TableRef, TypeFieldId, {union_type, UnionName}, Buffer)
            of
                {ok, 0} ->
                    %% NONE type
                    missing;
                {ok, TypeIndex} ->
                    {union, Members, _} = maps:get(UnionName, Defs),
                    MemberType = lists:nth(TypeIndex, Members),
                    case
                        flatbuferl_reader:get_field(
                            TableRef, FieldId, {union_value, UnionName}, Buffer
                        )
                    of
                        {ok, ValueRef} ->
                            traverse_for_update(Rest, Buffer, Defs, MemberType, ValueRef);
                        missing ->
                            missing
                    end;
                missing ->
                    missing
            end;
        {ok, FieldId, NestedType, _Default} when is_atom(NestedType) ->
            case maps:get(NestedType, Defs, undefined) of
                {table, _, _, _, _} ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, NestedType, Buffer) of
                        {ok, NestedRef} ->
                            traverse_for_update(Rest, Buffer, Defs, NestedType, NestedRef);
                        missing ->
                            missing
                    end;
                {struct, Fields} ->
                    %% Struct field - get offset and continue into struct
                    traverse_struct_for_update(TableRef, FieldId, Fields, Rest, Buffer, Defs);
                _ ->
                    {error, {not_traversable, FieldName, NestedType}}
            end;
        {ok, FieldId, {struct, Fields}, _Default} ->
            traverse_struct_for_update(TableRef, FieldId, Fields, Rest, Buffer, Defs);
        {ok, FieldId, {vector, {union_value, UnionName}}, _Default} ->
            %% Vector of unions - need index next
            traverse_union_vector_for_update(TableRef, FieldId, UnionName, Rest, Buffer, Defs);
        {ok, FieldId, {vector, ElemType}, _Default} ->
            %% Regular vector - need index next
            traverse_vector_for_update(TableRef, FieldId, ElemType, Rest, Buffer, Defs);
        {ok, _FieldId, Type, _Default} ->
            {error, {not_traversable, FieldName, Type}};
        error ->
            {error, {unknown_field, FieldName}}
    end.

%% Vector traversal (non-union)
traverse_vector_for_update(TableRef, FieldId, ElemType, [Index | Rest], Buffer, Defs) when
    is_integer(Index)
->
    case flatbuferl_reader:get_vector_info(TableRef, FieldId, {vector, ElemType}, Buffer) of
        {ok, {Length, Start, _}} ->
            ActualIndex =
                if
                    Index < 0 -> Length + Index;
                    true -> Index
                end,
            case ActualIndex >= 0 andalso ActualIndex < Length of
                true ->
                    ElemSize = flatbuferl_reader:element_size(ElemType),
                    case ElemType of
                        Type when is_atom(Type) ->
                            case maps:get(Type, Defs, undefined) of
                                {table, _, _, _, _} ->
                                    %% Vector of tables - read table ref and continue
                                    ElemOffset = Start + (ActualIndex * 4),
                                    <<_:ElemOffset/binary, RelOffset:32/little-unsigned, _/binary>> =
                                        Buffer,
                                    ElemTableRef = {table, ElemOffset + RelOffset, Buffer},
                                    traverse_for_update(Rest, Buffer, Defs, Type, ElemTableRef);
                                _ when Rest == [] ->
                                    %% Scalar at end of path
                                    {ok, Start + (ActualIndex * ElemSize), ElemType};
                                _ ->
                                    {error, {not_traversable, Index, ElemType}}
                            end;
                        _ when Rest == [] ->
                            %% Scalar element type at end of path
                            {ok, Start + (ActualIndex * ElemSize), ElemType};
                        _ ->
                            {error, {not_traversable, Index, ElemType}}
                    end;
                false ->
                    missing
            end;
        missing ->
            missing
    end;
traverse_vector_for_update(_TableRef, _FieldId, _ElemType, [Other | _], _Buffer, _Defs) ->
    {error, {invalid_path_element, Other}}.

%% Union vector traversal
traverse_union_vector_for_update(TableRef, FieldId, UnionName, [Index | Rest], Buffer, Defs) when
    is_integer(Index)
->
    %% Union vectors have parallel type and value vectors
    TypeFieldId = FieldId - 1,
    ValVecType = {vector, {union_value, UnionName}},
    TypeVecType = {vector, {union_type, UnionName}},
    case flatbuferl_reader:get_vector_info(TableRef, FieldId, ValVecType, Buffer) of
        {ok, {Length, _, _} = ValVecInfo} ->
            case flatbuferl_reader:get_vector_info(TableRef, TypeFieldId, TypeVecType, Buffer) of
                {ok, TypeVecInfo} ->
                    ActualIndex =
                        if
                            Index < 0 -> Length + Index;
                            true -> Index
                        end,
                    case ActualIndex >= 0 andalso ActualIndex < Length of
                        true ->
                            %% Read type index for this element
                            {ok, TypeIndex} = flatbuferl_reader:get_vector_element_at(
                                TypeVecInfo, ActualIndex, Buffer
                            ),
                            case TypeIndex of
                                0 ->
                                    %% NONE
                                    missing;
                                _ ->
                                    {union, Members, _} = maps:get(UnionName, Defs),
                                    MemberType = lists:nth(TypeIndex, Members),
                                    %% Get value ref
                                    {ok, ValueRef} = flatbuferl_reader:get_vector_element_at(
                                        ValVecInfo, ActualIndex, Buffer
                                    ),
                                    traverse_for_update(Rest, Buffer, Defs, MemberType, ValueRef)
                            end;
                        false ->
                            missing
                    end;
                missing ->
                    missing
            end;
        missing ->
            missing
    end;
traverse_union_vector_for_update(_TableRef, _FieldId, _UnionName, [Other | _], _Buffer, _Defs) ->
    {error, {invalid_path_element, Other}}.

traverse_struct_for_update(TableRef, FieldId, Fields, [StructField], Buffer, _Defs) ->
    %% Find field within struct
    case find_struct_field(Fields, StructField, 0) of
        {ok, FieldOffset, FieldType} ->
            %% Get the struct's base offset
            case flatbuferl_reader:get_field_offset(TableRef, FieldId, Buffer) of
                {ok, StructOffset} ->
                    {ok, StructOffset + FieldOffset, FieldType};
                missing ->
                    missing
            end;
        error ->
            {error, {unknown_field, StructField}}
    end;
traverse_struct_for_update(TableRef, FieldId, Fields, [StructField | Rest], Buffer, Defs) ->
    %% Nested struct traversal
    case find_struct_field(Fields, StructField, 0) of
        {ok, FieldOffset, {struct, NestedFields}} ->
            case flatbuferl_reader:get_field_offset(TableRef, FieldId, Buffer) of
                {ok, StructOffset} ->
                    traverse_nested_struct(
                        StructOffset + FieldOffset, NestedFields, Rest, Buffer, Defs
                    );
                missing ->
                    missing
            end;
        {ok, _FieldOffset, NestedType} when is_atom(NestedType) ->
            case maps:get(NestedType, Defs, undefined) of
                {struct, NestedFields} ->
                    case flatbuferl_reader:get_field_offset(TableRef, FieldId, Buffer) of
                        {ok, StructOffset} ->
                            case find_struct_field(Fields, StructField, 0) of
                                {ok, FieldOff, _} ->
                                    traverse_nested_struct(
                                        StructOffset + FieldOff, NestedFields, Rest, Buffer, Defs
                                    );
                                error ->
                                    {error, {unknown_field, StructField}}
                            end;
                        missing ->
                            missing
                    end;
                _ ->
                    {error, {not_traversable, StructField, NestedType}}
            end;
        {ok, _FieldOffset, Type} ->
            {error, {not_traversable, StructField, Type}};
        error ->
            {error, {unknown_field, StructField}}
    end.

traverse_nested_struct(BaseOffset, Fields, [FieldName], _Buffer, _Defs) ->
    case find_struct_field(Fields, FieldName, 0) of
        {ok, FieldOffset, FieldType} ->
            {ok, BaseOffset + FieldOffset, FieldType};
        error ->
            {error, {unknown_field, FieldName}}
    end;
traverse_nested_struct(BaseOffset, Fields, [FieldName | Rest], Buffer, Defs) ->
    case find_struct_field(Fields, FieldName, 0) of
        {ok, FieldOffset, {struct, NestedFields}} ->
            traverse_nested_struct(BaseOffset + FieldOffset, NestedFields, Rest, Buffer, Defs);
        {ok, FieldOffset, NestedType} when is_atom(NestedType) ->
            case maps:get(NestedType, Defs, undefined) of
                {struct, NestedFields} ->
                    traverse_nested_struct(
                        BaseOffset + FieldOffset, NestedFields, Rest, Buffer, Defs
                    );
                _ ->
                    {error, {not_traversable, FieldName, NestedType}}
            end;
        {ok, _FieldOffset, Type} ->
            {error, {not_traversable, FieldName, Type}};
        error ->
            {error, {unknown_field, FieldName}}
    end.

find_struct_field([], _Name, _Offset) ->
    error;
%% Enriched format - use precomputed offset
find_struct_field([#{name := FieldName, type := Type, offset := FieldOffset} | Rest], Name, Offset) ->
    case FieldName of
        Name -> {ok, FieldOffset, Type};
        _ -> find_struct_field(Rest, Name, Offset)
    end;
%% Raw tuple format
find_struct_field([{FieldName, Type} | Rest], Name, Offset) ->
    case FieldName of
        Name -> {ok, Offset, Type};
        _ ->
            Size = type_size(Type),
            find_struct_field(Rest, Name, Offset + Size)
    end;
find_struct_field([{FieldName, Type, _Attrs} | Rest], Name, Offset) ->
    case FieldName of
        Name -> {ok, Offset, Type};
        _ ->
            Size = type_size(Type),
            find_struct_field(Rest, Name, Offset + Size)
    end.

%% Get field offset in buffer (for final field in path)
lookup_field_offset(TableRef, Buffer, Defs, TableType, FieldName) ->
    case lookup_field_info(Defs, TableType, FieldName) of
        {ok, FieldId, Type, _Default} ->
            ResolvedType = resolve_type(Type, Defs),
            case flatbuferl_reader:get_field_offset(TableRef, FieldId, Buffer) of
                {ok, Offset} ->
                    {ok, Offset, ResolvedType};
                missing ->
                    missing
            end;
        error ->
            {error, {unknown_field, FieldName}}
    end.

lookup_field_info(Defs, TableType, FieldName) ->
    {table, _, _, Fields, _} = maps:get(TableType, Defs),
    find_field(Fields, FieldName).

find_field([], _Name) ->
    error;
find_field([#{name := Name, id := FieldId, type := Type, default := Default} | _], Name) ->
    {ok, FieldId, Type, Default};
find_field([_ | Rest], Name) ->
    find_field(Rest, Name).

resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _, _} -> {enum, Base, Type};
        _ -> Type
    end;
resolve_type(Type, _Defs) ->
    Type.

%% Check if type is fixed-size and return size
is_fixed_size_type(bool, _Defs) ->
    {true, 1};
is_fixed_size_type(byte, _Defs) ->
    {true, 1};
is_fixed_size_type(ubyte, _Defs) ->
    {true, 1};
is_fixed_size_type(int8, _Defs) ->
    {true, 1};
is_fixed_size_type(uint8, _Defs) ->
    {true, 1};
is_fixed_size_type(short, _Defs) ->
    {true, 2};
is_fixed_size_type(ushort, _Defs) ->
    {true, 2};
is_fixed_size_type(int16, _Defs) ->
    {true, 2};
is_fixed_size_type(uint16, _Defs) ->
    {true, 2};
is_fixed_size_type(int, _Defs) ->
    {true, 4};
is_fixed_size_type(uint, _Defs) ->
    {true, 4};
is_fixed_size_type(int32, _Defs) ->
    {true, 4};
is_fixed_size_type(uint32, _Defs) ->
    {true, 4};
is_fixed_size_type(float, _Defs) ->
    {true, 4};
is_fixed_size_type(float32, _Defs) ->
    {true, 4};
is_fixed_size_type(long, _Defs) ->
    {true, 8};
is_fixed_size_type(ulong, _Defs) ->
    {true, 8};
is_fixed_size_type(int64, _Defs) ->
    {true, 8};
is_fixed_size_type(uint64, _Defs) ->
    {true, 8};
is_fixed_size_type(double, _Defs) ->
    {true, 8};
is_fixed_size_type(float64, _Defs) ->
    {true, 8};
is_fixed_size_type({enum, Base, _EnumName}, Defs) ->
    is_fixed_size_type(Base, Defs);
is_fixed_size_type({struct, Fields}, Defs) ->
    {true, struct_size(Fields, Defs)};
is_fixed_size_type(TypeName, Defs) when is_atom(TypeName) ->
    case maps:get(TypeName, Defs, undefined) of
        {{enum, Base}, _, _} -> is_fixed_size_type(Base, Defs);
        {struct, Fields} -> {true, struct_size(Fields, Defs)};
        _ -> false
    end;
is_fixed_size_type(_, _Defs) ->
    false.

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
type_size(float) -> 4;
type_size(float32) -> 4;
type_size(long) -> 8;
type_size(ulong) -> 8;
type_size(int64) -> 8;
type_size(uint64) -> 8;
type_size(double) -> 8;
type_size(float64) -> 8;
type_size({struct, Fields}) -> struct_size(Fields, #{});
type_size(_) -> 0.

struct_size(Fields, _Defs) ->
    lists:sum(
        [type_size(Type) || {_, Type} <- Fields] ++
            [type_size(Type) || {_, Type, _} <- Fields]
    ).

%% Validate value matches expected type
validate_value(Value, bool, _Defs) when is_boolean(Value) -> ok;
validate_value(Value, Type, _Defs) when
    Type == byte;
    Type == int8;
    Type == ubyte;
    Type == uint8;
    Type == short;
    Type == int16;
    Type == ushort;
    Type == uint16;
    Type == int;
    Type == int32;
    Type == uint;
    Type == uint32;
    Type == long;
    Type == int64;
    Type == ulong;
    Type == uint64
->
    if
        is_integer(Value) -> ok;
        true -> {error, {type_mismatch, Type, Value}}
    end;
validate_value(Value, Type, _Defs) when
    Type == float; Type == float32; Type == double; Type == float64
->
    if
        is_number(Value) -> ok;
        true -> {error, {type_mismatch, Type, Value}}
    end;
validate_value(Value, {enum, _Base, EnumName}, Defs) ->
    {{enum, _}, Values, _} = maps:get(EnumName, Defs),
    case lists:member(Value, Values) of
        true -> ok;
        false -> {error, {invalid_enum_value, EnumName, Value}}
    end;
validate_value(Value, {struct, Fields}, Defs) when is_map(Value) ->
    validate_struct_value(Value, Fields, Defs);
validate_value(Value, TypeName, Defs) when is_atom(TypeName) ->
    case maps:get(TypeName, Defs, undefined) of
        {{enum, _Base}, Values, _} ->
            case lists:member(Value, Values) of
                true -> ok;
                false -> {error, {invalid_enum_value, TypeName, Value}}
            end;
        {struct, Fields} ->
            validate_struct_value(Value, Fields, Defs);
        _ ->
            {error, {type_mismatch, TypeName, Value}}
    end;
validate_value(Value, Type, _Defs) ->
    {error, {type_mismatch, Type, Value}}.

validate_struct_value(Value, Fields, Defs) when is_map(Value) ->
    Results = [
        validate_value(maps:get(Name, Value, undefined), Type, Defs)
     || {Name, Type} <- Fields
    ],
    case
        lists:filter(
            fun
                (ok) -> false;
                (_) -> true
            end,
            Results
        )
    of
        [] -> ok;
        [Err | _] -> Err
    end;
validate_struct_value(Value, _Fields, _Defs) ->
    {error, {expected_map, Value}}.
