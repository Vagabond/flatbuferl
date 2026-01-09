%% @doc FlatBuffers implementation for Erlang.
%%
%% Schemas are parsed at runtime without code generation. Encoding produces
%% iolists and decoding returns sub-binaries, both avoiding unnecessary copies.
-module(flatbuferl).

-include("flatbuferl_records.hrl").

-export([
    parse_schema/1,
    parse_schema_file/1,
    new/2,
    get/2,
    get/3,
    get_bytes/2,
    file_id/1,
    has/2,
    to_map/1,
    to_map/2,
    from_map/2,
    from_map/3,
    update/2,
    validate/2,
    validate/3
]).

%% @private Context accessors for internal modules
-export([ctx_buffer/1, ctx_schema/1, ctx_root/1]).

-export_type([ctx/0, path/0, decode_opts/0, schema/0, validate_opts/0, validation_error/0]).

-type schema() :: {flatbuferl_schema:definitions(), flatbuferl_schema:options()}.

%% Options for decoding:
%%   deprecated => skip | allow | error
%%     - skip: silently omit deprecated fields from output (default)
%%     - allow: include deprecated fields in output
%%     - error: raise error if deprecated field is present in buffer
-type decode_opts() :: #{
    deprecated => skip | allow | error
}.

-record(ctx, {
    buffer :: binary(),
    defs :: flatbuferl_schema:definitions(),
    root_type :: atom(),
    root :: {table, non_neg_integer(), binary()}
}).

-opaque ctx() :: #ctx{}.
-type path() :: [atom()].

%% =============================================================================
%% Schema Parsing
%% =============================================================================

%% @doc Parse a FlatBuffers schema from a string or binary.
-spec parse_schema(string() | binary()) -> {ok, schema()} | {error, term()}.
parse_schema(Schema) ->
    flatbuferl_schema:parse(Schema).

%% @doc Parse a FlatBuffers schema from a file.
%% Resolves `include' directives relative to the file's directory.
-spec parse_schema_file(file:filename()) -> {ok, schema()} | {error, term()}.
parse_schema_file(Filename) ->
    flatbuferl_schema:parse_file(Filename).

%% =============================================================================
%% Context Creation
%% =============================================================================

%% @doc Create a decoding context from a buffer and schema.
%% The context can be used with `get/2', `has/2', and `to_map/1'.
-spec new(binary(), schema()) -> ctx().
new(Buffer, {Defs, SchemaOpts}) ->
    RootType = maps:get(root_type, SchemaOpts),
    Root = flatbuferl_reader:get_root(Buffer),
    #ctx{
        buffer = Buffer,
        defs = Defs,
        root_type = RootType,
        root = Root
    }.

%% =============================================================================
%% Access API
%% =============================================================================

%% @doc Get a field value by path. Raises if field is missing and has no default.
-spec get(ctx(), path()) -> term().
get(Ctx, Path) ->
    case get_internal(Ctx, Path) of
        {ok, Value} -> Value;
        missing -> error({missing_field, Path, no_default})
    end.

%% @doc Get a field value by path, returning Default if missing.
-spec get(ctx(), path(), term()) -> term().
get(Ctx, Path, Default) ->
    case get_internal(Ctx, Path) of
        {ok, Value} -> Value;
        missing -> Default
    end.

%% @doc Check if a field is present in the buffer.
-spec has(ctx(), path()) -> boolean().
has(Ctx, Path) ->
    case get_internal(Ctx, Path) of
        {ok, _} -> true;
        missing -> false
    end.

%% =============================================================================
%% Full Deserialization
%% =============================================================================

%% @doc Decode the entire buffer to an Erlang map.
-spec to_map(ctx()) -> map().
to_map(Ctx) ->
    to_map(Ctx, #{}).

%% @doc Decode the entire buffer to an Erlang map with options.
-spec to_map(ctx(), decode_opts()) -> map().
to_map(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Opts) ->
    table_to_map(Root, Defs, RootType, Buffer, Opts).

%% @doc Encode an Erlang map to FlatBuffers iodata.
-spec from_map(map(), schema()) -> iodata().
from_map(Map, Schema) ->
    flatbuferl_builder:from_map(Map, Schema).

%% @doc Encode an Erlang map to FlatBuffers iodata with options.
-spec from_map(map(), schema(), flatbuferl_builder:encode_opts()) -> iodata().
from_map(Map, Schema, Opts) ->
    flatbuferl_builder:from_map(Map, Schema, Opts).

%% @doc Update fields in a FlatBuffer.
%%
%% For fixed-size scalar fields that exist in the buffer, performs an efficient
%% in-place splice without copying the buffer. For variable-length fields or
%% fields not present in the buffer, falls back to full re-encoding.
%%
%% Changes is a nested map mirroring the structure from `to_map/1':
%% ```
%% update(Ctx, #{hp => 150})              %% update scalar
%% update(Ctx, #{pos => #{x => 5.0}})     %% update nested struct field
%% update(Ctx, #{hp => 200, level => 10}) %% update multiple fields
%% '''
-spec update(ctx(), map()) -> iodata().
update(Ctx, Changes) ->
    flatbuferl_update:update(Ctx, Changes).

table_to_map(TableRef, Defs, TableType, Buffer, Opts) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    DeprecatedOpt = maps:get(deprecated, Opts, skip),
    VTable = flatbuferl_reader:read_vtable(TableRef),
    decode_fields(Fields, VTable, Defs, TableType, Buffer, Opts, DeprecatedOpt, #{}).

%% Recursive field decoder - reads vtable once, dispatches by field type
decode_fields([], _VTable, _Defs, _TableType, _Buffer, _Opts, _DeprecatedOpt, Acc) ->
    Acc;
%% Skip deprecated fields (common case: DeprecatedOpt = skip)
decode_fields(
    [#field_def{deprecated = true} | Rest], VTable, Defs, TableType, Buffer, Opts, skip, Acc
) ->
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, skip, Acc);
%% Inline array field - read fixed-size array data directly
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            resolved_type = {array, _, _} = RT,
            default = Def,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
            {ok, Value} -> Acc#{Name => Value};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Union type field - read discriminator and convert index to atom
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            resolved_type = #union_type_def{reverse_map = ReverseMap},
            deprecated = false,
            is_primitive = true
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    %% Use precomputed reverse map (index -> atom) for decoding
    Acc1 =
        case flatbuferl_reader:read_union_type_field(VTable, Id, Buffer) of
            {ok, 0} -> Acc;
            {ok, TypeIndex} ->
                MemberType = maps:get(TypeIndex, ReverseMap),
                Acc#{Name => MemberType};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Union value field - use precomputed type_field_id and reverse_map
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            resolved_type = #union_value_def{reverse_map = ReverseMap, type_field_id = TypeFieldId},
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_union_type_field(VTable, TypeFieldId, Buffer) of
            {ok, 0} -> Acc;
            {ok, TypeIndex} ->
                case flatbuferl_reader:read_union_value_field(VTable, Id, Buffer) of
                    {ok, TableRef} ->
                        MemberType = maps:get(TypeIndex, ReverseMap),
                        Acc#{Name => table_to_map(TableRef, Defs, MemberType, Buffer, Opts)};
                    missing -> Acc
                end;
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Primitive scalar - fast path with only 11 clause function
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            resolved_type = RT,
            default = Def,
            deprecated = false,
            is_primitive = true
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_scalar_field(VTable, Id, RT, Buffer) of
            {ok, Value} -> Acc#{Name => Value};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Non-primitive atom type (nested table) - needs convert_value for recursive decode
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            type = Type,
            resolved_type = RT,
            default = Def,
            deprecated = false,
            is_primitive = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) when Type /= string, is_atom(Type), is_atom(RT) ->
    Acc1 =
        case flatbuferl_reader:read_ref_field(VTable, Id, RT, Buffer) of
            {ok, Value} -> Acc#{Name => convert_value(Value, Type, Defs, Buffer, Opts)};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Non-deprecated string field - use fast string reader
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            type = string,
            default = Def,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_string_field(VTable, Id, Buffer) of
            {ok, Value} -> Acc#{Name => Value};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Non-deprecated struct field - inline data, use fast struct reader
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            resolved_type = #struct_def{} = StructDef,
            default = Def,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_struct_field(VTable, Id, StructDef, Buffer) of
            {ok, Value} -> Acc#{Name => Value};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Non-deprecated vector of atoms
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            type = {vector, ElemType},
            resolved_type = RT,
            default = Def,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) when is_atom(ElemType) ->
    Acc1 =
        case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
            {ok, Value} ->
                Acc#{Name => convert_value(Value, {vector, ElemType}, Defs, Buffer, Opts)};
            missing when Def /= undefined -> Acc#{Name => Def};
            missing ->
                Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Vector of union types - read discriminator bytes and map to member names
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            type = {vector, {union_type, UnionName}},
            resolved_type = RT,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
            {ok, TypeIndices} ->
                {union, Members, _} = maps:get(UnionName, Defs),
                TypeNames = [lists:nth(Idx, Members) || Idx <- TypeIndices, Idx > 0],
                Acc#{Name => TypeNames};
            missing ->
                Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Vector of union values - decode each table using its type
decode_fields(
    [
        #field_def{
            name = Name,
            id = Id,
            type = {vector, {union_value, UnionName}},
            resolved_type = RT,
            deprecated = false
        }
        | Rest
    ],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    DepOpt,
    Acc
) ->
    TypeFieldId = Id - 1,
    TypeVecDef = #vector_def{element_type = uint8, is_primitive = true, element_size = 1},
    Acc1 =
        case flatbuferl_reader:read_field(VTable, TypeFieldId, TypeVecDef, Buffer) of
            {ok, TypeIndices} ->
                case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
                    {ok, TableRefs} ->
                        {union, Members, _} = maps:get(UnionName, Defs),
                        DecodedValues = lists:zipwith(
                            fun(TypeIdx, TableValueRef) ->
                                MemberType = lists:nth(TypeIdx, Members),
                                table_to_map(TableValueRef, Defs, MemberType, Buffer, Opts)
                            end,
                            TypeIndices,
                            TableRefs
                        ),
                        Acc#{Name => DecodedValues};
                    missing ->
                        Acc
                end;
            missing ->
                Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, DepOpt, Acc1);
%% Deprecated field with error option
decode_fields(
    [#field_def{name = Name, id = Id, resolved_type = RT, deprecated = true} | Rest],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    error,
    Acc
) ->
    case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
        {ok, _} -> error({deprecated_field_present, TableType, Name});
        missing -> decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, error, Acc)
    end;
%% Deprecated field with allow option
decode_fields(
    [#field_def{name = Name, id = Id, resolved_type = RT, deprecated = true} | Rest],
    VTable,
    Defs,
    TableType,
    Buffer,
    Opts,
    allow,
    Acc
) ->
    Acc1 =
        case flatbuferl_reader:read_field(VTable, Id, RT, Buffer) of
            {ok, Value} -> Acc#{Name => Value};
            missing -> Acc
        end,
    decode_fields(Rest, VTable, Defs, TableType, Buffer, Opts, allow, Acc1).

convert_value({table, _, _} = TableRef, Type, Defs, Buffer, Opts) when is_atom(Type) ->
    %% Nested table - recursively convert
    table_to_map(TableRef, Defs, Type, Buffer, Opts);
convert_value(Values, {vector, ElemType}, Defs, Buffer, Opts) when
    is_list(Values), is_atom(ElemType)
->
    %% Vector of tables - convert each element
    case maps:get(ElemType, Defs, undefined) of
        #table_def{} ->
            [table_to_map(V, Defs, ElemType, Buffer, Opts) || V <- Values];
        _ ->
            Values
    end;
convert_value(Value, _Type, _Defs, _Buffer, _Opts) ->
    Value.

%% =============================================================================
%% Raw Bytes Access
%% =============================================================================

%% @doc Get raw bytes for a field, returning a sub-binary from the buffer.
-spec get_bytes(ctx(), path()) -> binary().
get_bytes(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Path) ->
    case get_bytes_internal(Root, Defs, RootType, Path, Buffer) of
        {ok, Bytes} -> Bytes;
        missing -> error({missing_field, Path})
    end.

get_bytes_internal(TableRef, Defs, TableType, [FieldName], Buffer) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, _Type, _Default} ->
            get_field_bytes(TableRef, FieldId, Buffer);
        error ->
            error({unknown_field, FieldName})
    end;
get_bytes_internal(TableRef, Defs, TableType, [FieldName | Rest], Buffer) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, NestedType, _Default} when is_atom(NestedType) ->
            case flatbuferl_reader:get_field(TableRef, FieldId, NestedType, Buffer) of
                {ok, NestedTableRef} ->
                    get_bytes_internal(NestedTableRef, Defs, NestedType, Rest, Buffer);
                missing ->
                    missing
            end;
        {ok, _FieldId, Type, _Default} ->
            error({not_a_table, FieldName, Type});
        error ->
            error({unknown_field, FieldName})
    end.

get_field_bytes({table, TableOffset, Buffer}, FieldId, _Buffer) ->
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
                    <<_:FieldPos/binary, DataOffset:32/little-unsigned, _/binary>> = Buffer,
                    DataPos = FieldPos + DataOffset,
                    <<_:DataPos/binary, Rest/binary>> = Buffer,
                    {ok, Rest}
            end;
        false ->
            missing
    end.

%% =============================================================================
%% Metadata
%% =============================================================================

%% @doc Extract the 4-byte file identifier from a buffer.
-spec file_id(ctx() | binary()) -> binary().
file_id(#ctx{buffer = Buffer}) ->
    flatbuferl_reader:get_file_id(Buffer);
file_id(Buffer) when is_binary(Buffer) ->
    flatbuferl_reader:get_file_id(Buffer).

%% @hidden
-spec ctx_buffer(ctx()) -> binary().
ctx_buffer(#ctx{buffer = Buffer}) -> Buffer.

%% @hidden
-spec ctx_schema(ctx()) -> schema().
ctx_schema(#ctx{defs = Defs, root_type = RootType}) ->
    {Defs, #{root_type => RootType}}.

%% @hidden
-spec ctx_root(ctx()) ->
    {
        Defs :: flatbuferl_schema:definitions(),
        RootType :: atom(),
        Root :: flatbuferl_reader:table_ref()
    }.
ctx_root(#ctx{defs = Defs, root_type = RootType, root = Root}) ->
    {Defs, RootType, Root}.

%% =============================================================================
%% Internal
%% =============================================================================

get_internal(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Path) ->
    get_path(Root, Defs, RootType, Path, Buffer).

get_path(TableRef, Defs, TableType, [FieldName], Buffer) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, Type, Default} ->
            ReaderType = resolve_for_reader(Type, Defs),
            case flatbuferl_reader:get_field(TableRef, FieldId, ReaderType, Buffer) of
                {ok, Value} ->
                    {ok, convert_enum_value(Value, Type, Defs)};
                missing when Default =/= undefined ->
                    {ok, Default};
                missing ->
                    missing
            end;
        error ->
            error({unknown_field, FieldName})
    end;
get_path(TableRef, Defs, TableType, [FieldName | Rest], Buffer) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, NestedType, _Default} when is_atom(NestedType) ->
            case flatbuferl_reader:get_field(TableRef, FieldId, NestedType, Buffer) of
                {ok, NestedTableRef} ->
                    get_path(NestedTableRef, Defs, NestedType, Rest, Buffer);
                missing ->
                    missing
            end;
        {ok, _FieldId, Type, _Default} ->
            error({not_a_table, FieldName, Type});
        error ->
            error({unknown_field, FieldName})
    end.

find_field([], _Name) ->
    error;
find_field(
    [#field_def{name = Name, id = FieldId, resolved_type = ResolvedType, default = Default} | _],
    Name
) ->
    {ok, FieldId, ResolvedType, Default};
find_field([_ | Rest], Name) ->
    find_field(Rest, Name).

%% Resolve type name to reader-compatible type
%% Handle types with defaults (unwrap first, but not type constructors)
resolve_for_reader({TypeName, Default}, Defs) when
    is_atom(TypeName),
    is_atom(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    resolve_for_reader(TypeName, Defs);
resolve_for_reader({TypeName, Default}, Defs) when is_atom(TypeName), is_number(Default) ->
    resolve_for_reader(TypeName, Defs);
resolve_for_reader({TypeName, Default}, Defs) when is_atom(TypeName), is_boolean(Default) ->
    resolve_for_reader(TypeName, Defs);
resolve_for_reader(TypeName, Defs) when is_atom(TypeName) ->
    case maps:get(TypeName, Defs, undefined) of
        {{enum, Base}, _, _} -> {enum, Base};
        _ -> TypeName
    end;
resolve_for_reader(Type, _Defs) ->
    Type.

%% Convert integer enum value back to atom
%% Handle types with defaults (unwrap first, but not type constructors)
convert_enum_value(Value, {TypeName, Default}, Defs) when
    is_atom(TypeName),
    is_atom(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    convert_enum_value(Value, TypeName, Defs);
convert_enum_value(Value, {TypeName, Default}, Defs) when is_atom(TypeName), is_number(Default) ->
    convert_enum_value(Value, TypeName, Defs);
convert_enum_value(Value, {TypeName, Default}, Defs) when is_atom(TypeName), is_boolean(Default) ->
    convert_enum_value(Value, TypeName, Defs);
%% Handle resolved enum type {enum, Base, IndexMap} from resolved_type
convert_enum_value(Value, {enum, _Base, IndexMap}, _Defs) when
    is_integer(Value), is_map(IndexMap)
->
    %% Reverse lookup: find atom name for this integer value
    case [Name || {Name, V} <- maps:to_list(IndexMap), V == Value] of
        [Name] -> Name;
        %% Unknown value, return as-is
        [] -> Value
    end;
convert_enum_value(Value, TypeName, Defs) when is_atom(TypeName), is_integer(Value) ->
    case maps:get(TypeName, Defs, undefined) of
        {{enum, _Base}, Values, _} ->
            %% Enum values are 0-indexed
            lists:nth(Value + 1, Values);
        _ ->
            Value
    end;
convert_enum_value(Value, _Type, _Defs) ->
    Value.

%% =============================================================================
%% Validation
%% =============================================================================

-type validate_opts() :: flatbuferl_schema:validate_opts().
-type validation_error() :: flatbuferl_schema:validation_error().

%% @doc Validate an Erlang map against a schema before encoding.
-spec validate(map(), schema()) -> ok | {error, [validation_error()]}.
validate(Map, Schema) ->
    validate(Map, Schema, #{}).

%% @doc Validate an Erlang map against a schema with options.
-spec validate(map(), schema(), validate_opts()) -> ok | {error, [validation_error()]}.
validate(Map, Schema, Opts) ->
    flatbuferl_schema:validate(Map, Schema, Opts).
