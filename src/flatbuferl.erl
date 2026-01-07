%% @doc FlatBuffers implementation for Erlang.
%%
%% Schemas are parsed at runtime without code generation. Encoding produces
%% iolists and decoding returns sub-binaries, both avoiding unnecessary copies.
-module(flatbuferl).

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
    validate/2,
    validate/3
]).

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

table_to_map(TableRef, Defs, TableType, Buffer, Opts) ->
    {table, Fields} = maps:get(TableType, Defs),
    DeprecatedOpt = maps:get(deprecated, Opts, skip),
    lists:foldl(
        fun(FieldDef, Acc) ->
            {FieldName, FieldId, Type, Default, Deprecated} = parse_field_def_full(FieldDef),
            %% Handle deprecated fields
            case {Deprecated, DeprecatedOpt} of
                {true, skip} ->
                    Acc;
                {true, error} ->
                    %% Check if field is present in buffer
                    case
                        flatbuferl_reader:get_field(
                            TableRef, FieldId, resolve_type(Type, Defs), Buffer
                        )
                    of
                        {ok, _} -> error({deprecated_field_present, TableType, FieldName});
                        missing -> Acc
                    end;
                _ ->
                    decode_field(
                        FieldName, FieldId, Type, Default, TableRef, Defs, Buffer, Opts, Acc
                    )
            end
        end,
        #{},
        Fields
    ).

parse_field_def_full({Name, Type, Attrs}) ->
    FieldId = maps:get(id, Attrs, 0),
    Deprecated = maps:get(deprecated, Attrs, false),
    Default = extract_default(Type),
    {Name, FieldId, normalize_type(Type), Default, Deprecated};
parse_field_def_full({Name, Type}) ->
    Default = extract_default(Type),
    {Name, 0, normalize_type(Type), Default, false}.

decode_field(FieldName, FieldId, Type, Default, TableRef, Defs, Buffer, Opts, Acc) ->
    ResolvedType = resolve_type(Type, Defs),
    case Type of
        {union_type, UnionName} ->
            %% Union type field - output as <field>_type with the member name
            case flatbuferl_reader:get_field(TableRef, FieldId, {union_type, UnionName}, Buffer) of
                {ok, 0} ->
                    %% NONE type - skip
                    Acc;
                {ok, TypeIndex} ->
                    {union, Members} = maps:get(UnionName, Defs),
                    MemberType = lists:nth(TypeIndex, Members),
                    Acc#{FieldName => MemberType};
                missing ->
                    Acc
            end;
        {union_value, UnionName} ->
            %% Union value field - output the nested table directly
            TypeFieldId = FieldId - 1,
            case
                flatbuferl_reader:get_field(TableRef, TypeFieldId, {union_type, UnionName}, Buffer)
            of
                {ok, 0} ->
                    %% NONE type
                    Acc;
                {ok, TypeIndex} ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                        {ok, TableValueRef} ->
                            {union, Members} = maps:get(UnionName, Defs),
                            MemberType = lists:nth(TypeIndex, Members),
                            ConvertedValue = table_to_map(
                                TableValueRef, Defs, MemberType, Buffer, Opts
                            ),
                            Acc#{FieldName => ConvertedValue};
                        missing ->
                            Acc
                    end;
                missing ->
                    Acc
            end;
        {vector, {union_type, UnionName}} ->
            %% Vector of union types - output as list of type names
            case flatbuferl_reader:get_field(TableRef, FieldId, {vector, ubyte}, Buffer) of
                {ok, TypeIndices} ->
                    {union, Members} = maps:get(UnionName, Defs),
                    TypeNames = [lists:nth(Idx, Members) || Idx <- TypeIndices, Idx > 0],
                    Acc#{FieldName => TypeNames};
                missing ->
                    Acc
            end;
        {vector, {union_value, UnionName}} ->
            %% Vector of union values - decode each table using its type
            TypeFieldId = FieldId - 1,
            case flatbuferl_reader:get_field(TableRef, TypeFieldId, {vector, ubyte}, Buffer) of
                {ok, TypeIndices} ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, {vector, table}, Buffer) of
                        {ok, TableRefs} ->
                            {union, Members} = maps:get(UnionName, Defs),
                            DecodedValues = lists:zipwith(
                                fun(TypeIdx, TableValueRef) ->
                                    MemberType = lists:nth(TypeIdx, Members),
                                    table_to_map(TableValueRef, Defs, MemberType, Buffer, Opts)
                                end,
                                TypeIndices,
                                TableRefs
                            ),
                            Acc#{FieldName => DecodedValues};
                        missing ->
                            Acc
                    end;
                missing ->
                    Acc
            end;
        _ ->
            case flatbuferl_reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                {ok, Value} ->
                    Acc#{FieldName => convert_value(Value, Type, Defs, Buffer, Opts)};
                missing when Default =/= undefined ->
                    Acc#{FieldName => Default};
                missing ->
                    Acc
            end
    end.

%% Resolve type name to its definition (for enums and structs)
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _Values} -> {enum, Base};
        {struct, Fields} -> {struct, Fields};
        _ -> Type
    end;
resolve_type({vector, ElemType}, Defs) ->
    {vector, resolve_type(ElemType, Defs)};
resolve_type({array, ElemType, Count}, Defs) ->
    {array, resolve_type(ElemType, Defs), Count};
resolve_type(Type, _Defs) ->
    Type.

convert_value({table, _, _} = TableRef, Type, Defs, Buffer, Opts) when is_atom(Type) ->
    %% Nested table - recursively convert
    table_to_map(TableRef, Defs, Type, Buffer, Opts);
convert_value(Values, {vector, ElemType}, Defs, Buffer, Opts) when
    is_list(Values), is_atom(ElemType)
->
    %% Vector of tables - convert each element
    case maps:get(ElemType, Defs, undefined) of
        {table, _} ->
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
    {table, Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, _Type, _Default} ->
            get_field_bytes(TableRef, FieldId, Buffer);
        error ->
            error({unknown_field, FieldName})
    end;
get_bytes_internal(TableRef, Defs, TableType, [FieldName | Rest], Buffer) ->
    {table, Fields} = maps:get(TableType, Defs),
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
    %% Read vtable to find field offset
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buffer,
    VTableOffset = TableOffset - VTableSOffset,
    <<_:VTableOffset/binary, VTableSize:16/little-unsigned, _/binary>> = Buffer,

    FieldOffsetPos = 4 + (FieldId * 2),
    case FieldOffsetPos < VTableSize of
        true ->
            <<_:VTableOffset/binary, _:16, _:16, VTableRest/binary>> = Buffer,
            FieldOffsetInVTable = FieldOffsetPos - 4,
            <<_:FieldOffsetInVTable/binary, FieldOffset:16/little-unsigned, _/binary>> = VTableRest,
            case FieldOffset of
                0 ->
                    missing;
                _ ->
                    %% Field is at TableOffset + FieldOffset
                    %% Read the uoffset to get actual data position
                    FieldPos = TableOffset + FieldOffset,
                    <<_:FieldPos/binary, DataOffset:32/little-unsigned, _/binary>> = Buffer,
                    DataPos = FieldPos + DataOffset,
                    %% Return slice from DataPos to end (caller can use vtable to determine size)
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

%% =============================================================================
%% Internal
%% =============================================================================

get_internal(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Path) ->
    get_path(Root, Defs, RootType, Path, Buffer).

get_path(TableRef, Defs, TableType, [FieldName], Buffer) ->
    {table, Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, Type, Default} ->
            case flatbuferl_reader:get_field(TableRef, FieldId, Type, Buffer) of
                {ok, Value} -> {ok, Value};
                missing when Default =/= undefined -> {ok, Default};
                missing -> missing
            end;
        error ->
            error({unknown_field, FieldName})
    end;
get_path(TableRef, Defs, TableType, [FieldName | Rest], Buffer) ->
    {table, Fields} = maps:get(TableType, Defs),
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
find_field([{Name, Type, Attrs} | _], Name) ->
    FieldId = maps:get(id, Attrs),
    Default = extract_default(Type),
    {ok, FieldId, normalize_type(Type), Default};
find_field([{Name, Type} | _], Name) ->
    Default = extract_default(Type),
    {ok, 0, normalize_type(Type), Default};
find_field([_ | Rest], Name) ->
    find_field(Rest, Name).

extract_default({_Type, Default}) when is_number(Default); is_boolean(Default) ->
    Default;
extract_default(_) ->
    undefined.

normalize_type({Type, Default}) when is_atom(Type), is_number(Default) ->
    Type;
normalize_type({Type, Default}) when is_atom(Type), is_boolean(Default) ->
    Type;
normalize_type({Type, undefined}) when is_atom(Type) ->
    %% optional scalar
    Type;
normalize_type(Type) ->
    Type.

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
