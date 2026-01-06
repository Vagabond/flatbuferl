-module(eflatbuffers).

-export([
    new/2,
    new/3,
    get/2,
    get/3,
    get_bytes/2,
    file_id/1,
    has/2,
    to_map/1,
    to_map/2,
    from_map/3,
    from_map/4,
    from_map/5
]).

-export_type([ctx/0, path/0, decode_opts/0]).

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
    defs :: schema:definitions(),
    root_type :: atom(),
    root :: {table, non_neg_integer(), binary()}
}).

-opaque ctx() :: #ctx{}.
-type path() :: [atom()].

%% =============================================================================
%% Context Creation
%% =============================================================================

-spec new(binary(), schema:definitions()) -> ctx().
new(Buffer, Defs) ->
    %% Try to find root_type from schema options, or use first table
    RootType = find_root_type(Defs),
    new(Buffer, Defs, RootType).

-spec new(binary(), schema:definitions(), atom()) -> ctx().
new(Buffer, Defs, RootType) ->
    Root = reader:get_root(Buffer),
    #ctx{
        buffer = Buffer,
        defs = Defs,
        root_type = RootType,
        root = Root
    }.

find_root_type(Defs) ->
    %% Find first table in defs
    Tables = [Name || {Name, {table, _}} <- maps:to_list(Defs)],
    case Tables of
        [First | _] -> First;
        [] -> error(no_tables_in_schema)
    end.

%% =============================================================================
%% Access API
%% =============================================================================

-spec get(ctx(), path()) -> term().
get(Ctx, Path) ->
    case get_internal(Ctx, Path) of
        {ok, Value} -> Value;
        missing -> error({missing_field, Path, no_default})
    end.

-spec get(ctx(), path(), term()) -> term().
get(Ctx, Path, Default) ->
    case get_internal(Ctx, Path) of
        {ok, Value} -> Value;
        missing -> Default
    end.

-spec has(ctx(), path()) -> boolean().
has(Ctx, Path) ->
    case get_internal(Ctx, Path) of
        {ok, _} -> true;
        missing -> false
    end.

%% =============================================================================
%% Full Deserialization
%% =============================================================================

-spec to_map(ctx()) -> map().
to_map(Ctx) ->
    to_map(Ctx, #{}).

-spec to_map(ctx(), decode_opts()) -> map().
to_map(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Opts) ->
    table_to_map(Root, Defs, RootType, Buffer, Opts).

-spec from_map(map(), schema:definitions(), atom()) -> iodata().
from_map(Map, Defs, RootType) ->
    builder:from_map(Map, Defs, RootType).

-spec from_map(map(), schema:definitions(), atom(), binary()) -> iodata().
from_map(Map, Defs, RootType, FileId) ->
    builder:from_map(Map, Defs, RootType, FileId).

-spec from_map(map(), schema:definitions(), atom(), binary() | no_file_id, builder:encode_opts()) -> iodata().
from_map(Map, Defs, RootType, FileId, Opts) ->
    builder:from_map(Map, Defs, RootType, FileId, Opts).

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
                    case reader:get_field(TableRef, FieldId, resolve_type(Type, Defs), Buffer) of
                        {ok, _} -> error({deprecated_field_present, TableType, FieldName});
                        missing -> Acc
                    end;
                _ ->
                    decode_field(FieldName, FieldId, Type, Default, TableRef, Defs, Buffer, Opts, Acc)
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
            case reader:get_field(TableRef, FieldId, {union_type, UnionName}, Buffer) of
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
            case reader:get_field(TableRef, TypeFieldId, {union_type, UnionName}, Buffer) of
                {ok, 0} ->
                    %% NONE type
                    Acc;
                {ok, TypeIndex} ->
                    case reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                        {ok, TableValueRef} ->
                            {union, Members} = maps:get(UnionName, Defs),
                            MemberType = lists:nth(TypeIndex, Members),
                            ConvertedValue = table_to_map(TableValueRef, Defs, MemberType, Buffer, Opts),
                            Acc#{FieldName => ConvertedValue};
                        missing ->
                            Acc
                    end;
                missing ->
                    Acc
            end;
        _ ->
            case reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
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
resolve_type(Type, _Defs) ->
    Type.

convert_value({table, _, _} = TableRef, Type, Defs, Buffer, Opts) when is_atom(Type) ->
    %% Nested table - recursively convert
    table_to_map(TableRef, Defs, Type, Buffer, Opts);
convert_value(Values, {vector, ElemType}, Defs, Buffer, Opts) when is_list(Values), is_atom(ElemType) ->
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
            case reader:get_field(TableRef, FieldId, NestedType, Buffer) of
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

-spec file_id(ctx() | binary()) -> binary().
file_id(#ctx{buffer = Buffer}) ->
    reader:get_file_id(Buffer);
file_id(Buffer) when is_binary(Buffer) ->
    reader:get_file_id(Buffer).

%% =============================================================================
%% Internal
%% =============================================================================

get_internal(#ctx{buffer = Buffer, defs = Defs, root_type = RootType, root = Root}, Path) ->
    get_path(Root, Defs, RootType, Path, Buffer).

get_path(TableRef, Defs, TableType, [FieldName], Buffer) ->
    {table, Fields} = maps:get(TableType, Defs),
    case find_field(Fields, FieldName) of
        {ok, FieldId, Type, Default} ->
            case reader:get_field(TableRef, FieldId, Type, Buffer) of
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
            case reader:get_field(TableRef, FieldId, NestedType, Buffer) of
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
normalize_type(Type) ->
    Type.
