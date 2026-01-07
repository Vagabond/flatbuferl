%% @doc Path-based access to FlatBuffer data.
%%
%% Provides `fetch/2' for flexible path-based access with support for:
%% - Vector/array indexing: `[monsters, 0, name]'
%% - Wildcards: `[monsters, '*', name]'
%% - Multi-field extraction: `[monsters, 0, [name, hp]]'
-module(flatbuferl_fetch).

-export([fetch/2]).

-type path() :: [path_element()].
-type path_element() :: atom() | integer() | '*' | [extract_spec()].
-type extract_spec() :: atom() | '*' | [path_element()].

-export_type([path/0]).

%% @doc Fetch a value from a FlatBuffer context using a path.
%%
%% Returns `undefined' for missing single values, `[]' for wildcards with no matches.
%% Raises on programmer errors (unknown field, type mismatch).
-spec fetch(flatbuferl:ctx(), path()) -> term() | undefined.
fetch(Ctx, Path) ->
    {Buffer, Defs, RootType, Root} = unpack_ctx(Ctx),
    case do_fetch(Root, Defs, RootType, Path, Buffer) of
        {ok, Value} -> Value;
        missing -> undefined;
        filtered_out -> undefined;
        {list, Values} -> Values
    end.

%% Extract fields from opaque ctx record
unpack_ctx(Ctx) ->
    %% Access ctx record fields via flatbuferl module
    %% The ctx record is: {ctx, buffer, defs, root_type, root}
    {ctx, Buffer, Defs, RootType, Root} = Ctx,
    {Buffer, Defs, RootType, Root}.

%% Main dispatch
do_fetch(TableRef, Defs, TableType, [Elem], Buffer) ->
    fetch_terminal(TableRef, Defs, TableType, Elem, Buffer);
do_fetch(TableRef, Defs, TableType, [Elem | Rest], Buffer) ->
    fetch_traverse(TableRef, Defs, TableType, Elem, Rest, Buffer).

%% Terminal element (last in path)
fetch_terminal(TableRef, Defs, TableType, FieldName, Buffer) when
    is_atom(FieldName), FieldName /= '*'
->
    fetch_field(TableRef, Defs, TableType, FieldName, Buffer);
fetch_terminal(_TableRef, _Defs, TableType, Index, _Buffer) when is_integer(Index) ->
    %% Index at terminal - this is an error, can't index into a table
    error({not_a_vector, TableType, Index});
fetch_terminal(TableRef, Defs, TableType, '*', Buffer) ->
    %% Wildcard at terminal on table - return as map
    to_map(TableRef, Defs, TableType, Buffer);
fetch_terminal(TableRef, Defs, TableType, ExtractSpec, Buffer) when is_list(ExtractSpec) ->
    %% Multi-field extraction
    extract_fields(TableRef, Defs, TableType, ExtractSpec, Buffer).

%% Traverse to next element
fetch_traverse(TableRef, Defs, TableType, FieldName, Rest, Buffer) when
    is_atom(FieldName), FieldName /= '*'
->
    case lookup_field(Defs, TableType, FieldName) of
        {ok, FieldId, {vector, _} = VecType, _Default} ->
            traverse_vector(TableRef, FieldId, VecType, Defs, Rest, Buffer);
        {ok, _FieldId, {array, _ElemType, Count}, _Default} when Rest == ['_size'] ->
            {ok, Count};
        {ok, FieldId, string, _Default} when Rest == ['_size'] ->
            case flatbuferl_reader:get_field(TableRef, FieldId, string, Buffer) of
                {ok, Bin} -> {ok, byte_size(Bin)};
                missing -> {ok, 0}
            end;
        {ok, FieldId, {union_value, UnionName}, _Default} ->
            traverse_union(TableRef, FieldId, UnionName, Defs, Rest, Buffer);
        {ok, FieldId, NestedType, _Default} when is_atom(NestedType) ->
            case maps:get(NestedType, Defs, undefined) of
                {table, _} ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, NestedType, Buffer) of
                        {ok, NestedTableRef} ->
                            do_fetch(NestedTableRef, Defs, NestedType, Rest, Buffer);
                        missing ->
                            missing
                    end;
                _ ->
                    error({not_a_table, FieldName, NestedType})
            end;
        {ok, FieldId, {struct, Fields}, _Default} ->
            case flatbuferl_reader:get_field(TableRef, FieldId, {struct, Fields}, Buffer) of
                {ok, StructMap} ->
                    fetch_from_struct(StructMap, Rest);
                missing ->
                    missing
            end;
        {ok, _FieldId, Type, _Default} ->
            error({not_a_table, FieldName, Type});
        error ->
            error({unknown_field, FieldName})
    end;
fetch_traverse(_TableRef, _Defs, _TableType, Index, _Rest, _Buffer) when is_integer(Index) ->
    error({not_a_vector, index_on_table}).

%% Union traversal
traverse_union(TableRef, FieldId, UnionName, Defs, Rest, Buffer) ->
    TypeFieldId = FieldId - 1,
    case flatbuferl_reader:get_field(TableRef, TypeFieldId, {union_type, UnionName}, Buffer) of
        {ok, 0} ->
            %% NONE type
            missing;
        {ok, TypeIndex} ->
            {union, Members} = maps:get(UnionName, Defs),
            MemberType = lists:nth(TypeIndex, Members),
            case Rest of
                ['_type'] ->
                    {ok, MemberType};
                ['_type' | MoreRest] ->
                    %% '_type' followed by more path - doesn't make sense
                    error({cannot_traverse, '_type', MoreRest});
                [ExtractSpec] when is_list(ExtractSpec) ->
                    %% Extraction spec directly after union - pass union type context
                    case flatbuferl_reader:get_field(TableRef, FieldId, {union_value, UnionName}, Buffer) of
                        {ok, ValueRef} ->
                            extract_fields(ValueRef, Defs, MemberType, ExtractSpec, Buffer, #{union_type => MemberType});
                        missing ->
                            missing
                    end;
                _ ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, {union_value, UnionName}, Buffer) of
                        {ok, ValueRef} ->
                            do_fetch(ValueRef, Defs, MemberType, Rest, Buffer);
                        missing ->
                            missing
                    end
            end;
        missing ->
            missing
    end.

%% Vector traversal
traverse_vector(TableRef, FieldId, {vector, ElemType}, Defs, Rest, Buffer) ->
    case flatbuferl_reader:get_vector_info(TableRef, FieldId, {vector, ElemType}, Buffer) of
        {ok, VecInfo} ->
            traverse_vector_with_info(VecInfo, ElemType, Defs, Rest, Buffer);
        missing ->
            case Rest of
                ['*' | _] -> {list, []};
                ['_size'] -> {ok, 0};
                [Index | _] when is_integer(Index) -> missing;
                _ -> missing
            end
    end.

traverse_vector_with_info({Length, _Start, _ElemType}, _ElemTypeOuter, _Defs, ['_size'], _Buffer) ->
    {ok, Length};
traverse_vector_with_info(VecInfo, ElemType, Defs, [Index | Rest], Buffer) when is_integer(Index) ->
    case flatbuferl_reader:get_vector_element_at(VecInfo, Index, Buffer) of
        {ok, Elem} ->
            continue_from_element(Elem, ElemType, Defs, Rest, Buffer);
        missing ->
            missing
    end;
traverse_vector_with_info(
    {Length, _Start, ElemType} = VecInfo, _ElemTypeOuter, Defs, ['*' | Rest], Buffer
) ->
    Results = wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, 0, []),
    {list, Results};
traverse_vector_with_info(_VecInfo, _ElemType, _Defs, [Other | _], _Buffer) ->
    error({invalid_path_element, Other}).

wildcard_over_vector(_VecInfo, Length, _ElemType, _Defs, _Rest, _Buffer, Idx, Acc) when
    Idx >= Length
->
    lists:reverse(Acc);
wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx, Acc) ->
    {ok, Elem} = flatbuferl_reader:get_vector_element_at(VecInfo, Idx, Buffer),
    case continue_from_element(Elem, ElemType, Defs, Rest, Buffer) of
        {ok, Value} ->
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, [
                Value | Acc
            ]);
        {list, Values} ->
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, [
                Values | Acc
            ]);
        missing ->
            %% Filter out missing elements
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, Acc);
        filtered_out ->
            %% Guard failed - filter out this element
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, Acc)
    end.

continue_from_element(Elem, ElemType, Defs, [], Buffer) when is_atom(ElemType) ->
    %% Check if it's a table type
    case maps:get(ElemType, Defs, undefined) of
        {table, _} ->
            to_map(Elem, Defs, ElemType, Buffer);
        _ ->
            %% Scalar type (string, int, etc.)
            {ok, Elem}
    end;
continue_from_element(Elem, _ElemType, _Defs, [], _Buffer) ->
    %% Scalar/struct element, no more path
    {ok, Elem};
continue_from_element(Elem, ElemType, Defs, Rest, Buffer) when is_atom(ElemType) ->
    %% Check if it's a table type
    case maps:get(ElemType, Defs, undefined) of
        {table, _} ->
            do_fetch(Elem, Defs, ElemType, Rest, Buffer);
        _ ->
            %% Scalar can't be traversed
            error({cannot_traverse, ElemType, Rest})
    end;
continue_from_element(StructMap, {struct, _Fields}, _Defs, Rest, _Buffer) when is_map(StructMap) ->
    fetch_from_struct(StructMap, Rest);
continue_from_element(_Elem, ElemType, _Defs, Rest, _Buffer) ->
    error({cannot_traverse, ElemType, Rest}).

%% Struct traversal (structs are already maps)
fetch_from_struct(StructMap, [FieldName]) when is_atom(FieldName), FieldName /= '*' ->
    case maps:find(FieldName, StructMap) of
        {ok, Value} -> {ok, Value};
        error -> missing
    end;
fetch_from_struct(StructMap, ['*']) ->
    {ok, StructMap};
fetch_from_struct(StructMap, [ExtractSpec]) when is_list(ExtractSpec) ->
    Values = [
        case maps:find(F, StructMap) of
            {ok, V} -> V;
            error -> undefined
        end
     || F <- ExtractSpec, is_atom(F), F /= '*'
    ],
    {ok, Values};
fetch_from_struct(StructMap, [FieldName | Rest]) when is_atom(FieldName) ->
    case maps:find(FieldName, StructMap) of
        {ok, NestedStruct} when is_map(NestedStruct) ->
            fetch_from_struct(NestedStruct, Rest);
        {ok, _} ->
            error({not_a_struct, FieldName});
        error ->
            missing
    end.

%% Field lookup
fetch_field(TableRef, Defs, TableType, FieldName, Buffer) ->
    case lookup_field(Defs, TableType, FieldName) of
        {ok, FieldId, Type, Default} ->
            ResolvedType = resolve_type(Type, Defs),
            case flatbuferl_reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                {ok, Value} ->
                    {ok, convert_value(Value, Type, Defs, Buffer)};
                missing when Default /= undefined ->
                    {ok, Default};
                missing ->
                    missing
            end;
        error ->
            error({unknown_field, FieldName})
    end.

%% Multi-field extraction
extract_fields(TableRef, Defs, TableType, Specs, Buffer) ->
    extract_fields(TableRef, Defs, TableType, Specs, Buffer, #{}).

extract_fields(TableRef, Defs, TableType, Specs, Buffer, Context) ->
    {Guards, Extractors} = partition_specs(Specs),
    case check_guards(Guards, TableRef, Defs, TableType, Buffer, Context) of
        true ->
            Values = lists:map(
                fun(Spec) -> extract_one(Spec, TableRef, Defs, TableType, Buffer) end,
                Extractors
            ),
            {ok, Values};
        false ->
            filtered_out
    end.

partition_specs(Specs) ->
    lists:partition(fun is_guard/1, Specs).

is_guard({Field, _Value}) when is_atom(Field) -> true;
is_guard({Path, _Value}) when is_list(Path) -> true;
is_guard(_) -> false.

check_guards([], _TableRef, _Defs, _TableType, _Buffer, _Context) ->
    true;
check_guards([{'_type', ExpectedType} | Rest], TableRef, Defs, TableType, Buffer, Context) ->
    %% Check union type from context
    case maps:get(union_type, Context, undefined) of
        ExpectedType ->
            check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
        _ ->
            false
    end;
check_guards([{Field, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when is_atom(Field) ->
    case fetch_field(TableRef, Defs, TableType, Field, Buffer) of
        {ok, ExpectedValue} ->
            check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
        {ok, _Other} ->
            false;
        missing ->
            false
    end;
check_guards([{Path, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when is_list(Path) ->
    case do_fetch(TableRef, Defs, TableType, Path, Buffer) of
        {ok, ExpectedValue} ->
            check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
        {ok, _Other} ->
            false;
        _ ->
            false
    end.

extract_one('*', TableRef, Defs, TableType, Buffer) ->
    {ok, Map} = to_map(TableRef, Defs, TableType, Buffer),
    Map;
extract_one(FieldName, TableRef, Defs, TableType, Buffer) when is_atom(FieldName) ->
    case fetch_field(TableRef, Defs, TableType, FieldName, Buffer) of
        {ok, V} -> V;
        missing -> undefined
    end;
extract_one(SubPath, TableRef, Defs, TableType, Buffer) when is_list(SubPath) ->
    case do_fetch(TableRef, Defs, TableType, SubPath, Buffer) of
        {ok, V} -> V;
        {list, V} -> V;
        missing -> undefined
    end.

%% Field lookup helper
lookup_field(Defs, TableType, FieldName) ->
    {table, Fields} = maps:get(TableType, Defs),
    find_field(Fields, FieldName).

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
    Type;
normalize_type(Type) ->
    Type.

%% Type resolution
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

%% Convert table refs to maps
to_map(TableRef, Defs, TableType, Buffer) ->
    {table, Fields} = maps:get(TableType, Defs),
    Map = lists:foldl(
        fun
            ({FieldName, Type, Attrs}, Acc) ->
                FieldId = maps:get(id, Attrs),
                ResolvedType = resolve_type(normalize_type(Type), Defs),
                case flatbuferl_reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                    {ok, Value} ->
                        Acc#{FieldName => convert_value(Value, Type, Defs, Buffer)};
                    missing ->
                        case extract_default(Type) of
                            undefined -> Acc;
                            Default -> Acc#{FieldName => Default}
                        end
                end;
            ({FieldName, Type}, Acc) ->
                ResolvedType = resolve_type(normalize_type(Type), Defs),
                case flatbuferl_reader:get_field(TableRef, 0, ResolvedType, Buffer) of
                    {ok, Value} ->
                        Acc#{FieldName => convert_value(Value, Type, Defs, Buffer)};
                    missing ->
                        Acc
                end
        end,
        #{},
        Fields
    ),
    {ok, Map}.

convert_value({table, _, _} = TableRef, Type, Defs, Buffer) when is_atom(Type) ->
    case to_map(TableRef, Defs, Type, Buffer) of
        {ok, Map} -> Map
    end;
convert_value(Values, {vector, ElemType}, Defs, Buffer) when is_list(Values), is_atom(ElemType) ->
    case maps:get(ElemType, Defs, undefined) of
        {table, _} ->
            [
                case to_map(V, Defs, ElemType, Buffer) of
                    {ok, M} -> M
                end
             || V <- Values
            ];
        _ ->
            Values
    end;
convert_value(Value, _Type, _Defs, _Buffer) ->
    Value.
