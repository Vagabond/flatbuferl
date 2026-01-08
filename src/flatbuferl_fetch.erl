%% @doc Path-based access to FlatBuffer data.
%%
%% This module provides {@link fetch/2} for flexible path-based access to
%% FlatBuffer data with support for vector indexing, wildcards, multi-field
%% extraction, guards, and union type handling.
%%
%% == Path Elements ==
%%
%% A path is a list of elements that navigate through the FlatBuffer structure:
%%
%% <ul>
%% <li><b>atom</b> - Field name</li>
%% <li><b>integer</b> - Vector/array index (negative counts from end)</li>
%% <li><b>'*'</b> - Wildcard, iterates all elements in vector/array</li>
%% <li><b>'_type'</b> - Union type discriminator (pseudo-field)</li>
%% <li><b>'_size'</b> - Vector/array/string length (pseudo-field)</li>
%% <li><b>[spec]</b> - Multi-field extraction list</li>
%% </ul>
%%
%% == Basic Field Access ==
%%
%% ```
%% fetch(Ctx, [name])              -> <<"Bob">>
%% fetch(Ctx, [pos, x])            -> 1.0
%% fetch(Ctx, [nonexistent])       -> undefined
%% '''
%%
%% == Vector/Array Indexing ==
%%
%% ```
%% fetch(Ctx, [monsters, 0])       -> #{name => <<"Goblin">>, ...}
%% fetch(Ctx, [monsters, 0, name]) -> <<"Goblin">>
%% fetch(Ctx, [monsters, -1, name])-> <<"Dragon">>  % last element
%% fetch(Ctx, [monsters, 100])     -> undefined     % out of bounds
%% '''
%%
%% == Wildcards ==
%%
%% The wildcard '*' iterates all elements. Result is always a list.
%%
%% ```
%% fetch(Ctx, [monsters, '*'])           -> [#{...}, #{...}, ...]
%% fetch(Ctx, [monsters, '*', name])     -> [<<"Goblin">>, <<"Orc">>]
%% fetch(Ctx, [monsters, '*', stats, hp])-> [30, 5000]  % nested access
%% '''
%%
%% Elements where the traversal returns missing data are filtered out:
%%
%% ```
%% %% If some monsters have no stats table set:
%% fetch(Ctx, [monsters, '*', stats, hp])-> [5000]  % only monsters with stats
%% '''
%%
%% == Multi-Field Extraction ==
%%
%% List as final element extracts multiple fields:
%%
%% ```
%% fetch(Ctx, [monsters, 0, [name, hp]])     -> [<<"Goblin">>, 10]
%% fetch(Ctx, [monsters, '*', [name, hp]])   -> [[<<"Goblin">>, 10], ...]
%% '''
%%
%% Nested paths in extraction:
%%
%% ```
%% fetch(Ctx, [monsters, '*', [name, [pos, x]]])
%% %% -> [[<<"Goblin">>, 1.0], [<<"Orc">>, 2.0], ...]
%% '''
%%
%% Use '*' in extraction to get the whole element as a map:
%%
%% ```
%% fetch(Ctx, [monsters, 0, [name, '*']])
%% %% -> [<<"Goblin">>, #{name => <<"Goblin">>, hp => 10, ...}]
%% '''
%%
%% == Guards (Filters) ==
%%
%% Tuples in extraction act as guards. Guards filter but produce no output.
%%
%% Equality guard - `{field, value}':
%%
%% ```
%% fetch(Ctx, [monsters, '*', [{is_boss, true}, name]])
%% %% -> [[<<"Dragon Lord">>]]  % only boss monsters
%% '''
%%
%% Comparison guard - `{field, Op, value}' where Op is one of:
%% '>', '>=', '&lt;', '=&lt;', '==', '/=', in, not_in
%%
%% ```
%% fetch(Ctx, [monsters, '*', [{hp, '>', 50}, name]])
%% %% -> [[<<"Dragon">>], [<<"Ogre">>]]
%%
%% fetch(Ctx, [items, '*', [{rarity, in, [rare, epic]}, name]])
%% %% -> items with rarity in the given list
%% '''
%%
%% Multiple guards are ANDed together.
%%
%% == Unions ==
%%
%% Use '_type' to get the union type discriminator:
%%
%% ```
%% fetch(Ctx, [equipped, '_type'])           -> 'Weapon'
%% fetch(Ctx, [equipped, name])              -> <<"Sword">>  % auto-resolves
%% fetch(Ctx, [equipped, ['_type', name]])   -> ['Weapon', <<"Sword">>]
%% '''
%%
%% Filter by union type:
%%
%% ```
%% fetch(Ctx, [inventory, '*', [{'_type', 'Weapon'}, name, damage]])
%% %% -> [[<<"Sword">>, 10], [<<"Axe">>, 25]]
%% '''
%%
%% When iterating union vectors, elements are filtered if the requested
%% field doesn't exist on that union member's type:
%%
%% ```
%% %% inventory contains Weapon, Armor, and Consumable items
%% %% only Weapon has 'damage' field
%% fetch(Ctx, [inventory, '*', item, damage])
%% %% -> [50, 25]  % only weapons, others filtered out
%% '''
%%
%% == Size Pseudo-field ==
%%
%% The '_size' pseudo-field returns the length of vectors, arrays, or strings:
%%
%% ```
%% fetch(Ctx, [monsters, '_size'])   -> 3
%% fetch(Ctx, [name, '_size'])       -> 5  % string length
%% '''
%%
%% == Error Conditions ==
%%
%% Errors are raised when the path is incompatible with the schema
%% (the lookup is impossible regardless of the data):
%%
%% <ul>
%% <li><b>{unknown_field, Name}</b> - Field does not exist in schema</li>
%% <li><b>{not_a_table, Name, Type}</b> - Path continues into a scalar field</li>
%% <li><b>{not_a_vector, Name, Type}</b> - Index or wildcard on non-vector field</li>
%% <li><b>{invalid_path_element, Elem}</b> - Unrecognized element in path</li>
%% </ul>
%%
%% These are programmer errors indicating a bug in the path specification,
%% not runtime conditions. Missing data returns `undefined' or filters out.
%%
%% @end
-module(flatbuferl_fetch).

-include("flatbuferl_records.hrl").

-export([fetch/2]).

-type path() :: [path_element()].
%% A list of path elements for navigating FlatBuffer data.

-type path_element() :: atom() | integer() | '*' | [extract_spec()].
%% An element in a fetch path.
%% Can be a field name (atom), vector index (integer, negative from end),
%% wildcard ('*'), or extraction spec (list).

-type extract_spec() ::
    atom()
    | '*'
    | [path_element()]
    | {atom(), term()}
    | {atom(), guard_op(), term()}
    | {[path_element()], term()}
    | {[path_element()], guard_op(), term()}.
%% Specification for multi-field extraction.
%% Can be a field name, nested path, '*' for whole element, or guard tuple.

-type guard_op() :: '>' | '>=' | '<' | '=<' | '==' | '/=' | in | not_in.
%% Comparison operators for guards.

-export_type([path/0, path_element/0, extract_spec/0, guard_op/0]).

%% @doc Fetch a value from a FlatBuffer context using a path.
%%
%% Navigates through the FlatBuffer structure following the path elements.
%% Supports field access, vector indexing, wildcards, multi-field extraction,
%% and filtering with guards.
%%
%% Returns:
%% <ul>
%% <li>The value at the path for successful single access</li>
%% <li>`undefined' for missing fields or out-of-bounds indices</li>
%% <li>A list for wildcard access (possibly empty if all filtered)</li>
%% <li>A list of values for multi-field extraction</li>
%% </ul>
%%
%% Raises exceptions when the path is incompatible with the schema (the
%% lookup is impossible regardless of data). Missing data at runtime
%% returns `undefined' or filters out in wildcards.
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
        {ok, FieldId, {array, ElemType, Count} = ArrayType, _Default} ->
            traverse_array(TableRef, FieldId, ArrayType, ElemType, Count, Defs, Rest, Buffer);
        {ok, FieldId, string, _Default} when Rest == ['_size'] ->
            case flatbuferl_reader:get_field(TableRef, FieldId, string, Buffer) of
                {ok, Bin} -> {ok, byte_size(Bin)};
                missing -> {ok, 0}
            end;
        {ok, FieldId, {union_value, UnionName}, _Default} ->
            traverse_union(TableRef, FieldId, UnionName, Defs, Rest, Buffer);
        {ok, FieldId, NestedType, _Default} when is_atom(NestedType) ->
            case maps:get(NestedType, Defs, undefined) of
                #table_def{} ->
                    case flatbuferl_reader:get_field(TableRef, FieldId, NestedType, Buffer) of
                        {ok, NestedTableRef} ->
                            do_fetch(NestedTableRef, Defs, NestedType, Rest, Buffer);
                        missing ->
                            missing
                    end;
                #struct_def{} = StructDef ->
                    %% Named struct type - look up and fetch from struct data
                    case flatbuferl_reader:get_field(TableRef, FieldId, StructDef, Buffer) of
                        {ok, StructMap} ->
                            fetch_from_struct(StructMap, Rest);
                        missing ->
                            missing
                    end;
                {struct, Fields} ->
                    %% Named struct type - look up and fetch from struct data
                    case flatbuferl_reader:get_field(TableRef, FieldId, {struct, Fields}, Buffer) of
                        {ok, StructMap} ->
                            fetch_from_struct(StructMap, Rest);
                        missing ->
                            missing
                    end;
                _ ->
                    error({not_a_table, FieldName, NestedType})
            end;
        {ok, FieldId, #struct_def{} = StructDef, _Default} ->
            case flatbuferl_reader:get_field(TableRef, FieldId, StructDef, Buffer) of
                {ok, StructMap} ->
                    fetch_from_struct(StructMap, Rest);
                missing ->
                    missing
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
            {union, Members, _} = maps:get(UnionName, Defs),
            MemberType = lists:nth(TypeIndex, Members),
            case Rest of
                ['_type'] ->
                    {ok, MemberType};
                ['_type' | MoreRest] ->
                    %% '_type' followed by more path - doesn't make sense
                    error({cannot_traverse, '_type', MoreRest});
                [ExtractSpec] when is_list(ExtractSpec) ->
                    %% Extraction spec directly after union - pass union type context
                    case
                        flatbuferl_reader:get_field(
                            TableRef, FieldId, {union_value, UnionName}, Buffer
                        )
                    of
                        {ok, ValueRef} ->
                            extract_fields(ValueRef, Defs, MemberType, ExtractSpec, Buffer, #{
                                union_type => MemberType
                            });
                        missing ->
                            missing
                    end;
                [FieldName | _] when is_atom(FieldName), FieldName /= '*' ->
                    %% Path continues with a field name. First validate the field
                    %% exists on at least one union member (schema validation).
                    case any_union_member_has_field(Members, FieldName, Defs) of
                        false ->
                            error({unknown_field, FieldName});
                        true ->
                            %% Field exists on some member. Check if THIS member has it.
                            case type_has_field(MemberType, FieldName, Defs) of
                                false ->
                                    %% This member doesn't have it - filter out
                                    missing;
                                true ->
                                    case
                                        flatbuferl_reader:get_field(
                                            TableRef, FieldId, {union_value, UnionName}, Buffer
                                        )
                                    of
                                        {ok, ValueRef} ->
                                            do_fetch(ValueRef, Defs, MemberType, Rest, Buffer);
                                        missing ->
                                            missing
                                    end
                            end
                    end;
                _ ->
                    case
                        flatbuferl_reader:get_field(
                            TableRef, FieldId, {union_value, UnionName}, Buffer
                        )
                    of
                        {ok, ValueRef} ->
                            do_fetch(ValueRef, Defs, MemberType, Rest, Buffer);
                        missing ->
                            missing
                    end
            end;
        missing ->
            missing
    end.

%% Array traversal (fixed-size arrays)
traverse_array(_TableRef, _FieldId, _ArrayType, _ElemType, Count, _Defs, ['_size'], _Buffer) ->
    {ok, Count};
traverse_array(TableRef, FieldId, ArrayType, ElemType, Count, Defs, Rest, Buffer) ->
    case flatbuferl_reader:get_field(TableRef, FieldId, ArrayType, Buffer) of
        {ok, Elements} when is_list(Elements) ->
            traverse_array_elements(Elements, ElemType, Count, Defs, Rest, Buffer);
        missing ->
            case Rest of
                ['*' | _] -> {list, []};
                [Index | _] when is_integer(Index) -> missing;
                _ -> missing
            end
    end.

traverse_array_elements(_Elements, _ElemType, Count, _Defs, ['_size'], _Buffer) ->
    {ok, Count};
traverse_array_elements(Elements, ElemType, Count, Defs, [Index | Rest], Buffer) when
    is_integer(Index)
->
    %% Handle negative indices
    ActualIndex =
        if
            Index < 0 -> Count + Index;
            true -> Index
        end,
    case ActualIndex >= 0 andalso ActualIndex < Count of
        true ->
            Elem = lists:nth(ActualIndex + 1, Elements),
            continue_from_element(Elem, ElemType, Defs, Rest, Buffer);
        false ->
            missing
    end;
traverse_array_elements(Elements, ElemType, _Count, Defs, ['*' | Rest], Buffer) ->
    Results = lists:filtermap(
        fun(Elem) ->
            case continue_from_element(Elem, ElemType, Defs, Rest, Buffer) of
                {ok, Value} -> {true, Value};
                {list, Values} -> {true, Values};
                missing -> false;
                filtered_out -> false
            end
        end,
        Elements
    ),
    {list, Results};
traverse_array_elements(_Elements, _ElemType, _Count, _Defs, [Other | _], _Buffer) ->
    error({invalid_path_element, Other}).

%% Vector traversal
traverse_vector(
    TableRef, FieldId, {vector, {union_value, UnionName} = ElemType}, Defs, Rest, Buffer
) ->
    %% Union vectors have parallel type and value vectors
    TypeFieldId = FieldId - 1,
    TypeVecType = {vector, {union_type, UnionName}},
    case flatbuferl_reader:get_vector_info(TableRef, FieldId, {vector, ElemType}, Buffer) of
        {ok, ValVecInfo} ->
            case flatbuferl_reader:get_vector_info(TableRef, TypeFieldId, TypeVecType, Buffer) of
                {ok, TypeVecInfo} ->
                    traverse_union_vector(ValVecInfo, TypeVecInfo, UnionName, Defs, Rest, Buffer);
                missing ->
                    %% Type vector missing - shouldn't happen in valid data
                    missing
            end;
        missing ->
            case Rest of
                ['*' | _] -> {list, []};
                ['_size'] -> {ok, 0};
                [Index | _] when is_integer(Index) -> missing;
                _ -> missing
            end
    end;
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

%% Union vector traversal
traverse_union_vector({Length, _, _}, _TypeVecInfo, _UnionName, _Defs, ['_size'], _Buffer) ->
    {ok, Length};
traverse_union_vector(
    {Length, _, _} = ValVecInfo, TypeVecInfo, UnionName, Defs, [Index | Rest], Buffer
) when is_integer(Index) ->
    %% Handle negative indices
    ActualIndex =
        if
            Index < 0 -> Length + Index;
            true -> Index
        end,
    case ActualIndex >= 0 andalso ActualIndex < Length of
        true ->
            {ok, TypeIndex} = flatbuferl_reader:get_vector_element_at(
                TypeVecInfo, ActualIndex, Buffer
            ),
            {ok, ValueRef} = flatbuferl_reader:get_vector_element_at(
                ValVecInfo, ActualIndex, Buffer
            ),
            {union, Members, _} = maps:get(UnionName, Defs),
            MemberType = lists:nth(TypeIndex, Members),
            continue_from_union_element(ValueRef, MemberType, Defs, Rest, Buffer);
        false ->
            missing
    end;
traverse_union_vector(
    {Length, _, _} = ValVecInfo, TypeVecInfo, UnionName, Defs, ['*' | Rest], Buffer
) ->
    Results = wildcard_over_union_vector(
        ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, 0, []
    ),
    {list, Results};
traverse_union_vector(_ValVecInfo, _TypeVecInfo, _UnionName, _Defs, [Other | _], _Buffer) ->
    error({invalid_path_element, Other}).

wildcard_over_union_vector(
    _ValVecInfo, _TypeVecInfo, Length, _UnionName, _Defs, _Rest, _Buffer, Idx, Acc
) when Idx >= Length ->
    lists:reverse(Acc);
wildcard_over_union_vector(
    ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, Idx, Acc
) ->
    {ok, TypeIndex} = flatbuferl_reader:get_vector_element_at(TypeVecInfo, Idx, Buffer),
    {ok, ValueRef} = flatbuferl_reader:get_vector_element_at(ValVecInfo, Idx, Buffer),
    {union, Members, _} = maps:get(UnionName, Defs),
    MemberType = lists:nth(TypeIndex, Members),
    %% Catch unknown_field errors - different union members have different fields
    Result =
        try
            continue_from_union_element(ValueRef, MemberType, Defs, Rest, Buffer)
        catch
            error:{unknown_field, _} -> filtered_out
        end,
    case Result of
        {ok, Value} ->
            wildcard_over_union_vector(
                ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, Idx + 1, [
                    Value | Acc
                ]
            );
        {list, Values} ->
            wildcard_over_union_vector(
                ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, Idx + 1, [
                    Values | Acc
                ]
            );
        missing ->
            wildcard_over_union_vector(
                ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, Idx + 1, Acc
            );
        filtered_out ->
            wildcard_over_union_vector(
                ValVecInfo, TypeVecInfo, Length, UnionName, Defs, Rest, Buffer, Idx + 1, Acc
            )
    end.

continue_from_union_element(ValueRef, MemberType, Defs, [], Buffer) ->
    %% No more path - return as map
    to_map(ValueRef, Defs, MemberType, Buffer);
continue_from_union_element(_ValueRef, MemberType, _Defs, ['_type'], _Buffer) ->
    {ok, MemberType};
continue_from_union_element(ValueRef, MemberType, Defs, [ExtractSpec], Buffer) when
    is_list(ExtractSpec)
->
    %% Extraction spec - pass union type context
    extract_fields(ValueRef, Defs, MemberType, ExtractSpec, Buffer, #{union_type => MemberType});
continue_from_union_element(ValueRef, MemberType, Defs, Rest, Buffer) ->
    %% Continue into the union value table
    do_fetch(ValueRef, Defs, MemberType, Rest, Buffer).

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
    %% For regular (non-union) vectors, all elements have the same type.
    %% Unknown fields are schema errors and should raise, not filter.
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
            %% Filter out elements with missing data
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, Acc);
        filtered_out ->
            %% Guard failed - filter out
            wildcard_over_vector(VecInfo, Length, ElemType, Defs, Rest, Buffer, Idx + 1, Acc)
    end.

continue_from_element(Elem, ElemType, Defs, [], Buffer) when is_atom(ElemType) ->
    %% Check if it's a table type
    case maps:get(ElemType, Defs, undefined) of
        #table_def{} ->
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
        #table_def{} ->
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
                fun(Spec) -> extract_one(Spec, TableRef, Defs, TableType, Buffer, Context) end,
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
is_guard({Field, Op, _Value}) when is_atom(Field), is_atom(Op) -> true;
is_guard({Path, Op, _Value}) when is_list(Path), is_atom(Op) -> true;
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
check_guards([{Field, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when
    is_atom(Field)
->
    case fetch_field(TableRef, Defs, TableType, Field, Buffer) of
        {ok, ExpectedValue} ->
            check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
        {ok, _Other} ->
            false;
        missing ->
            false
    end;
check_guards([{Path, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when
    is_list(Path)
->
    case do_fetch(TableRef, Defs, TableType, Path, Buffer) of
        {ok, ExpectedValue} ->
            check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
        {ok, _Other} ->
            false;
        _ ->
            false
    end;
%% Comparison guards: {field, op, value}
check_guards([{Field, Op, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when
    is_atom(Field)
->
    case fetch_field(TableRef, Defs, TableType, Field, Buffer) of
        {ok, ActualValue} ->
            case compare(Op, ActualValue, ExpectedValue) of
                true -> check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
                false -> false
            end;
        missing ->
            false
    end;
check_guards([{Path, Op, ExpectedValue} | Rest], TableRef, Defs, TableType, Buffer, Context) when
    is_list(Path)
->
    case do_fetch(TableRef, Defs, TableType, Path, Buffer) of
        {ok, ActualValue} ->
            case compare(Op, ActualValue, ExpectedValue) of
                true -> check_guards(Rest, TableRef, Defs, TableType, Buffer, Context);
                false -> false
            end;
        _ ->
            false
    end.

compare('>', A, B) -> A > B;
compare('>=', A, B) -> A >= B;
compare('<', A, B) -> A < B;
compare('=<', A, B) -> A =< B;
compare('==', A, B) -> A == B;
compare('/=', A, B) -> A /= B;
compare(in, A, B) when is_list(B) -> lists:member(A, B);
compare(not_in, A, B) when is_list(B) -> not lists:member(A, B).

extract_one('*', TableRef, Defs, TableType, Buffer, _Context) ->
    {ok, Map} = to_map(TableRef, Defs, TableType, Buffer),
    Map;
extract_one('_type', _TableRef, _Defs, _TableType, _Buffer, Context) ->
    %% Extract union type from context
    maps:get(union_type, Context, undefined);
extract_one(FieldName, TableRef, Defs, TableType, Buffer, _Context) when is_atom(FieldName) ->
    case fetch_field(TableRef, Defs, TableType, FieldName, Buffer) of
        {ok, V} -> V;
        missing -> undefined
    end;
extract_one(SubPath, TableRef, Defs, TableType, Buffer, _Context) when is_list(SubPath) ->
    case do_fetch(TableRef, Defs, TableType, SubPath, Buffer) of
        {ok, V} -> V;
        {list, V} -> V;
        missing -> undefined
    end.

%% Field lookup helper
lookup_field(Defs, TableType, FieldName) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    find_field(Fields, FieldName).

find_field([], _Name) ->
    error;
find_field([#field_def{name = Name, id = FieldId, type = Type, default = Default} | _], Name) ->
    {ok, FieldId, Type, Default};
find_field([_ | Rest], Name) ->
    find_field(Rest, Name).

%% Check if a type has a given field
type_has_field(TypeName, FieldName, Defs) ->
    case maps:get(TypeName, Defs, undefined) of
        #table_def{all_fields = Fields} -> field_exists(Fields, FieldName);
        _ -> false
    end.

field_exists([], _Name) -> false;
field_exists([#field_def{name = Name} | _], Name) -> true;
field_exists([_ | Rest], Name) -> field_exists(Rest, Name).

%% Check if any union member has a given field
any_union_member_has_field(Members, FieldName, Defs) ->
    lists:any(fun(Member) -> type_has_field(Member, FieldName, Defs) end, Members).

%% Type resolution
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _Values} -> {enum, Base};
        #struct_def{} = StructDef -> StructDef;
        {struct, Fields} -> {struct, Fields};
        _ -> Type
    end;
resolve_type({vector, ElemType}, Defs) ->
    {vector, resolve_type(ElemType, Defs)};
resolve_type(Type, _Defs) ->
    Type.

%% Convert table refs to maps
to_map(TableRef, Defs, TableType, Buffer) ->
    #table_def{all_fields = Fields} = maps:get(TableType, Defs),
    Map = lists:foldl(
        fun(#field_def{name = FieldName, id = FieldId, type = Type, default = Default}, Acc) ->
            ResolvedType = resolve_type(Type, Defs),
            case flatbuferl_reader:get_field(TableRef, FieldId, ResolvedType, Buffer) of
                {ok, Value} ->
                    Acc#{FieldName => convert_value(Value, Type, Defs, Buffer)};
                missing ->
                    case Default of
                        undefined -> Acc;
                        _ -> Acc#{FieldName => Default}
                    end
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
        #table_def{} ->
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
