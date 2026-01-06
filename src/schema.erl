-module(schema).
-export([parse/1, parse_file/1, process/1]).

-type type_name() :: atom().
-type field_def() :: {atom(), atom() | tuple()} | {atom(), atom() | tuple(), map()}.
-type table_def() :: {table, [field_def()]}.
-type enum_def() :: {{enum, atom()}, [atom()]}.
-type union_def() :: {union, [atom()]}.
-type definitions() :: #{type_name() => table_def() | enum_def() | union_def()}.
-type options() :: #{
    namespace => atom(),
    root_type => atom(),
    file_identifier => binary(),
    file_extension => binary(),
    include => binary(),
    attribute => binary()
}.

-export_type([definitions/0, options/0, field_def/0]).

%% Parse a schema string
-spec parse(string() | binary()) ->
    {ok, {Definitions :: definitions(), Options :: options()}} | {error, term()}.
parse(Schema) when is_binary(Schema) ->
    parse(binary_to_list(Schema));
parse(Schema) when is_list(Schema) ->
    case schema_lexer:string(Schema) of
        {ok, Tokens, _} ->
            case schema_parser:parse(Tokens) of
                {ok, Parsed} -> {ok, process(Parsed)};
                {error, _} = Err -> Err
            end;
        {error, _, _} = Err ->
            {error, Err}
    end.

%% Parse a schema file
-spec parse_file(file:filename()) -> {ok, {map(), map()}} | {error, term()}.
parse_file(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} -> parse(Contents);
        {error, _} = Err -> Err
    end.

%% Post-process parsed schema: assign field IDs, validate
-spec process({map(), map()}) -> {map(), map()}.
process({Defs, Opts}) ->
    ProcessedDefs = maps:map(fun(_Name, Def) -> process_def(Def, Defs) end, Defs),
    {ProcessedDefs, Opts}.

process_def({table, Fields}, Defs) ->
    %% Expand union fields into type + value pairs before assigning IDs
    ExpandedFields = expand_union_fields(Fields, Defs),
    {table, assign_field_ids(ExpandedFields)};
process_def(Other, _Defs) ->
    Other.

%% Expand union fields into type field + value field
expand_union_fields(Fields, Defs) ->
    lists:flatmap(
        fun(Field) ->
            {Name, Type, Attrs} = normalize_field(Field),
            case maps:get(Type, Defs, undefined) of
                {union, _Members} ->
                    %% Union field becomes two fields: name_type and name
                    TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
                    [
                        {TypeFieldName, {union_type, Type}, Attrs},
                        {Name, {union_value, Type}, Attrs}
                    ];
                _ ->
                    [Field]
            end
        end,
        Fields
    ).

normalize_field({Name, Type}) -> {Name, Type, #{}};
normalize_field({Name, Type, Attrs}) -> {Name, Type, Attrs}.

%% Assign sequential IDs to fields, respecting explicit IDs
assign_field_ids(Fields) ->
    %% First pass: collect explicit IDs
    ExplicitIds = lists:foldl(
        fun(Field, Acc) ->
            case get_explicit_id(Field) of
                undefined -> Acc;
                Id -> sets:add_element(Id, Acc)
            end
        end,
        sets:new(),
        Fields
    ),

    %% Second pass: assign IDs, filling gaps
    {Processed, _} = lists:mapfoldl(
        fun(Field, NextCandidate) ->
            case get_explicit_id(Field) of
                undefined ->
                    %% Find next available ID starting from NextCandidate
                    AvailableId = find_next_id(NextCandidate, ExplicitIds),
                    {set_field_id(Field, AvailableId), AvailableId + 1};
                _ExplicitId ->
                    %% Field already has ID, don't change NextCandidate
                    {Field, NextCandidate}
            end
        end,
        0,
        Fields
    ),
    Processed.

get_explicit_id({_Name, _Type, Attrs}) when is_map(Attrs) ->
    maps:get(id, Attrs, undefined);
get_explicit_id({_Name, _Type}) ->
    undefined.

set_field_id({Name, Type, Attrs}, Id) ->
    {Name, Type, Attrs#{id => Id}};
set_field_id({Name, Type}, Id) ->
    {Name, Type, #{id => Id}}.

find_next_id(Candidate, ExplicitIds) ->
    case sets:is_element(Candidate, ExplicitIds) of
        true -> find_next_id(Candidate + 1, ExplicitIds);
        false -> Candidate
    end.
