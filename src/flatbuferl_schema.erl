%% @private
-module(flatbuferl_schema).
-export([parse/1, parse_file/1, process/1, validate/3]).

-type type_name() :: atom().
-type field_def() :: {atom(), atom() | tuple()} | {atom(), atom() | tuple(), map()}.
-type table_def() :: {table, Scalars :: [map()], Refs :: [map()], AllFields :: [map()], MaxId :: integer()}.
-type enum_def() :: {{enum, atom()}, [atom()], #{atom() => non_neg_integer()}}.
-type union_def() :: {union, [atom()], #{atom() => pos_integer()}}.
-type definitions() :: #{type_name() => table_def() | enum_def() | union_def()}.
-type options() :: #{
    namespace => atom(),
    root_type => atom(),
    file_identifier => binary(),
    file_extension => binary(),
    include => [binary()],
    attribute => binary()
}.

-export_type([definitions/0, options/0, field_def/0, validate_opts/0, validation_error/0]).

%% Options for validation:
%%   unknown_fields => ignore | error  (default: ignore)
-type validate_opts() :: #{
    unknown_fields => ignore | error
}.

-type validation_error() ::
    {type_mismatch, atom(), expected_type(), term()}
    | {missing_required, atom()}
    | {unknown_field, atom()}
    | {invalid_enum, atom(), term(), [atom()]}
    | {invalid_union_type, atom(), term(), [atom()]}
    | {invalid_vector_element, atom(), non_neg_integer(), validation_error()}
    | {nested_errors, atom(), [validation_error()]}.

-type expected_type() :: atom() | {vector, atom()} | {enum, atom()} | {union, atom()}.

%% Parse a schema string
-spec parse(string() | binary()) ->
    {ok, {Definitions :: definitions(), Options :: options()}} | {error, term()}.
parse(Schema) when is_binary(Schema) ->
    parse(binary_to_list(Schema));
parse(Schema) when is_list(Schema) ->
    case flatbuferl_lexer:string(Schema) of
        {ok, Tokens, _} ->
            case flatbuferl_parser:parse(Tokens) of
                {ok, Parsed} -> {ok, process(Parsed)};
                {error, _} = Err -> Err
            end;
        {error, _, _} = Err ->
            {error, Err}
    end.

%% Parse a schema file
-spec parse_file(file:filename()) -> {ok, {map(), map()}} | {error, term()}.
parse_file(Filename) ->
    AbsPath = filename:absname(Filename),
    parse_file(AbsPath, sets:new()).

%% Internal: parse with cycle detection
-spec parse_file(file:filename(), sets:set(file:filename())) ->
    {ok, {map(), map()}} | {error, term()}.
parse_file(AbsPath, Seen) ->
    case sets:is_element(AbsPath, Seen) of
        true ->
            {error, {circular_include, AbsPath}};
        false ->
            case file:read_file(AbsPath) of
                {ok, Contents} ->
                    BaseDir = filename:dirname(AbsPath),
                    NewSeen = sets:add_element(AbsPath, Seen),
                    parse_with_includes(Contents, BaseDir, NewSeen);
                {error, _} = Err ->
                    Err
            end
    end.

%% Parse content and process includes
parse_with_includes(Schema, BaseDir, Seen) when is_binary(Schema) ->
    parse_with_includes(binary_to_list(Schema), BaseDir, Seen);
parse_with_includes(Schema, BaseDir, Seen) when is_list(Schema) ->
    case flatbuferl_lexer:string(Schema) of
        {ok, Tokens, _} ->
            case flatbuferl_parser:parse(Tokens) of
                {ok, {Defs, Opts}} ->
                    process_includes(Defs, Opts, BaseDir, Seen);
                {error, _} = Err ->
                    Err
            end;
        {error, _, _} = Err ->
            {error, Err}
    end.

%% Process include directives
process_includes(Defs, Opts, BaseDir, Seen) ->
    Includes = maps:get(include, Opts, []),
    case process_includes_list(Includes, Defs, BaseDir, Seen) of
        {ok, MergedDefs} ->
            %% Remove includes from opts (they've been processed)
            CleanOpts = maps:remove(include, Opts),
            {ok, process({MergedDefs, CleanOpts})};
        {error, _} = Err ->
            Err
    end.

process_includes_list([], Defs, _BaseDir, _Seen) ->
    {ok, Defs};
process_includes_list([Include | Rest], Defs, BaseDir, Seen) ->
    IncludePath = filename:absname(binary_to_list(Include), BaseDir),
    case parse_file(IncludePath, Seen) of
        {ok, {IncludedDefs, _IncludedOpts}} ->
            case merge_definitions(Defs, IncludedDefs) of
                {ok, MergedDefs} ->
                    process_includes_list(Rest, MergedDefs, BaseDir, Seen);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% Merge definitions, error on duplicates
merge_definitions(Defs1, Defs2) ->
    Duplicates = maps:keys(maps:with(maps:keys(Defs1), Defs2)),
    case Duplicates of
        [] ->
            {ok, maps:merge(Defs1, Defs2)};
        _ ->
            {error, {duplicate_types, Duplicates}}
    end.

%% Post-process parsed flatbuferl_schema: assign field IDs, validate
%% Two-phase processing:
%%   1. Process enums/unions to add index maps
%%   2. Process tables using enriched defs so resolve_type sees index maps
-spec process({map(), map()}) -> {map(), map()}.
process({Defs, Opts}) ->
    %% Phase 1: enrich enums and unions with precomputed index maps
    EnrichedDefs = maps:map(fun(_Name, Def) -> enrich_def(Def) end, Defs),
    %% Phase 2: process tables using enriched definitions
    ProcessedDefs = maps:map(fun(_Name, Def) -> process_def(Def, EnrichedDefs) end, EnrichedDefs),
    {ProcessedDefs, Opts}.

%% Phase 1: add index maps to enums/unions, precompute struct field offsets
enrich_def({union, Members}) ->
    IndexMap = maps:from_list(lists:zip(Members, lists:seq(1, length(Members)))),
    {union, Members, IndexMap};
enrich_def({{enum, BaseType}, Values}) ->
    IndexMap = maps:from_list(lists:zip(Values, lists:seq(0, length(Values) - 1))),
    {{enum, BaseType}, Values, IndexMap};
enrich_def({struct, Fields}) ->
    %% Precompute field offsets and sizes for efficient struct encoding
    {EnrichedFields, _, _} = lists:foldl(
        fun({Name, Type}, {Acc, Off, MaxAlign}) ->
            Size = primitive_type_size(Type),
            AlignedOff = align_to(Off, Size),
            Field = #{name => Name, type => Type, offset => AlignedOff, size => Size},
            {[Field | Acc], AlignedOff + Size, max(MaxAlign, Size)}
        end,
        {[], 0, 1},
        Fields
    ),
    {struct, lists:reverse(EnrichedFields)};
enrich_def(Other) ->
    Other.

%% Type size for struct field alignment (primitive types only)
primitive_type_size(bool) -> 1;
primitive_type_size(byte) -> 1;
primitive_type_size(ubyte) -> 1;
primitive_type_size(int8) -> 1;
primitive_type_size(uint8) -> 1;
primitive_type_size(short) -> 2;
primitive_type_size(ushort) -> 2;
primitive_type_size(int16) -> 2;
primitive_type_size(uint16) -> 2;
primitive_type_size(int) -> 4;
primitive_type_size(uint) -> 4;
primitive_type_size(int32) -> 4;
primitive_type_size(uint32) -> 4;
primitive_type_size(long) -> 8;
primitive_type_size(ulong) -> 8;
primitive_type_size(int64) -> 8;
primitive_type_size(uint64) -> 8;
primitive_type_size(float) -> 4;
primitive_type_size(float32) -> 4;
primitive_type_size(double) -> 8;
primitive_type_size(float64) -> 8;
primitive_type_size(_) -> 4.

align_to(Off, Align) ->
    case Off rem Align of
        0 -> Off;
        R -> Off + (Align - R)
    end.

%% Phase 2: process tables (enums/unions already enriched, pass through)
process_def({table, Fields}, Defs) ->
    %% Expand union fields into type + value pairs before assigning IDs
    ExpandedFields = expand_union_fields(Fields, Defs),
    %% Fix enum default values (parser stores as binary, need atom)
    NormalizedFields = normalize_enum_defaults(ExpandedFields, Defs),
    %% Assign field IDs
    FieldsWithIds = assign_field_ids(NormalizedFields),
    %% Convert to optimized map format with precomputed values
    OptimizedFields = [optimize_field(F, Defs) || F <- FieldsWithIds],
    %% Pre-sort fields by layout order (size desc, id desc) for encoding
    SortedFields = lists:sort(
        fun(#{layout_key := A}, #{layout_key := B}) -> A > B end,
        OptimizedFields
    ),
    %% Pre-partition into scalars and refs (eliminates runtime partitioning)
    %% Also precompute AllFields to avoid ++ at decode time
    {Scalars, Refs} = lists:partition(fun(#{is_scalar := S}) -> S end, SortedFields),
    AllFields = Scalars ++ Refs,
    %% Precompute max field id to avoid lists:max at encode time
    MaxId = case AllFields of
        [] -> -1;
        _ -> lists:max([maps:get(id, F) || F <- AllFields])
    end,
    {table, Scalars, Refs, AllFields, MaxId};
process_def(Other, _Defs) ->
    %% Enums, unions, structs already processed in phase 1
    Other.

%% Convert field tuple to optimized map with precomputed values
%% Pass through already-optimized map fields (from included schemas)
optimize_field(#{name := _} = Map, _Defs) ->
    Map;
optimize_field({Name, Type, Attrs}, Defs) ->
    NormalizedType = normalize_type(Type),
    Id = maps:get(id, Attrs, 0),
    InlineSize = field_inline_size(NormalizedType, Defs),
    #{
        name => Name,
        id => Id,
        type => NormalizedType,
        default => extract_default(Type),
        required => maps:get(required, Attrs, false),
        deprecated => maps:get(deprecated, Attrs, false),
        inline_size => InlineSize,
        is_scalar => is_scalar_type(NormalizedType, Defs),
        resolved_type => resolve_type(NormalizedType, Defs),
        %% Layout key for fast sorting: size * 65536 + id (both descending)
        layout_key => InlineSize * 65536 + Id
    };
optimize_field({Name, Type}, Defs) ->
    NormalizedType = normalize_type(Type),
    InlineSize = field_inline_size(NormalizedType, Defs),
    #{
        name => Name,
        id => 0,
        type => NormalizedType,
        default => extract_default(Type),
        required => false,
        deprecated => false,
        inline_size => InlineSize,
        is_scalar => is_scalar_type(NormalizedType, Defs),
        resolved_type => resolve_type(NormalizedType, Defs),
        layout_key => InlineSize * 65536
    }.

%% Determine if a type is scalar (stored inline) vs reference (stored via offset)
%% Scalars: primitives, enums, structs, fixed arrays, union type discriminator
%% References: strings, vectors, union values
is_scalar_type(string, _Defs) ->
    false;
is_scalar_type({vector, _}, _Defs) ->
    false;
is_scalar_type({union_value, _}, _Defs) ->
    false;
is_scalar_type({union_type, _}, _Defs) ->
    true;
is_scalar_type({struct, _}, _Defs) ->
    true;
is_scalar_type({array, _, _}, _Defs) ->
    true;
is_scalar_type({enum, _}, _Defs) ->
    true;
is_scalar_type({enum, _, _}, _Defs) ->
    true;
is_scalar_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {struct, _} -> true;
        {{enum, _}, _, _} -> true;
        {union, _, _} -> false;
        {table, _, _, _, _} -> false;
        %% Unprocessed table format (during phase 2 processing)
        {table, _} -> false;
        % primitive types
        undefined -> true
    end;
is_scalar_type(_, _Defs) ->
    false.

%% Normalize type: strip default value wrapper, but preserve type constructors
normalize_type({Type, _Default}) when
    is_atom(Type),
    Type /= vector,
    Type /= enum,
    Type /= struct,
    Type /= array,
    Type /= union_type,
    Type /= union_value
->
    Type;
normalize_type(Type) ->
    Type.

%% Resolve type name to its definition (for enums and structs)
%% This is precomputed into the schema for use during encoding
%% NOTE: Tables are NOT resolved - they stay as atom names for lookup in Defs
%% Only structs are resolved because struct data is encoded inline
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, _Values, IndexMap} -> {enum, normalize_scalar_type(Base), IndexMap};
        {struct, Fields} -> {struct, Fields};
        % Keep table types as atoms for Defs lookup (both processed and unprocessed)
        {table, _, _, _, _} -> Type;
        {table, _} -> Type;
        _ -> normalize_scalar_type(Type)
    end;
resolve_type({vector, ElemType}, Defs) ->
    {vector, resolve_type(ElemType, Defs)};
resolve_type({array, ElemType, Count}, Defs) ->
    {array, resolve_type(ElemType, Defs), Count};
resolve_type(Type, _Defs) ->
    Type.

%% Normalize scalar type aliases to canonical forms for faster pattern matching
%% This eliminates guard conditions like `when Type == int; Type == int32`
normalize_scalar_type(byte) -> int8;
normalize_scalar_type(ubyte) -> uint8;
normalize_scalar_type(short) -> int16;
normalize_scalar_type(ushort) -> uint16;
normalize_scalar_type(int) -> int32;
normalize_scalar_type(uint) -> uint32;
normalize_scalar_type(long) -> int64;
normalize_scalar_type(ulong) -> uint64;
normalize_scalar_type(float) -> float32;
normalize_scalar_type(double) -> float64;
normalize_scalar_type(Type) -> Type.

%% Extract default value from type, but not from type constructors
extract_default({Type, D}) when
    is_atom(Type),
    (is_number(D) orelse is_boolean(D) orelse is_atom(D)),
    Type /= vector,
    Type /= enum,
    Type /= struct,
    Type /= array,
    Type /= union_type,
    Type /= union_value
->
    D;
extract_default(_) ->
    undefined.

%% Size of field as stored inline in table (refs are 4-byte uoffsets)
field_inline_size(string, _Defs) ->
    4;
field_inline_size({vector, _}, _Defs) ->
    4;
field_inline_size({union_value, _}, _Defs) ->
    4;
field_inline_size({union_type, _}, _Defs) ->
    1;
field_inline_size({struct, Fields}, Defs) ->
    calc_struct_size(Fields, Defs);
field_inline_size({array, ElemType, Count}, Defs) ->
    type_size(ElemType, Defs) * Count;
field_inline_size({enum, Base}, Defs) ->
    type_size(Base, Defs);
field_inline_size({enum, Base, _Values}, Defs) ->
    type_size(Base, Defs);
field_inline_size(Type, Defs) when is_atom(Type) ->
    %% Check if it's a user-defined type
    case maps:get(Type, Defs, undefined) of
        {struct, Fields} -> calc_struct_size(Fields, Defs);
        {{enum, Base}, _, _} -> type_size(Base, Defs);
        _ -> type_size(Type, Defs)
    end;
field_inline_size(_, _Defs) ->
    4.

%% Type sizes
type_size(bool, _) -> 1;
type_size(byte, _) -> 1;
type_size(ubyte, _) -> 1;
type_size(int8, _) -> 1;
type_size(uint8, _) -> 1;
type_size(short, _) -> 2;
type_size(ushort, _) -> 2;
type_size(int16, _) -> 2;
type_size(uint16, _) -> 2;
type_size(int, _) -> 4;
type_size(uint, _) -> 4;
type_size(int32, _) -> 4;
type_size(uint32, _) -> 4;
type_size(long, _) -> 8;
type_size(ulong, _) -> 8;
type_size(int64, _) -> 8;
type_size(uint64, _) -> 8;
type_size(float, _) -> 4;
type_size(float32, _) -> 4;
type_size(double, _) -> 8;
type_size(float64, _) -> 8;
type_size({enum, Base}, Defs) -> type_size(Base, Defs);
type_size({enum, Base, _Values}, Defs) -> type_size(Base, Defs);
type_size({struct, Fields}, Defs) -> calc_struct_size(Fields, Defs);
type_size({array, ElemType, Count}, Defs) -> type_size(ElemType, Defs) * Count;
type_size({union_type, _}, _) -> 1;
type_size(_, _) -> 4.

%% Calculate struct size with proper alignment
%% Handles both raw tuple format {Name, Type} and enriched map format
calc_struct_size(Fields, Defs) ->
    {_, EndOffset, MaxAlign} = lists:foldl(
        fun
            (#{type := Type, offset := Offset, size := Size}, {_, _, MaxAlignAcc}) ->
                %% Enriched field - use precomputed offset and size
                {ok, Offset + Size, max(MaxAlignAcc, Size)};
            ({_Name, Type}, {_, CurOffset, MaxAlignAcc}) ->
                %% Raw tuple format
                Size = type_size(Type, Defs),
                Align = Size,
                AlignedOffset = align_offset(CurOffset, Align),
                {ok, AlignedOffset + Size, max(MaxAlignAcc, Align)}
        end,
        {ok, 0, 1},
        Fields
    ),
    align_offset(EndOffset, MaxAlign).

align_offset(Off, Align) ->
    case Off rem Align of
        0 -> Off;
        R -> Off + (Align - R)
    end.

%% Expand union fields into type field + value field
expand_union_fields(Fields, Defs) ->
    lists:flatmap(
        fun(Field) ->
            {Name, Type, Attrs} = normalize_field(Field),
            case Type of
                {vector, ElemType} ->
                    %% Check if element type is a union
                    case maps:get(ElemType, Defs, undefined) of
                        {union, _, _} ->
                            %% Vector of union becomes two vector fields
                            TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
                            [
                                {TypeFieldName, {vector, {union_type, ElemType}}, Attrs},
                                {Name, {vector, {union_value, ElemType}}, Attrs}
                            ];
                        _ ->
                            [Field]
                    end;
                _ ->
                    case maps:get(Type, Defs, undefined) of
                        {union, _, _} ->
                            %% Union field becomes two fields: name_type and name
                            TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
                            [
                                {TypeFieldName, {union_type, Type}, Attrs},
                                {Name, {union_value, Type}, Attrs}
                            ];
                        _ ->
                            [Field]
                    end
            end
        end,
        Fields
    ).

normalize_field(#{name := Name, type := Type} = Map) -> {Name, Type, Map};
normalize_field({Name, Type}) -> {Name, Type, #{}};
normalize_field({Name, Type, Attrs}) -> {Name, Type, Attrs}.

%% Fix enum default values: parser stores {EnumType, <<"Value">>}, needs {EnumType, 'Value'}
normalize_enum_defaults(Fields, Defs) ->
    lists:map(fun(Field) -> normalize_enum_default(Field, Defs) end, Fields).

normalize_enum_default({Name, {TypeName, Default}, Attrs}, Defs) when
    is_binary(Default), is_atom(TypeName)
->
    %% 3-tuple with attrs: check if TypeName refers to an enum
    case maps:get(TypeName, Defs, undefined) of
        {{enum, _BaseType}, _, _} ->
            %% Convert binary default to atom
            {Name, {TypeName, binary_to_atom(Default, utf8)}, Attrs};
        _ ->
            %% Not an enum, keep as-is (e.g. string default)
            {Name, {TypeName, Default}, Attrs}
    end;
normalize_enum_default({Name, {TypeName, Default}}, Defs) when
    is_binary(Default), is_atom(TypeName)
->
    %% 2-tuple (no attrs): check if TypeName refers to an enum
    case maps:get(TypeName, Defs, undefined) of
        {{enum, _BaseType}, _, _} ->
            %% Convert binary default to atom
            {Name, {TypeName, binary_to_atom(Default, utf8)}};
        _ ->
            %% Not an enum, keep as-is
            {Name, {TypeName, Default}}
    end;
normalize_enum_default(Field, _Defs) ->
    Field.

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

get_explicit_id(#{id := Id}) ->
    Id;
get_explicit_id({_Name, _Type, Attrs}) when is_map(Attrs) ->
    maps:get(id, Attrs, undefined);
get_explicit_id({_Name, _Type}) ->
    undefined.

set_field_id(#{} = Map, Id) -> Map#{id => Id};
set_field_id({Name, Type, Attrs}, Id) -> {Name, Type, Attrs#{id => Id}};
set_field_id({Name, Type}, Id) -> {Name, Type, #{id => Id}}.

find_next_id(Candidate, ExplicitIds) ->
    case sets:is_element(Candidate, ExplicitIds) of
        true -> find_next_id(Candidate + 1, ExplicitIds);
        false -> Candidate
    end.

%% =============================================================================
%% Validation
%% =============================================================================

-spec validate(map(), {definitions(), options()}, validate_opts()) ->
    ok | {error, [validation_error()]}.
validate(Map, {Defs, SchemaOpts}, Opts) ->
    RootType = maps:get(root_type, SchemaOpts),
    case validate_table(Map, RootType, Defs, Opts) of
        [] -> ok;
        Errors -> {error, Errors}
    end.

validate_table(Map, TableType, Defs, Opts) when is_map(Map) ->
    case maps:get(TableType, Defs, undefined) of
        {table, _, _, Fields, _} ->
            validate_table_fields(Map, Fields, Defs, Opts);
        undefined ->
            [{unknown_type, TableType}]
    end;
validate_table(Value, TableType, _Defs, _Opts) ->
    [{type_mismatch, TableType, table, Value}].

validate_table_fields(Map, Fields, Defs, Opts) ->
    %% Build set of known field names
    KnownFields = lists:foldl(
        fun(FieldDef, Acc) ->
            Name = get_field_name(FieldDef),
            sets:add_element(Name, Acc)
        end,
        sets:new(),
        Fields
    ),

    %% Check for unknown fields if strict mode
    UnknownErrors =
        case maps:get(unknown_fields, Opts, ignore) of
            error ->
                lists:filtermap(
                    fun(Key) ->
                        KeyAtom = to_field_atom(Key),
                        case sets:is_element(KeyAtom, KnownFields) of
                            true -> false;
                            false -> {true, {unknown_field, KeyAtom}}
                        end
                    end,
                    maps:keys(Map)
                );
            ignore ->
                []
        end,

    %% Validate each field
    FieldErrors = lists:flatmap(
        fun(FieldDef) -> validate_field(Map, FieldDef, Defs, Opts) end,
        Fields
    ),

    UnknownErrors ++ FieldErrors.

validate_field(Map, FieldDef, Defs, Opts) ->
    {Name, Type, Required} = get_field_info(FieldDef),

    case get_map_value(Map, Name) of
        undefined when Required ->
            [{missing_required, Name}];
        undefined ->
            [];
        Value ->
            validate_value(Name, Value, Type, Defs, Opts)
    end.

%% Get field name from either map or tuple format
get_field_name(#{name := Name}) -> Name;
get_field_name({Name, _Type}) -> Name;
get_field_name({Name, _Type, _Attrs}) -> Name.

%% Get field info from either map or tuple format
get_field_info(#{name := Name, type := Type, required := Required}) ->
    {Name, Type, Required};
get_field_info({Name, Type, Attrs}) ->
    {Name, Type, maps:get(required, Attrs, false)};
get_field_info({Name, Type}) ->
    {Name, Type, false}.

get_map_value(Map, Key) ->
    case maps:find(Key, Map) of
        {ok, V} ->
            V;
        error ->
            BinKey = atom_to_binary(Key),
            maps:get(BinKey, Map, undefined)
    end.

to_field_atom(A) when is_atom(A) -> A;
to_field_atom(B) when is_binary(B) ->
    try
        binary_to_existing_atom(B, utf8)
    catch
        error:badarg -> binary_to_atom(B, utf8)
    end.

validate_value(Name, Value, {vector, ElemType}, Defs, Opts) ->
    validate_vector(Name, Value, ElemType, Defs, Opts);
validate_value(Name, Value, {array, ElemType, Count}, Defs, Opts) ->
    validate_array(Name, Value, ElemType, Count, Defs, Opts);
validate_value(Name, Value, {union_type, UnionName}, Defs, _Opts) ->
    validate_union_type(Name, Value, UnionName, Defs);
validate_value(_Name, Value, {union_value, _UnionName}, _Defs, _Opts) when is_map(Value) ->
    [];
validate_value(Name, Value, {union_value, UnionName}, _Defs, _Opts) ->
    [{type_mismatch, Name, {union_value, UnionName}, Value}];
%% Strip default value wrapper and recurse
validate_value(Name, Value, {Type, _Default}, Defs, Opts) when is_atom(Type) ->
    validate_value(Name, Value, Type, Defs, Opts);
validate_value(Name, Value, Type, Defs, Opts) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, _BaseType}, Members, _IndexMap} ->
            validate_enum(Name, Value, Members);
        {table, _, _, _, _} ->
            case validate_table(Value, Type, Defs, Opts) of
                [] -> [];
                Errors -> [{nested_errors, Name, Errors}]
            end;
        {struct, Fields} ->
            validate_struct(Name, Value, Fields);
        {union, _, _} ->
            [{type_mismatch, Name, {union, Type}, Value}];
        undefined ->
            validate_scalar(Name, Value, Type)
    end.

validate_scalar(_Name, Value, bool) when is_boolean(Value) -> [];
validate_scalar(_Name, Value, byte) when is_integer(Value), Value >= -128, Value =< 127 -> [];
validate_scalar(_Name, Value, ubyte) when is_integer(Value), Value >= 0, Value =< 255 -> [];
validate_scalar(_Name, Value, short) when is_integer(Value), Value >= -32768, Value =< 32767 -> [];
validate_scalar(_Name, Value, ushort) when is_integer(Value), Value >= 0, Value =< 65535 -> [];
validate_scalar(_Name, Value, int) when
    is_integer(Value), Value >= -2147483648, Value =< 2147483647
->
    [];
validate_scalar(_Name, Value, uint) when is_integer(Value), Value >= 0, Value =< 4294967295 -> [];
validate_scalar(_Name, Value, long) when is_integer(Value) -> [];
validate_scalar(_Name, Value, ulong) when is_integer(Value), Value >= 0 -> [];
validate_scalar(_Name, Value, float) when is_number(Value) -> [];
validate_scalar(_Name, Value, double) when is_number(Value) -> [];
validate_scalar(_Name, Value, string) when is_binary(Value) -> [];
validate_scalar(_Name, Value, int8) when is_integer(Value), Value >= -128, Value =< 127 -> [];
validate_scalar(_Name, Value, uint8) when is_integer(Value), Value >= 0, Value =< 255 -> [];
validate_scalar(_Name, Value, int16) when is_integer(Value), Value >= -32768, Value =< 32767 -> [];
validate_scalar(_Name, Value, uint16) when is_integer(Value), Value >= 0, Value =< 65535 -> [];
validate_scalar(_Name, Value, int32) when
    is_integer(Value), Value >= -2147483648, Value =< 2147483647
->
    [];
validate_scalar(_Name, Value, uint32) when is_integer(Value), Value >= 0, Value =< 4294967295 -> [];
validate_scalar(_Name, Value, int64) when is_integer(Value) -> [];
validate_scalar(_Name, Value, uint64) when is_integer(Value), Value >= 0 -> [];
validate_scalar(_Name, Value, float32) when is_number(Value) -> [];
validate_scalar(_Name, Value, float64) when is_number(Value) -> [];
validate_scalar(Name, Value, Type) ->
    [{type_mismatch, Name, Type, Value}].

validate_enum(Name, Value, Members) when is_atom(Value) ->
    case lists:member(Value, Members) of
        true -> [];
        false -> [{invalid_enum, Name, Value, Members}]
    end;
validate_enum(Name, Value, Members) when is_binary(Value) ->
    try
        validate_enum(Name, binary_to_existing_atom(Value, utf8), Members)
    catch
        error:badarg -> [{invalid_enum, Name, Value, Members}]
    end;
validate_enum(Name, Value, Members) when is_integer(Value) ->
    case Value >= 0 andalso Value < length(Members) of
        true -> [];
        false -> [{invalid_enum, Name, Value, Members}]
    end;
validate_enum(Name, Value, Members) ->
    [{invalid_enum, Name, Value, Members}].

validate_vector(Name, Values, ElemType, Defs, Opts) when is_list(Values) ->
    {Errors, _} = lists:foldl(
        fun(Elem, {ErrAcc, Idx}) ->
            case validate_value(Name, Elem, ElemType, Defs, Opts) of
                [] -> {ErrAcc, Idx + 1};
                [Err | _] -> {[{invalid_vector_element, Name, Idx, Err} | ErrAcc], Idx + 1}
            end
        end,
        {[], 0},
        Values
    ),
    lists:reverse(Errors);
validate_vector(Name, Value, ElemType, _Defs, _Opts) ->
    [{type_mismatch, Name, {vector, ElemType}, Value}].

validate_array(Name, Values, ElemType, Count, Defs, Opts) when
    is_list(Values), length(Values) == Count
->
    {Errors, _} = lists:foldl(
        fun(Elem, {ErrAcc, Idx}) ->
            case validate_value(Name, Elem, ElemType, Defs, Opts) of
                [] -> {ErrAcc, Idx + 1};
                [Err | _] -> {[{invalid_array_element, Name, Idx, Err} | ErrAcc], Idx + 1}
            end
        end,
        {[], 0},
        Values
    ),
    lists:reverse(Errors);
validate_array(Name, Values, _ElemType, Count, _Defs, _Opts) when is_list(Values) ->
    [{array_length_mismatch, Name, Count, length(Values)}];
validate_array(Name, Value, ElemType, Count, _Defs, _Opts) ->
    [{type_mismatch, Name, {array, ElemType, Count}, Value}].

validate_union_type(Name, Value, UnionName, Defs) ->
    {union, Members, _} = maps:get(UnionName, Defs),
    MemberAtom =
        case Value of
            A when is_atom(A) -> A;
            B when is_binary(B) ->
                try
                    binary_to_existing_atom(B, utf8)
                catch
                    error:badarg -> B
                end;
            _ ->
                Value
        end,
    case lists:member(MemberAtom, Members) of
        true -> [];
        false -> [{invalid_union_type, Name, Value, Members}]
    end.

validate_struct(Name, Value, Fields) when is_map(Value) ->
    Errors = lists:flatmap(
        fun
            ({FieldName, FieldType}) ->
                case get_map_value(Value, FieldName) of
                    undefined -> [{missing_required, FieldName}];
                    FieldValue -> validate_scalar(FieldName, FieldValue, FieldType)
                end;
            ({FieldName, FieldType, _Attrs}) ->
                case get_map_value(Value, FieldName) of
                    undefined -> [{missing_required, FieldName}];
                    FieldValue -> validate_scalar(FieldName, FieldValue, FieldType)
                end
        end,
        Fields
    ),
    case Errors of
        [] -> [];
        _ -> [{nested_errors, Name, Errors}]
    end;
validate_struct(Name, Value, _Fields) ->
    [{type_mismatch, Name, struct, Value}].
