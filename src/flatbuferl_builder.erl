%% @private
-module(flatbuferl_builder).

-export([from_map/2, from_map/3]).
-export_type([encode_opts/0]).

-include("flatbuferl_records.hrl").

-type schema() :: {flatbuferl_schema:definitions(), flatbuferl_schema:options()}.

%% Options for encoding:
%%   file_id => boolean() | <<_:32>>
%%     - true: include file_identifier from schema (default)
%%     - false: omit file_identifier
%%     - <<4 bytes>>: use this file_identifier instead
%%   deprecated => skip | allow | error
%%     - skip: silently ignore deprecated fields (default)
%%     - allow: encode deprecated fields if present
%%     - error: raise error if deprecated field has value
-type encode_opts() :: #{
    file_id => boolean() | <<_:32>>,
    deprecated => skip | allow | error
}.

%% =============================================================================
%% Public API
%% =============================================================================

-spec from_map(map(), schema()) -> iodata().
from_map(Map, Schema) ->
    from_map(Map, Schema, #{}).

-spec from_map(map(), schema(), encode_opts()) -> iodata().
from_map(Map, {Defs, SchemaOpts}, Opts) ->
    RootType = maps:get(root_type, SchemaOpts),
    FileId =
        case maps:get(file_id, Opts, true) of
            true -> maps:get(file_identifier, SchemaOpts, no_file_id);
            false -> no_file_id;
            <<_:32>> = Override -> Override
        end,
    from_map_internal(Map, Defs, RootType, FileId, Opts).

from_map_internal(Map, Defs, RootType, FileId, Opts) ->
    #table_def{
        scalars = ScalarDefs,
        refs = RefDefs,
        all_fields = AllFieldDefs,
        encode_layout = EncodeLayout
    } = maps:get(RootType, Defs),
    validate_fields(Map, AllFieldDefs, RootType, Opts),

    %% Collect values for pre-partitioned field defs
    Scalars = collect_field_values(Map, ScalarDefs, Defs, Opts),
    Refs = collect_field_values(Map, RefDefs, Defs, Opts),

    %% Header size: 4 (root offset) + 4 (file id) if file id present
    HeaderSize =
        case FileId of
            no_file_id -> 4;
            _ -> 8
        end,

    %% Check if we can use the fast path (precomputed layout, all fields present)
    case EncodeLayout of
        #encode_layout{all_field_ids = AllFieldIds} ->
            PresentCount = length(Scalars) + length(Refs),
            case PresentCount == length(AllFieldIds) of
                true ->
                    encode_fast_path(Scalars, Refs, EncodeLayout, HeaderSize, FileId, Defs);
                false ->
                    %% Medium path: use precomputed layout with adjustment
                    encode_medium_path(Scalars, Refs, EncodeLayout, HeaderSize, FileId, Defs)
            end;
        undefined ->
            encode_slow_path(Scalars, Refs, HeaderSize, FileId, Defs)
    end.

%% Fast path: all fields present, use precomputed layout
encode_fast_path(Scalars, Refs, EncodeLayout, HeaderSize, FileId, Defs) ->
    #encode_layout{
        vtable = PrecomputedVTable,
        vtable_size = VTableSize,
        table_size = BaseTableSize,
        slots = PrecomputedSlots,
        max_id = MaxId
    } = EncodeLayout,

    %% Convert {offset, size} slots to just offsets
    Slots = maps:map(fun(_Id, {Offset, _Size}) -> Offset end, PrecomputedSlots),

    AllFields = merge_by_layout_key(Scalars, Refs),
    RawTableSize = BaseTableSize - 4,

    %% Check if ref padding changes the table size
    TableSizeWithPadding = calc_table_size_with_padding(
        AllFields, RawTableSize, Refs, HeaderSize, Defs, MaxId
    ),

    %% Use precomputed vtable if table size unchanged, otherwise rebuild
    {VTable, VTableForComparison} =
        case TableSizeWithPadding == BaseTableSize of
            true ->
                {PrecomputedVTable, PrecomputedVTable};
            false ->
                NewVT = build_vtable_with_size(AllFields, Slots, TableSizeWithPadding, MaxId),
                {NewVT, build_vtable_from_fields(AllFields, Slots, BaseTableSize, MaxId)}
        end,

    {RefVTables, LayoutCache} = collect_ref_vtables(Refs, Defs),
    case lists:member(VTableForComparison, RefVTables) of
        true ->
            encode_root_vtable_after(
                Defs,
                FileId,
                AllFields,
                Slots,
                TableSizeWithPadding,
                VTableForComparison,
                HeaderSize,
                LayoutCache
            );
        false ->
            encode_root_vtable_before(
                AllFields,
                Slots,
                BaseTableSize,
                VTable,
                VTableSize,
                TableSizeWithPadding,
                HeaderSize,
                FileId,
                Defs,
                LayoutCache
            )
    end.

%% Medium path: some fields missing, adjust precomputed layout
%% Uses the adjustment algorithm to avoid recomputing field placements
encode_medium_path(Scalars, Refs, EncodeLayout, HeaderSize, FileId, Defs) ->
    #encode_layout{
        slots = PrecomputedSlots,
        all_field_ids = AllFieldIds
    } = EncodeLayout,

    AllFields = merge_by_layout_key(Scalars, Refs),
    MaxId =
        case AllFields of
            [] -> -1;
            _ -> lists:max([F#field.id || F <- AllFields])
        end,

    %% Build set of present field IDs
    PresentIds = [F#field.id || F <- AllFields],

    %% Adjust slots using the correction algorithm
    %% Walk through fields in ascending layout_key order (lowest first)
    %% Accumulate missing field sizes; present fields shift by cumulative missing
    AdjustedSlots = adjust_slots_for_missing(PrecomputedSlots, PresentIds, AllFieldIds),

    %% Calculate new table size from present fields
    RawSize = lists:sum([F#field.size || F <- AllFields]),
    BaseTableSize = 4 + align_offset(RawSize, 4),

    RawTableSize = BaseTableSize - 4,
    TableSizeWithPadding = calc_table_size_with_padding(
        AllFields, RawTableSize, Refs, HeaderSize, Defs, MaxId
    ),
    VTable = build_vtable_with_size(AllFields, AdjustedSlots, TableSizeWithPadding, MaxId),
    VTableSize = vtable_size(VTable),
    VTableForComparison = build_vtable_from_fields(AllFields, AdjustedSlots, BaseTableSize, MaxId),
    {RefVTables, LayoutCache} = collect_ref_vtables(Refs, Defs),
    case lists:member(VTableForComparison, RefVTables) of
        true ->
            encode_root_vtable_after(
                Defs,
                FileId,
                AllFields,
                AdjustedSlots,
                TableSizeWithPadding,
                VTableForComparison,
                HeaderSize,
                LayoutCache
            );
        false ->
            encode_root_vtable_before(
                AllFields,
                AdjustedSlots,
                BaseTableSize,
                VTable,
                VTableSize,
                TableSizeWithPadding,
                HeaderSize,
                FileId,
                Defs,
                LayoutCache
            )
    end.

%% Adjust precomputed slots for missing fields.
%%
%% The correct approach is to re-place fields from the new TableSize.
%% Simple subtraction doesn't work with mixed field sizes because:
%% 1. TableSize can change by more than sum of missing sizes (due to alignment)
%% 2. Alignment gaps between fields can change
%%
%% This is O(n) where n is number of fields in the table.
adjust_slots_for_missing(PrecomputedSlots, PresentIds, AllFieldIds) ->
    PresentSet = sets:from_list(PresentIds),

    %% Filter to present fields in layout order (highest layout_key first)
    %% AllFieldIds is already in descending layout_key order
    PresentFieldInfo = [
        {Id, Size}
     || Id <- AllFieldIds,
        sets:is_element(Id, PresentSet),
        {_, Size} <- [maps:get(Id, PrecomputedSlots)]
    ],

    %% Calculate new table size
    RawSize = lists:sum([Size || {_, Size} <- PresentFieldInfo]),
    TableDataSize = align_offset(RawSize, 4),
    NewTableSize = 4 + TableDataSize,

    %% Place fields backward from new TableSize with proper alignment
    {Slots, _} = lists:foldl(
        fun({Id, Size}, {Acc, EndPos}) ->
            Align = min(Size, 4),
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, NewTableSize},
        PresentFieldInfo
    ),
    Slots.

%% Slow path: some fields missing, compute layout dynamically
encode_slow_path(Scalars, Refs, HeaderSize, FileId, Defs) ->
    AllFields = merge_by_layout_key(Scalars, Refs),
    MaxId =
        case AllFields of
            [] -> -1;
            _ -> lists:max([F#field.id || F <- AllFields])
        end,
    {Slots, BaseTableSize} = place_fields_and_calc_size(AllFields),
    RawTableSize = BaseTableSize - 4,
    TableSizeWithPadding = calc_table_size_with_padding(
        AllFields, RawTableSize, Refs, HeaderSize, Defs, MaxId
    ),
    VTable = build_vtable_with_size(AllFields, Slots, TableSizeWithPadding, MaxId),
    VTableSize = vtable_size(VTable),
    VTableForComparison = build_vtable_from_fields(AllFields, Slots, BaseTableSize, MaxId),
    {RefVTables, LayoutCache} = collect_ref_vtables(Refs, Defs),
    case lists:member(VTableForComparison, RefVTables) of
        true ->
            encode_root_vtable_after(
                Defs,
                FileId,
                AllFields,
                Slots,
                TableSizeWithPadding,
                VTableForComparison,
                HeaderSize,
                LayoutCache
            );
        false ->
            encode_root_vtable_before(
                AllFields,
                Slots,
                BaseTableSize,
                VTable,
                VTableSize,
                TableSizeWithPadding,
                HeaderSize,
                FileId,
                Defs,
                LayoutCache
            )
    end.

%% Standard layout: header | [pad] | vtable | soffset | table | ref_data
encode_root_vtable_before(
    AllFields,
    Slots,
    BaseTableSize,
    VTable,
    VTableSize,
    _TableSizeWithPadding,
    HeaderSize,
    FileId,
    Defs,
    LayoutCache
) ->
    %% Calculate table position (4-byte aligned, plus adjustment for 8-byte fields)
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos4 = align_offset(TablePosUnaligned, 4),

    %% If table has 8-byte fields, ensure they're 8-byte aligned in the buffer
    First8ByteOffset = find_first_8byte_field_offset(AllFields, Slots),
    TablePos =
        case First8ByteOffset of
            none ->
                TablePos4;
            Offset ->
                case (TablePos4 + Offset) rem 8 of
                    0 -> TablePos4;
                    _ -> TablePos4 + 4
                end
        end,
    PreVTablePad = TablePos - VTableSize - HeaderSize,

    %% Build table data (using precomputed Slots)
    {TableData, RefDataBin} = build_table_data2(
        AllFields, Slots, BaseTableSize, TablePos, Defs, LayoutCache
    ),

    %% soffset points back to vtable (positive)
    VTablePos = HeaderSize + PreVTablePad,
    SOffset = TablePos - VTablePos,

    [
        <<TablePos:32/little-unsigned>>,
        file_id_bin(FileId),
        <<0:(PreVTablePad * 8)>>,
        VTable,
        <<SOffset:32/little-signed>>,
        TableData,
        RefDataBin
    ].

%% Shared vtable layout: header | soffset | table | ref_data (vtable is inside ref_data)
%% Note: This path needs slots computed with TableSizeWithPadding (includes padding)
encode_root_vtable_after(
    Defs,
    FileId,
    AllFields,
    _Slots,
    TableSizeWithPadding,
    VTable,
    HeaderSize,
    LayoutCache
) ->
    %% Root table starts right after header (no vtable before it)
    TablePos = HeaderSize,
    RefDataStart = TablePos + TableSizeWithPadding,

    %% Build field layout - need slots with TableSizeWithPadding for this path
    PaddedSlots = place_fields_backward(AllFields, TableSizeWithPadding),
    FieldLayout = [F#field{offset = maps:get(F#field.id, PaddedSlots)} || F <- AllFields],
    SortedLayout = lists:sort(fun(A, B) -> A#field.offset =< B#field.offset end, FieldLayout),

    %% Extract refs in flatc order with field offsets
    RefFields = [F || F <- SortedLayout, not F#field.is_scalar],
    RefFieldsOrdered = sort_refs_flatc_order(RefFields),

    %% Build ref data with vtable sharing, getting ref positions and shared vtable position
    {RefDataBin, RefPositions, SharedVTablePos} = build_ref_data_with_vtable_sharing(
        RefFieldsOrdered, RefDataStart, VTable, Defs, LayoutCache
    ),

    %% Build table data with correct uoffsets pointing to ref positions
    TableData = build_inline_data(SortedLayout, TablePos, RefPositions),

    %% Calculate padding between table data and ref data
    %% TableSizeWithPadding includes soffset (4 bytes) + inline data + padding
    ActualTableSize = 4 + iolist_size(TableData),
    TablePadding = TableSizeWithPadding - ActualTableSize,

    %% soffset points forward to shared vtable (negative because vtable is after)
    %% Will be negative
    SOffset = TablePos - SharedVTablePos,

    [
        <<TablePos:32/little-unsigned>>,
        file_id_bin(FileId),
        <<SOffset:32/little-signed>>,
        TableData,
        <<0:(TablePadding * 8)>>,
        RefDataBin
    ].

file_id_bin(no_file_id) -> <<>>;
file_id_bin(B) when byte_size(B) == 4 -> B;
file_id_bin(_) -> error(invalid_file_id).

%% =============================================================================
%% Field Validation
%% =============================================================================

validate_fields(Map, Fields, TableType, Opts) ->
    lists:foreach(
        fun(FieldDef) ->
            {Name, Required, Deprecated} = get_field_attrs(FieldDef),
            HasValue = has_field_value(Map, Name),
            %% Check required
            case {Required, HasValue} of
                {true, false} ->
                    error({required_field_missing, TableType, Name});
                _ ->
                    ok
            end,
            %% Check deprecated
            case {Deprecated, HasValue, maps:get(deprecated, Opts, skip)} of
                {true, true, error} ->
                    error({deprecated_field_set, TableType, Name});
                _ ->
                    ok
            end
        end,
        Fields
    ).

get_field_attrs(#field_def{name = Name, required = Required, deprecated = Deprecated}) ->
    {Name, Required, Deprecated}.

has_field_value(Map, Name) when is_atom(Name) ->
    maps:is_key(Name, Map) orelse maps:is_key(atom_to_binary(Name), Map).

%% =============================================================================
%% Field Collection
%% =============================================================================

%% Collect field values from pre-partitioned field definitions.
%% The schema already partitions fields into scalars and refs, so this function
%% simply iterates over a single partition and collects values.
%% Returns list of #field{} records in layout_key order (same as input).
collect_field_values(Map, FieldDefs, Defs, Opts) ->
    DeprecatedOpt = maps:get(deprecated, Opts, skip),
    lists:filtermap(
        fun
            (#field_def{deprecated = true}) when DeprecatedOpt == skip ->
                false;
            (FieldDef) ->
                case collect_field(Map, FieldDef, Defs) of
                    [] -> false;
                    [Field] -> {true, Field}
                end
        end,
        FieldDefs
    ).

%% Returns #field{} records
collect_field(
    Map,
    #field_def{
        name = Name,
        id = FieldId,
        default = Default,
        inline_size = InlineSize,
        is_scalar = IsScalar,
        resolved_type = Type,
        layout_key = LayoutKey
    },
    Defs
) ->
    case Type of
        {union_type, UnionName} ->
            %% Union type field - look for <field>_type key directly
            case get_field_value(Map, Name) of
                undefined ->
                    [];
                MemberType when is_atom(MemberType) ->
                    {union, _Members, IndexMap} = maps:get(UnionName, Defs),
                    TypeIndex = find_union_index(MemberType, IndexMap),
                    [
                        #field{
                            id = FieldId,
                            type = {union_type, UnionName},
                            value = TypeIndex,
                            size = InlineSize,
                            is_scalar = true,
                            layout_key = LayoutKey
                        }
                    ];
                MemberType when is_binary(MemberType) ->
                    {union, _Members, IndexMap} = maps:get(UnionName, Defs),
                    TypeIndex = find_union_index(binary_to_atom(MemberType), IndexMap),
                    [
                        #field{
                            id = FieldId,
                            type = {union_type, UnionName},
                            value = TypeIndex,
                            size = InlineSize,
                            is_scalar = true,
                            layout_key = LayoutKey
                        }
                    ];
                _ ->
                    []
            end;
        {union_value, UnionName} ->
            TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
            case get_field_value(Map, Name) of
                undefined ->
                    [];
                TableValue when is_map(TableValue) ->
                    MemberType =
                        case get_field_value(Map, TypeFieldName) of
                            T when is_atom(T) -> T;
                            T when is_binary(T) -> binary_to_atom(T);
                            _ -> error({missing_union_type_field, TypeFieldName})
                        end,
                    [
                        #field{
                            id = FieldId,
                            type = {union_value, UnionName},
                            value = #{type => MemberType, value => TableValue},
                            size = InlineSize,
                            is_scalar = false,
                            layout_key = LayoutKey
                        }
                    ];
                _ ->
                    []
            end;
        #vector_def{element_type = {union_type, UnionName}} ->
            collect_union_type_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey, Defs);
        {vector, {union_type, UnionName}} ->
            collect_union_type_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey, Defs);
        #vector_def{element_type = {union_value, UnionName}} ->
            collect_union_value_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey);
        {vector, {union_value, UnionName}} ->
            collect_union_value_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey);
        _ ->
            case get_field_value(Map, Name) of
                undefined ->
                    [];
                Value when Value == Default -> [];
                Value ->
                    [
                        #field{
                            id = FieldId,
                            type = Type,
                            value = Value,
                            size = InlineSize,
                            is_scalar = IsScalar,
                            layout_key = LayoutKey
                        }
                    ]
            end
    end.

%% Helper for collecting union type vectors
collect_union_type_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey, Defs) ->
    case get_field_value(Map, Name) of
        undefined ->
            [];
        TypeList when is_list(TypeList) ->
            {union, _Members, IndexMap} = maps:get(UnionName, Defs),
            TypeIndices = [
                begin
                    T =
                        case is_binary(MT) of
                            true -> binary_to_atom(MT);
                            false -> MT
                        end,
                    find_union_index(T, IndexMap)
                end
             || MT <- TypeList
            ],
            [
                #field{
                    id = FieldId,
                    type = {vector, ubyte},
                    value = TypeIndices,
                    size = InlineSize,
                    is_scalar = false,
                    layout_key = LayoutKey
                }
            ];
        _ ->
            []
    end.

%% Helper for collecting union value vectors
collect_union_value_vector(Map, Name, FieldId, UnionName, InlineSize, LayoutKey) ->
    TypeFieldName = list_to_atom(atom_to_list(Name) ++ "_type"),
    case get_field_value(Map, Name) of
        undefined ->
            [];
        ValueList when is_list(ValueList) ->
            TypeList =
                case get_field_value(Map, TypeFieldName) of
                    TL when is_list(TL) -> TL;
                    _ -> error({missing_union_type_field, TypeFieldName})
                end,
            length(TypeList) == length(ValueList) orelse
                error(
                    {union_vector_length_mismatch, Name, length(TypeList), length(ValueList)}
                ),
            TaggedValues = lists:zipwith(
                fun(T, V) ->
                    Type0 =
                        case is_binary(T) of
                            true -> binary_to_atom(T);
                            false -> T
                        end,
                    #{type => Type0, value => V}
                end,
                TypeList,
                ValueList
            ),
            [
                #field{
                    id = FieldId,
                    type = {vector, {union_value, UnionName}},
                    value = TaggedValues,
                    size = InlineSize,
                    is_scalar = false,
                    layout_key = LayoutKey
                }
            ];
        _ ->
            []
    end.

%% O(1) lookup using precomputed index map from schema
find_union_index(Type, IndexMap) when is_map(IndexMap) ->
    maps:get(Type, IndexMap, 0).

%% Look up field by atom key or binary key
get_field_value(Map, Name) when is_atom(Name) ->
    case maps:get(Name, Map, undefined) of
        undefined -> maps:get(atom_to_binary(Name), Map, undefined);
        Value -> Value
    end.

%% Unwrap types with default values: {TypeName, DefaultValue} -> TypeName
%% Only unwrap when TypeName is NOT a known type constructor
is_scalar_type({TypeName, Default}) when
    is_atom(TypeName),
    is_atom(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    %% Enum with default value like {'Color', 'Blue'}
    is_scalar_type(TypeName);
is_scalar_type({TypeName, Default}) when
    is_atom(TypeName),
    is_number(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    %% Scalar with default value like {int, 100}
    is_scalar_type(TypeName);
is_scalar_type({TypeName, Default}) when
    is_atom(TypeName),
    is_boolean(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    %% Bool with default value like {bool, true}
    is_scalar_type(TypeName);
is_scalar_type({enum, _}) ->
    true;
is_scalar_type({enum, _, _}) ->
    true;
%% Structs are inline fixed-size data
is_scalar_type({struct, _}) ->
    true;
%% Fixed arrays are inline fixed-size data
is_scalar_type({array, _, _}) ->
    true;
%% Union type field is ubyte
is_scalar_type({union_type, _}) ->
    true;
is_scalar_type(bool) ->
    true;
is_scalar_type(byte) ->
    true;
is_scalar_type(ubyte) ->
    true;
is_scalar_type(int8) ->
    true;
is_scalar_type(uint8) ->
    true;
is_scalar_type(short) ->
    true;
is_scalar_type(ushort) ->
    true;
is_scalar_type(int16) ->
    true;
is_scalar_type(uint16) ->
    true;
is_scalar_type(int) ->
    true;
is_scalar_type(uint) ->
    true;
is_scalar_type(int32) ->
    true;
is_scalar_type(uint32) ->
    true;
is_scalar_type(long) ->
    true;
is_scalar_type(ulong) ->
    true;
is_scalar_type(int64) ->
    true;
is_scalar_type(uint64) ->
    true;
is_scalar_type(float) ->
    true;
is_scalar_type(float32) ->
    true;
is_scalar_type(double) ->
    true;
is_scalar_type(float64) ->
    true;
is_scalar_type(_) ->
    false.

%% Resolve type name to its definition (for enums and structs)
%% Unwrap types with default values first (not type constructors)
resolve_type({TypeName, Default}, Defs) when
    is_atom(TypeName),
    is_atom(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    resolve_type(TypeName, Defs);
resolve_type({TypeName, Default}, Defs) when
    is_atom(TypeName),
    is_number(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    resolve_type(TypeName, Defs);
resolve_type({TypeName, Default}, Defs) when
    is_atom(TypeName),
    is_boolean(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    resolve_type(TypeName, Defs);
resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {{enum, Base}, Values} -> {enum, Base, Values};
        #struct_def{} = StructDef -> StructDef;
        {struct, Fields} -> {struct, Fields};
        _ -> Type
    end;
resolve_type({vector, ElemType}, Defs) ->
    {vector, resolve_type(ElemType, Defs)};
resolve_type({array, ElemType, Count}, Defs) ->
    {array, resolve_type(ElemType, Defs), Count};
resolve_type(Type, _Defs) ->
    Type.

%% =============================================================================
%% VTable Building
%% =============================================================================

%% VTables are lists of 2-byte lists: [[VTSizeLo, VTSizeHi], [TblSizeLo, TblSizeHi], [Slot0Lo, Slot0Hi], ...]
%% Comparison works with ==, and they're valid iolists (nested lists flatten).

%% Convert uint16 to little-endian 2-byte list
uint16_bytes(X) when X < 256 -> [X, 0];
uint16_bytes(V) -> [V band 16#FF, (V bsr 8) band 16#FF].

%% Get byte size of vtable
vtable_size(VT) -> length(VT) * 2.

build_vtable(Scalars, Refs, MaxId) ->
    {VTable, _, _, _} = compute_table_layout(Scalars, Refs, MaxId),
    VTable.

%% Compute table layout info: {VTable, AllFields, BaseTableSize, Slots}
%% Used to avoid duplicate computation in table vector encoding
%% Note: MaxId is computed from actual fields being encoded, not schema MaxId
compute_table_layout(Scalars, Refs, _SchemaMaxId) ->
    AllFields = merge_by_layout_key(Scalars, Refs),
    %% Compute max field ID from actual fields being encoded (#field{} records)
    FieldMaxId =
        case AllFields of
            [] -> -1;
            _ -> lists:max([F#field.id || F <- AllFields])
        end,
    RawSize = calc_backward_table_size(AllFields),
    BaseTableSize = 4 + align_offset(RawSize, 4),
    Slots = place_fields_backward(AllFields, BaseTableSize),
    VTable = build_vtable_from_fields(AllFields, Slots, BaseTableSize, FieldMaxId),
    {VTable, AllFields, BaseTableSize, Slots}.

%% Build vtable from pre-merged fields with precomputed Slots map and TableSize
build_vtable_from_fields(_AllFields, Slots, TableSize, MaxId) ->
    case MaxId of
        -1 ->
            [uint16_bytes(4), uint16_bytes(4)];
        _ ->
            NumSlots = MaxId + 1,
            VTableSize = 4 + (NumSlots * 2),
            %% Convert Slots map to list of uint16_bytes
            SlotsList = [uint16_bytes(maps:get(Id, Slots, 0)) || Id <- lists:seq(0, NumSlots - 1)],
            [uint16_bytes(VTableSize), uint16_bytes(TableSize) | SlotsList]
    end.

%% Build vtable with precomputed Slots map and table size (including ref padding)
build_vtable_with_size(_AllFields, Slots, TableSize, MaxId) ->
    case MaxId of
        -1 ->
            [uint16_bytes(4), uint16_bytes(4)];
        _ ->
            NumSlots = MaxId + 1,
            VTableSize = 4 + (NumSlots * 2),
            %% Convert Slots map to list of uint16_bytes
            SlotsList = [uint16_bytes(maps:get(Id, Slots, 0)) || Id <- lists:seq(0, NumSlots - 1)],
            [uint16_bytes(VTableSize), uint16_bytes(TableSize) | SlotsList]
    end.

%% Calculate table size including padding for ref data alignment
calc_table_size_with_padding(_AllFields, RawSize, Refs, HeaderSize, Defs, MaxId) ->
    %% Table data must be 4-byte aligned (for soffset alignment)
    TableDataSize = align_offset(RawSize, 4),
    BaseTableSize = 4 + TableDataSize,

    %% Calculate vtable size to determine table position
    NumSlots = MaxId + 1,
    VTableSize = 4 + (NumSlots * 2),
    TablePosUnaligned = HeaderSize + VTableSize,
    TablePos = align_offset(TablePosUnaligned, 4),

    %% Sort refs in flatc order
    RefFieldsOrdered = sort_refs_flatc_order(Refs),

    %% Calculate ref padding
    RefPadding = calc_ref_padding_for_refs(RefFieldsOrdered, TablePos + BaseTableSize, Defs),
    BaseTableSize + RefPadding.

%% Sort refs in flatc order:
%% - If there's an id=0 ref: ascending order (1, 2, ..., N, 0)
%% - If NO id=0 ref: descending order (N, N-1, ..., 1)
sort_refs_flatc_order(Refs) ->
    {ZeroRefs, NonZeroRefs} = lists:partition(fun(#field{id = Id}) -> Id == 0 end, Refs),
    case ZeroRefs of
        [] ->
            %% No id=0 ref: descending order
            lists:sort(fun(#field{id = IdA}, #field{id = IdB}) -> IdA >= IdB end, NonZeroRefs);
        _ ->
            %% Has id=0 ref: ascending with 0 at end
            SortedNonZero = lists:sort(
                fun(#field{id = IdA}, #field{id = IdB}) -> IdA =< IdB end, NonZeroRefs
            ),
            SortedNonZero ++ ZeroRefs
    end.

%% Find the offset of the first 8-byte field using precomputed Slots map
%% Fields must be pre-sorted by layout_key
find_first_8byte_field_offset(AllFields, Slots) ->
    %% Find 8-byte fields and get their offsets from precomputed Slots
    EightByteFields = [F || F <- AllFields, F#field.size == 8],
    case EightByteFields of
        [] ->
            none;
        _ ->
            Offsets = [maps:get(F#field.id, Slots) || F <- EightByteFields],
            %% Return smallest offset (first in table)
            lists:min(Offsets)
    end.

%% Calculate ref padding for field list (without offsets)
%% Ensure nested table's SOFFSET is 4-byte aligned (not just vtable start)
%% Position of soffset = Pos + VTableSize, which must be 4-byte aligned
calc_ref_padding_for_refs([], _Pos, _Defs) ->
    0;
calc_ref_padding_for_refs([#field{type = Type, value = Value} | _], Pos, Defs) when
    is_atom(Type), is_map(Value)
->
    case maps:get(Type, Defs, undefined) of
        #table_def{max_id = MaxId} ->
            %% Calculate nested vtable size using precomputed MaxId
            VTableSize = calc_vtable_size(MaxId),
            %% Pad to make (Pos + VTableSize) 4-byte aligned
            SOffsetPos = Pos + VTableSize,
            (4 - (SOffsetPos rem 4)) rem 4;
        _ ->
            0
    end;
calc_ref_padding_for_refs(
    [#field{type = {union_value, UnionName}, value = #{type := MemberType, value := Value}} | _],
    Pos,
    Defs
) ->
    {union, _, _} = maps:get(UnionName, Defs),
    calc_ref_padding_for_refs(
        [
            #field{
                id = 0,
                type = MemberType,
                value = Value,
                size = 4,
                is_scalar = false,
                layout_key = 0
            }
        ],
        Pos,
        Defs
    );
calc_ref_padding_for_refs(_, _Pos, _Defs) ->
    0.

%% Merge two lists already sorted by layout_key descending - O(n) instead of O(n log n)
merge_by_layout_key([], Bs) ->
    Bs;
merge_by_layout_key(As, []) ->
    As;
merge_by_layout_key([A | As] = AllA, [B | Bs] = AllB) ->
    case A#field.layout_key >= B#field.layout_key of
        true -> [A | merge_by_layout_key(As, AllB)];
        false -> [B | merge_by_layout_key(AllA, Bs)]
    end.

%% Calculate raw data size for all fields using precomputed sizes
calc_backward_table_size(Fields) ->
    lists:sum([F#field.size || F <- Fields]).

%% Place fields backward from end of table using precomputed sizes
place_fields_backward(Fields, TableSize) ->
    {Slots, _} = lists:foldl(
        fun(#field{id = Id, size = Size}, {Acc, EndPos}) ->
            Align = min(Size, 4),
            %% Work backward: EndPos is where current field ends
            %% Field starts at EndPos - Size, but must be aligned
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, TableSize},
        Fields
    ),
    Slots.

%% Compute slots and base table size for dynamic layout (no precomputation)
place_fields_and_calc_size(AllFields) ->
    RawSize = calc_backward_table_size(AllFields),
    BaseTableSize = 4 + align_offset(RawSize, 4),
    Slots = place_fields_backward(AllFields, BaseTableSize),
    {Slots, BaseTableSize}.

%% Align offset DOWN to alignment boundary
align_down(Off, Align) ->
    Off - (Off rem Align).

%% =============================================================================
%% Data Building
%% =============================================================================

%% Build table data with fields placed backward from end (like flatc)
%% Used for nested tables where we don't have precomputed Slots
build_table_data(Scalars, Refs, TablePos, Defs) ->
    AllFields = merge_by_layout_key(Scalars, Refs),
    RawSize = calc_backward_table_size(AllFields),
    BaseTableSize = 4 + align_offset(RawSize, 4),
    Slots = place_fields_backward(AllFields, BaseTableSize),
    build_table_data2(AllFields, Slots, BaseTableSize, TablePos, Defs, #layout_cache{}).

%% Version that accepts pre-merged AllFields with precomputed Slots and BaseTableSize
build_table_data2(AllFields, Slots, BaseTableSize, TablePos, Defs, LayoutCache) ->
    %% Build field layout with positions from precomputed Slots
    FieldLayout = [F#field{offset = maps:get(F#field.id, Slots)} || F <- AllFields],
    SortedLayout = lists:sort(fun(A, B) -> A#field.offset =< B#field.offset end, FieldLayout),

    %% Extract refs from sorted layout (has offsets) and sort in flatc order
    RefFields = [F || F <- SortedLayout, not F#field.is_scalar],
    RefFieldsOrdered = sort_refs_flatc_order(RefFields),

    %% Calculate padding needed before ref data for nested table alignment
    RefPadding = calc_ref_padding_cached(
        RefFieldsOrdered, TablePos + BaseTableSize, Defs, LayoutCache
    ),
    TableSize = BaseTableSize + RefPadding,
    RefDataStart = TablePos + TableSize,

    {RefDataIo, RefPositions} = build_ref_data(RefFieldsOrdered, RefDataStart, Defs, LayoutCache),

    %% Build table inline data with padding
    TableDataIo = build_inline_data(SortedLayout, TablePos, RefPositions),

    %% Return as iolists
    case RefPadding of
        0 -> {TableDataIo, RefDataIo};
        _ -> {[TableDataIo, <<0:(RefPadding * 8)>>], RefDataIo}
    end.

%% Calculate padding needed before ref data (using layout cache)
%% Ensure nested table's SOFFSET is 4-byte aligned (Pos + VTableSize must be 4-byte aligned)
calc_ref_padding_cached([], _Pos, _Defs, _Cache) ->
    0;
calc_ref_padding_cached([#field{type = Type, value = Value} | _], Pos, Defs, #layout_cache{}) when
    is_atom(Type), is_map(Value)
->
    case maps:get(Type, Defs, undefined) of
        #table_def{max_id = MaxId} ->
            %% VTable size only depends on MaxId, no need to check cache
            VTableSize = calc_vtable_size(MaxId),
            %% Pad to make (Pos + VTableSize) 4-byte aligned
            SOffsetPos = Pos + VTableSize,
            (4 - (SOffsetPos rem 4)) rem 4;
        _ ->
            0
    end;
calc_ref_padding_cached(
    [#field{type = {union_value, UnionName}, value = #{type := MemberType, value := Value}} | _],
    Pos,
    Defs,
    Cache
) ->
    {union, _, _} = maps:get(UnionName, Defs),
    calc_ref_padding_cached(
        [
            #field{
                id = 0,
                type = MemberType,
                value = Value,
                size = 4,
                is_scalar = false,
                layout_key = 0
            }
        ],
        Pos,
        Defs,
        Cache
    );
calc_ref_padding_cached(_, _Pos, _Defs, _Cache) ->
    0.

%% Collect all vtables that will be used in ref data (for vtable sharing detection)
%% Also builds a layout cache to avoid recomputing layouts during encoding.
%% Returns: {RefVTables, LayoutCache}
collect_ref_vtables(Refs, Defs) ->
    {VTables, _Seen, Cache} = lists:foldl(
        fun(#field{type = Type, value = Value}, {VTAcc, Seen, CacheAcc}) ->
            collect_vtables_from_ref(Type, Value, Defs, VTAcc, Seen, CacheAcc)
        end,
        {[], #{}, #layout_cache{}},
        Refs
    ),
    {VTables, Cache}.

collect_vtables_from_ref(
    #vector_def{element_type = ElemType}, Values, Defs, VTAcc, Seen, Cache
) when
    is_list(Values)
->
    collect_vtables_from_ref({vector, ElemType}, Values, Defs, VTAcc, Seen, Cache);
collect_vtables_from_ref({vector, ElemType}, Values, Defs, VTAcc, Seen, Cache) when
    is_list(Values)
->
    %% For table vectors, collect vtables from elements and cache layouts
    case maps:get(ElemType, Defs, undefined) of
        #table_def{scalars = ScalarDefs, refs = RefDefs, max_id = MaxId} ->
            lists:foldl(
                fun(Value, {Acc, S, #layout_cache{tables = T} = C}) ->
                    CacheKey = {ElemType, Value},
                    Scalars = collect_field_values(Value, ScalarDefs, Defs, #{}),
                    Refs = collect_field_values(Value, RefDefs, Defs, #{}),
                    {VT, AllFields, BaseTableSize, Slots} = compute_table_layout(
                        Scalars, Refs, MaxId
                    ),
                    %% Cache the layout
                    NewT = T#{CacheKey => {Scalars, Refs, VT, AllFields, BaseTableSize, Slots}},
                    NewC = C#layout_cache{tables = NewT},
                    case maps:is_key(VT, S) of
                        true -> {Acc, S, NewC};
                        false -> {[VT | Acc], S#{VT => true}, NewC}
                    end
                end,
                {VTAcc, Seen, Cache},
                Values
            );
        _ ->
            {VTAcc, Seen, Cache}
    end;
collect_vtables_from_ref(Type, Value, Defs, VTAcc, Seen, Cache) when is_atom(Type), is_map(Value) ->
    %% Nested table
    case maps:get(Type, Defs, undefined) of
        #table_def{scalars = ScalarDefs, refs = RefDefs, max_id = MaxId} ->
            CacheKey = {Type, Value},
            Scalars = collect_field_values(Value, ScalarDefs, Defs, #{}),
            Refs = collect_field_values(Value, RefDefs, Defs, #{}),
            {VT, AllFields, BaseTableSize, Slots} = compute_table_layout(Scalars, Refs, MaxId),
            %% Cache the layout
            #layout_cache{tables = T} = Cache,
            NewCache = Cache#layout_cache{
                tables = T#{CacheKey => {Scalars, Refs, VT, AllFields, BaseTableSize, Slots}}
            },
            case maps:is_key(VT, Seen) of
                true -> {VTAcc, Seen, NewCache};
                false -> {[VT | VTAcc], Seen#{VT => true}, NewCache}
            end;
        _ ->
            {VTAcc, Seen, Cache}
    end;
collect_vtables_from_ref(
    {union_value, UnionName}, #{type := MemberType, value := Value}, Defs, VTAcc, Seen, Cache
) ->
    %% Union value - cache the member table layout
    {union, _, _} = maps:get(UnionName, Defs),
    collect_vtables_from_ref(MemberType, Value, Defs, VTAcc, Seen, Cache);
collect_vtables_from_ref(_, _, _, VTAcc, Seen, Cache) ->
    {VTAcc, Seen, Cache}.

%% Build ref data with vtable sharing, returning the position of the shared vtable
%% RefFieldsOrdered: list of #field{} records with offset set
%% Returns: {RefDataIoList, RefPositions, SharedVTablePos}
build_ref_data_with_vtable_sharing(RefFieldsOrdered, RefDataStart, SharedVTable, Defs, LayoutCache) ->
    SharedVTableBin = iolist_to_binary(SharedVTable),
    %% Encode refs and track shared vtable position and ref positions
    {DataIoList, Positions, VTablePos, _} = lists:foldl(
        fun(
            #field{type = Type, value = Value, offset = FieldOff}, {DataAcc, PosAcc, VTPos, DataPos}
        ) ->
            %% Add padding for 8-byte vector alignment if needed
            AlignPad = calc_vector_alignment_padding(Type, DataPos, Defs),
            PaddedPos = DataPos + AlignPad,

            DataIo = encode_ref(Type, Value, Defs, LayoutCache),
            DataSize = iolist_size(DataIo),

            %% For nested tables, uoffset should point to soffset (after vtable)
            RefTargetPos =
                case is_nested_table_type_cached(Type, Value, Defs, LayoutCache) of
                    {true, VTableSize} -> PaddedPos + VTableSize;
                    false -> PaddedPos
                end,

            %% Search for shared vtable in this ref's data (if not already found)
            NewVTPos =
                case VTPos of
                    0 ->
                        %% Haven't found it yet - search
                        DataBin = iolist_to_binary(DataIo),
                        case find_vtable_in_binary(DataBin, SharedVTableBin) of
                            {found, RelPos} -> PaddedPos + RelPos;
                            not_found -> 0
                        end;
                    _ ->
                        %% Already found
                        VTPos
                end,

            PadBin = <<0:(AlignPad * 8)>>,
            {
                [DataIo, PadBin | DataAcc],
                PosAcc#{FieldOff => RefTargetPos},
                NewVTPos,
                PaddedPos + DataSize
            }
        end,
        {[], #{}, 0, RefDataStart},
        RefFieldsOrdered
    ),
    {lists:reverse(DataIoList), Positions, VTablePos}.

%% Search for vtable bytes in a binary
find_vtable_in_binary(Bin, VTBin) ->
    VTSize = byte_size(VTBin),
    find_vtable_in_binary(Bin, VTBin, VTSize, 0).

find_vtable_in_binary(Bin, VTBin, VTSize, Offset) when byte_size(Bin) >= VTSize ->
    case Bin of
        <<VTBin:VTSize/binary, _/binary>> ->
            {found, Offset};
        <<_, Rest/binary>> ->
            find_vtable_in_binary(Rest, VTBin, VTSize, Offset + 1)
    end;
find_vtable_in_binary(_, _, _, _) ->
    not_found.

%% Build ref data and return map of field_offset -> data_position
%% For nested tables, position points to soffset, not vtable start
build_ref_data(RefFields, RefDataStart, Defs, LayoutCache) ->
    encode_refs_with_positions(RefFields, RefDataStart, Defs, LayoutCache, fun encode_ref/4).

%% Common helper for encoding refs and tracking positions.
%% Used by build_ref_data, build_ref_data_minimal_padding, etc.
%% EncoderFun: fun(Type, Value, Defs, LayoutCache) -> iodata()
encode_refs_with_positions(RefFields, RefDataStart, Defs, LayoutCache, EncoderFun) ->
    {DataIoList, Positions, _} = lists:foldl(
        fun(#field{type = Type, value = Value, offset = FieldOff}, {DataAcc, PosAcc, DataPos}) ->
            AlignPad = calc_vector_alignment_padding(Type, DataPos, Defs),
            PaddedPos = DataPos + AlignPad,
            DataIo = EncoderFun(Type, Value, Defs, LayoutCache),
            RefTargetPos =
                case is_nested_table_type_cached(Type, Value, Defs, LayoutCache) of
                    {true, VTableSize} -> PaddedPos + VTableSize;
                    false -> PaddedPos
                end,
            PadBin = <<0:(AlignPad * 8)>>,
            {
                [DataIo, PadBin | DataAcc],
                PosAcc#{FieldOff => RefTargetPos},
                PaddedPos + iolist_size(DataIo)
            }
        end,
        {[], #{}, RefDataStart},
        RefFields
    ),
    {lists:reverse(DataIoList), Positions}.

%% Calculate padding needed for vector 8-byte alignment
%% Vector data (after 4-byte length) must be 8-byte aligned for 8-byte elements
calc_vector_alignment_padding(#vector_def{element_type = ElemType}, DataPos, Defs) ->
    calc_vector_alignment_padding({vector, ElemType}, DataPos, Defs);
calc_vector_alignment_padding({vector, ElemType}, DataPos, Defs) ->
    ResolvedType = resolve_type(ElemType, Defs),
    case type_size(ResolvedType) of
        8 ->
            %% Need (DataPos + 4) % 8 == 0, so DataPos % 8 == 4
            (12 - (DataPos rem 8)) rem 8;
        _ ->
            0
    end;
calc_vector_alignment_padding(_, _, _) ->
    0.

%% Check if type is a nested table (vtable is at START, need offset to point to soffset)
%% Returns {true, VTableSize} or false
%% VTable size only depends on MaxId (from schema), not field values
is_nested_table_type_cached(Type, _Value, Defs, _Cache) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        #table_def{max_id = MaxId} ->
            VTableSize = calc_vtable_size(MaxId),
            {true, VTableSize};
        _ ->
            false
    end;
is_nested_table_type_cached(
    {union_value, UnionName}, #{type := MemberType, value := Value}, Defs, Cache
) ->
    %% Union value - check the member type
    {union, _, _} = maps:get(UnionName, Defs),
    is_nested_table_type_cached(MemberType, Value, Defs, Cache);
is_nested_table_type_cached(_, _, _, _) ->
    false.

%% Calculate vtable size for a table (uses precomputed MaxId)
calc_vtable_size(MaxId) ->
    case MaxId of
        -1 -> 4;
        _ -> 4 + ((MaxId + 1) * 2)
    end.

%% Build inline table data (scalars inline, refs as uoffsets)
%% Returns iolist to avoid intermediate binary creation
build_inline_data(FieldLayout, TablePos, RefPositions) ->
    {DataReversed, _} = lists:foldl(
        fun(
            #field{type = Type, value = Value, offset = FieldOff, is_scalar = IsScalar},
            {Acc, CurOff}
        ) ->
            %% Add padding if needed
            Pad = FieldOff - CurOff,

            %% Encode the field
            FieldIo =
                case IsScalar of
                    true ->
                        encode_scalar(Value, Type);
                    false ->
                        %% Reference - write uoffset to ref data
                        FieldAbsPos = TablePos + FieldOff,
                        RefDataPos = maps:get(FieldOff, RefPositions),
                        UOffset = RefDataPos - FieldAbsPos,
                        <<UOffset:32/little-signed>>
                end,
            FieldSize = iolist_size(FieldIo),
            case Pad of
                0 -> {[FieldIo | Acc], FieldOff + FieldSize};
                _ -> {[FieldIo, <<0:(Pad * 8)>> | Acc], FieldOff + FieldSize}
            end
        end,
        %% Start at offset 4
        {[], 4},
        FieldLayout
    ),
    lists:reverse(DataReversed).

%% Encode string as iolist - preserves sub-binary references
encode_string(Bin) ->
    Len = byte_size(Bin),
    TotalLen = 4 + Len + 1,
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    [<<Len:32/little>>, Bin, <<0, 0:(PadLen * 8)>>].

%% Encode byte vector from binary - allows natural Erlang idiom for [ubyte]
encode_byte_vector(Bin) ->
    Len = byte_size(Bin),
    TotalLen = 4 + Len,
    PadLen = (4 - (TotalLen rem 4)) rem 4,
    [<<Len:32/little>>, Bin, <<0:(PadLen * 8)>>].

%% Unwrap types with default values (not type constructors)
encode_ref({TypeName, Default}, Value, Defs, LayoutCache) when
    is_atom(TypeName),
    is_atom(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    encode_ref(TypeName, Value, Defs, LayoutCache);
encode_ref({TypeName, Default}, Value, Defs, LayoutCache) when
    is_atom(TypeName),
    is_number(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    encode_ref(TypeName, Value, Defs, LayoutCache);
encode_ref({TypeName, Default}, Value, Defs, LayoutCache) when
    is_atom(TypeName),
    is_boolean(Default),
    TypeName /= vector,
    TypeName /= enum,
    TypeName /= struct,
    TypeName /= array,
    TypeName /= union_type,
    TypeName /= union_value
->
    encode_ref(TypeName, Value, Defs, LayoutCache);
encode_ref(string, Bin, _Defs, _LayoutCache) when is_binary(Bin) ->
    %% Return iolist to preserve sub-binary references
    encode_string(Bin);
%% Vector with enriched record - delegate to tuple form
encode_ref(#vector_def{element_type = ElemType}, Bin, Defs, LayoutCache) when is_binary(Bin) ->
    encode_ref({vector, ElemType}, Bin, Defs, LayoutCache);
encode_ref(#vector_def{element_type = ElemType}, Values, Defs, LayoutCache) when is_list(Values) ->
    encode_ref({vector, ElemType}, Values, Defs, LayoutCache);
encode_ref({vector, ElemType}, Bin, _Defs, _LayoutCache) when
    is_binary(Bin),
    (ElemType == ubyte orelse ElemType == byte orelse ElemType == int8 orelse ElemType == uint8)
->
    %% Allow binaries for byte vectors - natural Erlang idiom
    encode_byte_vector(Bin);
encode_ref({vector, ElemType}, Values, Defs, LayoutCache) when is_list(Values) ->
    encode_vector(ElemType, Values, Defs, LayoutCache);
encode_ref({union_value, UnionName}, #{type := MemberType, value := Value}, Defs, LayoutCache) ->
    %% Union value - encode as the member table type
    {union, _, _} = maps:get(UnionName, Defs),
    encode_nested_table(MemberType, Value, Defs, LayoutCache);
encode_ref(TableType, Map, Defs, LayoutCache) when is_atom(TableType), is_map(Map) ->
    %% Nested table - build it inline
    encode_nested_table(TableType, Map, Defs, LayoutCache).

encode_vector(ElemType, Values, Defs, LayoutCache) ->
    ResolvedType = resolve_type(ElemType, Defs),
    case is_scalar_type(ResolvedType) of
        true ->
            %% Scalar vector: length + inline elements
            Len = length(Values),
            Elements = [encode_scalar(V, ResolvedType) || V <- Values],
            %% Calculate size without flattening
            ElementsSize = iolist_size(Elements),
            %% Pad to 4-byte boundary
            TotalLen = 4 + ElementsSize,
            PadLen = (4 - (TotalLen rem 4)) rem 4,
            [<<Len:32/little>>, Elements, <<0:(PadLen * 8)>>];
        false ->
            %% Reference vector (e.g., strings, tables): length + offsets, then data
            encode_ref_vector(ElemType, Values, Defs, LayoutCache)
    end.

encode_ref_vector(ElemType, Values, Defs, LayoutCache) ->
    case ElemType of
        string ->
            encode_string_vector_dedup(Values);
        _ ->
            encode_ref_vector_standard(ElemType, Values, Defs, LayoutCache)
    end.

encode_string_vector_dedup(Values) ->
    %% String vector - flatc writes data in REVERSE order
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Encode strings in reverse order (like flatc does)
    ReversedValues = lists:reverse(Values),
    {DataIoLists, PosByValue, _} = lists:foldl(
        fun(Str, {DataAcc, PosMap, DataPos}) ->
            case maps:get(Str, PosMap, undefined) of
                undefined ->
                    DataIo = encode_string(Str),
                    {[DataIo | DataAcc], PosMap#{Str => DataPos}, DataPos + iolist_size(DataIo)};
                _CachedPos ->
                    %% Already encoded, reuse position
                    {DataAcc, PosMap, DataPos}
            end
        end,
        {[], #{}, HeaderSize},
        ReversedValues
    ),

    %% Build offsets for original order, pointing to reverse-order data
    Offsets = lists:map(
        fun({Idx, Str}) ->
            OffsetPos = 4 + (Idx * 4),
            DataPos = maps:get(Str, PosByValue),
            UOffset = DataPos - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), Values)
    ),

    %% Return as iolist - preserves sub-binary references until final flatten
    [<<Len:32/little>>, Offsets, lists:reverse(DataIoLists)].

encode_ref_vector_standard(ElemType, Values, Defs, LayoutCache) ->
    %% Check if this is a table vector that needs vtable sharing
    case maps:get(ElemType, Defs, undefined) of
        #table_def{} ->
            encode_table_vector_with_sharing(ElemType, Values, Defs, LayoutCache);
        _ ->
            encode_ref_vector_simple(ElemType, Values, Defs, LayoutCache)
    end.

%% Simple ref vector encoding (non-table elements)
encode_ref_vector_simple(ElemType, Values, Defs, LayoutCache) ->
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    ReversedValues = lists:reverse(Values),
    EncodedElems = [{encode_ref(ElemType, V, Defs, LayoutCache), V} || V <- ReversedValues],

    {_, ElemPositions} = lists:foldl(
        fun({ElemData, Value}, {DataPos, PosAcc}) ->
            VTableOffset =
                case is_nested_table_type_cached(ElemType, Value, Defs, LayoutCache) of
                    {true, VTableSize} -> VTableSize;
                    false -> 0
                end,
            {DataPos + iolist_size(ElemData), [{DataPos, VTableOffset, ElemData} | PosAcc]}
        end,
        {HeaderSize, []},
        EncodedElems
    ),

    Offsets = lists:map(
        fun({Idx, {DataPos, VTableOffset, _}}) ->
            OffsetPos = 4 + (Idx * 4),
            UOffset = (DataPos + VTableOffset) - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), ElemPositions)
    ),

    [<<Len:32/little>>, Offsets, [Data || {Data, _} <- EncodedElems]].

%% Table vector encoding with vtable sharing (flatc compatible)
%% Layout: elements with vtable-after first, then shared vtable, then elements with vtable-before
%%
%% flatc processes elements in reverse order (last first), and for each unique vtable:
%% - The FIRST element processed (LAST in original order) gets vtable-after
%% - The LAST element processed (FIRST in original order) gets vtable-before
%% - The vtable is placed immediately before the vtable-before element
encode_table_vector_with_sharing(TableType, Values, Defs, #layout_cache{tables = T} = LayoutCache) ->
    Len = length(Values),
    OffsetsSize = Len * 4,
    HeaderSize = 4 + OffsetsSize,

    %% Process in reverse order (last element first, like flatc)
    IndexedValues = lists:zip(lists:seq(0, Len - 1), Values),
    ReversedIndexedValues = lists:reverse(IndexedValues),

    %% Collect vtable info and layout for each element - use cached layouts when available
    #table_def{scalars = ScalarDefs, refs = RefDefs, max_id = MaxId} = maps:get(TableType, Defs),
    ElemLayouts = lists:map(
        fun({OrigIdx, Value}) ->
            CacheKey = {TableType, Value},
            case maps:get(CacheKey, T, undefined) of
                {_Scalars, Refs, VTable, AllFields, BaseTableSize, Slots} ->
                    %% Use cached layout
                    {OrigIdx, VTable, AllFields, BaseTableSize, Slots, Refs};
                undefined ->
                    %% Not cached - compute (rare case, shouldn't happen if cache is populated)
                    Scalars = collect_field_values(Value, ScalarDefs, Defs, #{}),
                    Refs = collect_field_values(Value, RefDefs, Defs, #{}),
                    {VTable, AllFields, BaseTableSize, Slots} = compute_table_layout(
                        Scalars, Refs, MaxId
                    ),
                    {OrigIdx, VTable, AllFields, BaseTableSize, Slots, Refs}
            end
        end,
        ReversedIndexedValues
    ),

    %% Identify vtable owners: FIRST element in original order (LAST in processing order)
    %% = element with LOWEST OrigIdx for each vtable
    VTableOwners = lists:foldl(
        fun({OrigIdx, VTable, _AllFields, _BaseTableSize, _Slots, _Refs}, Acc) ->
            case maps:get(VTable, Acc, undefined) of
                undefined -> Acc#{VTable => OrigIdx};
                ExistingIdx when OrigIdx < ExistingIdx -> Acc#{VTable => OrigIdx};
                _ -> Acc
            end
        end,
        #{},
        ElemLayouts
    ),

    %% Encode all elements using precomputed layout (no duplicate merge/slots computation)
    EncodedElems = lists:map(
        fun({OrigIdx, VTable, AllFields, BaseTableSize, Slots, _Refs}) ->
            OwnerIdx = maps:get(VTable, VTableOwners),
            IsOwner = OrigIdx == OwnerIdx,
            %% Owner elements use normal padding (they're last and need trailing padding)
            %% Non-owner elements use minimal padding (followed by vtable which needs 2-byte align)
            {BodySize, BodyIo} =
                case IsOwner of
                    true ->
                        encode_table_body_with_layout(
                            AllFields, BaseTableSize, Slots, Defs, normal, LayoutCache
                        );
                    false ->
                        encode_table_body_with_layout(
                            AllFields, BaseTableSize, Slots, Defs, minimal, LayoutCache
                        )
                end,
            %% Total size includes 4-byte soffset
            TotalSize = 4 + BodySize,
            {OrigIdx, VTable, TotalSize, BodyIo, IsOwner}
        end,
        ElemLayouts
    ),

    %% Buffer order is same as ElemVTables order (reverse of original)
    %% But we need to insert vtables between vtable-after and vtable-before elements
    %%
    %% For each vtable group:
    %% - Non-owner elements come first (vtable-after, negative soffset)
    %% - Then the vtable
    %% - Then the owner element (vtable-before, positive soffset)

    %% Group elements by vtable and sort within each group
    VTableGroups = lists:foldl(
        fun({OrigIdx, VTable, TotalSize, BodyIo, IsOwner}, Acc) ->
            Group = maps:get(VTable, Acc, []),
            Acc#{VTable => [{OrigIdx, TotalSize, BodyIo, IsOwner} | Group]}
        end,
        #{},
        EncodedElems
    ),

    %% Build buffer: for each vtable group (in processing order), output non-owners, vtable, owner
    %% Sort vtable groups by the highest OrigIdx in each group (processing order)
    VTableGroupList = maps:to_list(VTableGroups),
    SortedVTableGroups = lists:sort(
        fun({_VT1, G1}, {_VT2, G2}) ->
            MaxIdx1 = lists:max([Idx || {Idx, _, _, _} <- G1]),
            MaxIdx2 = lists:max([Idx || {Idx, _, _, _} <- G2]),
            %% Descending by max index (first processed first)
            MaxIdx1 > MaxIdx2
        end,
        VTableGroupList
    ),
    SortedByVTable = lists:flatmap(
        fun({VTable, Group}) ->
            {Owners, NonOwners} = lists:partition(fun({_, _, _, IsOwner}) -> IsOwner end, Group),
            %% Non-owners in descending OrigIdx order (same as processing order)
            SortedNonOwners = lists:sort(
                fun({IdxA, _, _, _}, {IdxB, _, _, _}) -> IdxA > IdxB end,
                NonOwners
            ),
            %% Convert to buffer elements with {Type, OrigIdx, TotalSize, BodyIo, VTable}
            NonOwnerElems = [
                {vtable_after, Idx, Size, Body, VTable}
             || {Idx, Size, Body, _} <- SortedNonOwners
            ],
            OwnerElems = [
                {vtable_before, Idx, Size, Body, VTable}
             || {Idx, Size, Body, _} <- Owners
            ],
            %% Insert vtable before owner
            NonOwnerElems ++ [{vtable_insert, VTable}] ++ OwnerElems
        end,
        SortedVTableGroups
    ),

    %% Calculate positions with proper alignment
    %% Tables must be 4-byte aligned, so vtables need padding to ensure
    %% the following vtable_before element is 4-byte aligned
    {ElemInfos, VTablePositions, PaddingMap, _FinalPos} = lists:foldl(
        fun(Elem, {InfoAcc, VTPos, PadMap, DataPos}) ->
            case Elem of
                {vtable_insert, VTable} ->
                    VTSize = vtable_size(VTable),
                    %% Calculate padding needed so that (DataPos + Pad + VTSize) is 4-byte aligned
                    %% This ensures the vtable_before element after the vtable is aligned
                    AlignPad = (4 - ((DataPos + VTSize) rem 4)) rem 4,
                    PaddedPos = DataPos + AlignPad,
                    {InfoAcc, VTPos#{VTable => PaddedPos}, PadMap#{VTable => AlignPad},
                        PaddedPos + VTSize};
                {vtable_after, OrigIdx, TotalSize, BodyIo, _VTable} ->
                    %% soffset is at start of body
                    SOffsetPos = DataPos,
                    {
                        [{OrigIdx, SOffsetPos, BodyIo} | InfoAcc],
                        VTPos,
                        PadMap,
                        DataPos + TotalSize
                    };
                {vtable_before, OrigIdx, TotalSize, BodyIo, _VTable} ->
                    %% soffset is at start of body
                    SOffsetPos = DataPos,
                    {
                        [{OrigIdx, SOffsetPos, BodyIo} | InfoAcc],
                        VTPos,
                        PadMap,
                        DataPos + TotalSize
                    }
            end
        end,
        {[], #{}, #{}, HeaderSize},
        SortedByVTable
    ),

    %% Sort by original index to build correct offset array
    ElemInfosByOrigIdx = lists:sort(fun({IdxA, _, _}, {IdxB, _, _}) -> IdxA =< IdxB end, ElemInfos),

    %% Build offsets array in original order
    Offsets = lists:map(
        fun({Idx, {_OrigIdx, SOffsetPos, _BodyIo}}) ->
            OffsetPos = 4 + (Idx * 4),
            UOffset = SOffsetPos - OffsetPos,
            <<UOffset:32/little-signed>>
        end,
        lists:zip(lists:seq(0, Len - 1), ElemInfosByOrigIdx)
    ),

    %% Build lookup map for soffset positions
    SOffsetPosMap = maps:from_list([{Idx, Pos} || {Idx, Pos, _BodyIo} <- ElemInfos]),

    %% Build data section with proper soffsets
    DataIo = lists:map(
        fun(Elem) ->
            case Elem of
                {vtable_insert, VTable} ->
                    %% Insert padding before vtable for alignment
                    PadLen = maps:get(VTable, PaddingMap, 0),
                    [<<0:(PadLen * 8)>>, VTable];
                {vtable_after, OrigIdx, _TotalSize, BodyIo, VTable} ->
                    %% Negative soffset (vtable is after this element)
                    VTablePos = maps:get(VTable, VTablePositions),
                    SOffsetPos = maps:get(OrigIdx, SOffsetPosMap),
                    %% Negative
                    SOffset = SOffsetPos - VTablePos,
                    [<<SOffset:32/little-signed>>, BodyIo];
                {vtable_before, OrigIdx, _TotalSize, BodyIo, VTable} ->
                    %% Positive soffset (vtable is before this element)
                    VTablePos = maps:get(VTable, VTablePositions),
                    SOffsetPos = maps:get(OrigIdx, SOffsetPosMap),
                    %% Positive
                    SOffset = SOffsetPos - VTablePos,
                    [<<SOffset:32/little-signed>>, BodyIo]
            end
        end,
        SortedByVTable
    ),

    [<<Len:32/little>>, Offsets, DataIo].

%% Encode table body with precomputed layout (avoids duplicate computation)
%% PaddingMode: normal | minimal (for vtable sharing)
encode_table_body_with_layout(AllFields, BaseTableSize, Slots, Defs, PaddingMode, LayoutCache) ->
    TablePos = 0,
    FieldLayout = [F#field{offset = maps:get(F#field.id, Slots)} || F <- AllFields],
    SortedLayout = lists:sort(fun(A, B) -> A#field.offset =< B#field.offset end, FieldLayout),
    RefFields = [F || F <- SortedLayout, not F#field.is_scalar],
    RefFieldsOrdered = sort_refs_flatc_order(RefFields),
    RefPadding = calc_ref_padding_cached(
        RefFieldsOrdered, TablePos + BaseTableSize, Defs, LayoutCache
    ),
    TableSize = BaseTableSize + RefPadding,
    RefDataStart = TablePos + TableSize,
    {RefDataIo, RefPositions} =
        case PaddingMode of
            minimal ->
                build_ref_data_minimal_padding(RefFieldsOrdered, RefDataStart, Defs, LayoutCache);
            normal ->
                build_ref_data(RefFieldsOrdered, RefDataStart, Defs, LayoutCache)
        end,
    TableDataIo = build_inline_data(SortedLayout, TablePos, RefPositions),
    TableData =
        case RefPadding of
            0 -> TableDataIo;
            _ -> [TableDataIo, <<0:(RefPadding * 8)>>]
        end,
    %% Return {BodySize, BodyIoList} - caller will prepend soffset
    %% This avoids iolist_to_binary and allows caller to set correct soffset
    BodyIoList = [TableData, RefDataIo],
    BodySize = iolist_size(BodyIoList),
    {BodySize, BodyIoList}.

%% Build ref data with minimal string padding
build_ref_data_minimal_padding(RefFields, RefDataStart, Defs, LayoutCache) ->
    encode_refs_with_positions(
        RefFields, RefDataStart, Defs, LayoutCache, fun encode_ref_minimal_padding/4
    ).

%% Encode ref with minimal string padding (no 4-byte alignment padding)
%% Returns iolist to preserve sub-binary references
encode_ref_minimal_padding(string, Bin, _Defs, _LayoutCache) when is_binary(Bin) ->
    Len = byte_size(Bin),
    %% No padding - just length + data + null terminator
    [<<Len:32/little>>, Bin, <<0>>];
encode_ref_minimal_padding(Type, Value, Defs, LayoutCache) ->
    encode_ref(Type, Value, Defs, LayoutCache).

encode_nested_table(TableType, Map, Defs, _LayoutCache) ->
    %% Build a nested table - vtable first, then table (positive soffset, like flatc)
    #table_def{scalars = ScalarDefs, refs = RefDefs, max_id = MaxId} = maps:get(TableType, Defs),
    Scalars = collect_field_values(Map, ScalarDefs, Defs, #{}),
    Refs = collect_field_values(Map, RefDefs, Defs, #{}),

    VTable = build_vtable(Scalars, Refs, MaxId),
    VTableSize = vtable_size(VTable),

    %% Layout: vtable | soffset | table_data | ref_data
    %% soffset is positive, pointing back to vtable start
    {TableData, RefDataIo} = build_table_data(Scalars, Refs, VTableSize, Defs),

    %% Positive = backward to vtable at start
    SOffset = VTableSize,

    [VTable, <<SOffset:32/little-signed>>, TableData, RefDataIo].

%% =============================================================================
%% Scalar Encoding
%% =============================================================================

encode_scalar(false, bool) ->
    <<0:8>>;
encode_scalar(true, bool) ->
    <<1:8>>;
encode_scalar(Value, byte) ->
    <<Value:8/signed>>;
encode_scalar(Value, int8) ->
    <<Value:8/signed>>;
encode_scalar(Value, ubyte) ->
    <<Value:8/unsigned>>;
encode_scalar(Value, uint8) ->
    <<Value:8/unsigned>>;
encode_scalar(Value, short) ->
    <<Value:16/little-signed>>;
encode_scalar(Value, int16) ->
    <<Value:16/little-signed>>;
encode_scalar(Value, ushort) ->
    <<Value:16/little-unsigned>>;
encode_scalar(Value, uint16) ->
    <<Value:16/little-unsigned>>;
encode_scalar(Value, int) ->
    <<Value:32/little-signed>>;
encode_scalar(Value, int32) ->
    <<Value:32/little-signed>>;
encode_scalar(Value, uint) ->
    <<Value:32/little-unsigned>>;
encode_scalar(Value, uint32) ->
    <<Value:32/little-unsigned>>;
encode_scalar(Value, long) ->
    <<Value:64/little-signed>>;
encode_scalar(Value, int64) ->
    <<Value:64/little-signed>>;
encode_scalar(Value, ulong) ->
    <<Value:64/little-unsigned>>;
encode_scalar(Value, uint64) ->
    <<Value:64/little-unsigned>>;
encode_scalar(Value, float) ->
    <<Value:32/little-float>>;
encode_scalar(Value, float32) ->
    <<Value:32/little-float>>;
encode_scalar(Value, double) ->
    <<Value:64/little-float>>;
encode_scalar(Value, float64) ->
    <<Value:64/little-float>>;
encode_scalar(Value, {enum, Base}) ->
    encode_scalar(Value, Base);
encode_scalar(Value, {enum, Base, IndexMap}) when is_atom(Value), is_map(IndexMap) ->
    %% O(1) lookup using precomputed index map from schema
    case maps:find(Value, IndexMap) of
        {ok, Index} -> encode_scalar(Index, Base);
        error -> error({unknown_enum_value, Value, maps:keys(IndexMap)})
    end;
encode_scalar(Value, {enum, Base, _IndexMap}) when is_integer(Value) ->
    %% Already an integer, use directly
    encode_scalar(Value, Base);
encode_scalar(Map, #struct_def{fields = Fields}) when is_map(Map) ->
    encode_struct(Map, Fields);
encode_scalar(Map, {struct, Fields}) when is_map(Map) ->
    encode_struct(Map, Fields);
encode_scalar(List, {array, ElemType, Count}) when is_list(List) ->
    encode_array(List, ElemType, Count);
encode_scalar(TypeIndex, {union_type, _UnionName}) when is_integer(TypeIndex) ->
    <<TypeIndex:8/unsigned>>.

%% Encode fixed-size array as inline data
encode_array(List, ElemType, Count) ->
    length(List) == Count orelse
        error({array_length_mismatch, expected, Count, got, length(List)}),
    [encode_scalar(Elem, ElemType) || Elem <- List].

%% Encode struct as inline data (returns iolist)
%% Handles both enriched format (maps with precomputed offsets) and raw tuple format
encode_struct(Map, Fields) ->
    {IoReversed, EndOff} = lists:foldl(
        fun
            (#{name := Name, type := Type, offset := FieldOff, size := Size}, {Acc, Off}) ->
                %% Enriched field - use precomputed offset
                Pad = FieldOff - Off,
                Value = get_field_value(Map, Name),
                ValIo = encode_scalar(Value, Type),
                case Pad of
                    0 -> {[ValIo | Acc], FieldOff + Size};
                    _ -> {[ValIo, <<0:(Pad * 8)>> | Acc], FieldOff + Size}
                end;
            ({Name, Type}, {Acc, Off}) ->
                %% Raw tuple format
                Size = type_size(Type),
                AlignedOff = align_offset(Off, Size),
                Pad = AlignedOff - Off,
                Value = get_field_value(Map, Name),
                ValIo = encode_scalar(Value, Type),
                case Pad of
                    0 -> {[ValIo | Acc], AlignedOff + Size};
                    _ -> {[ValIo, <<0:(Pad * 8)>> | Acc], AlignedOff + Size}
                end
        end,
        {[], 0},
        Fields
    ),
    %% Pad to struct alignment
    StructSize = calc_struct_size(Fields),
    TrailingPad = StructSize - EndOff,
    case TrailingPad of
        0 -> lists:reverse(IoReversed);
        _ -> lists:reverse([<<0:(TrailingPad * 8)>> | IoReversed])
    end.

%% =============================================================================
%% Helpers
%% =============================================================================

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
type_size(long) -> 8;
type_size(ulong) -> 8;
type_size(int64) -> 8;
type_size(uint64) -> 8;
type_size(float) -> 4;
type_size(float32) -> 4;
type_size(double) -> 8;
type_size(float64) -> 8;
type_size({enum, Base}) -> type_size(Base);
type_size({enum, Base, _Values}) -> type_size(Base);
type_size(#struct_def{total_size = TotalSize}) -> TotalSize;
type_size({struct, Fields}) -> calc_struct_size(Fields);
type_size({array, ElemType, Count}) -> type_size(ElemType) * Count;
%% Union type is ubyte
type_size({union_type, _}) -> 1;
type_size(_) -> 4.

%% Calculate struct size with proper alignment
%% Handles both enriched format (maps) and raw tuple format
calc_struct_size(Fields) ->
    {_, EndOffset, MaxAlign} = lists:foldl(
        fun
            (#{offset := FieldOff, size := Size}, {_, _, MaxAlignAcc}) ->
                %% Enriched field - use precomputed values
                {ok, FieldOff + Size, max(MaxAlignAcc, Size)};
            ({_Name, Type}, {_, CurOffset, MaxAlignAcc}) ->
                %% Raw tuple format
                Size = type_size(Type),
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test vtable sharing between root and nested table with identical structure
vtable_sharing_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_nested.fbs"),
    Map = #{name => <<"Player">>, hp => 200, pos => #{x => 1.5, y => 2.5, z => 3.5}},

    %% Encode and decode roundtrip
    Buffer = iolist_to_binary(from_map(Map, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    ?assertEqual(200, maps:get(hp, Result)),
    ?assertEqual(<<"Player">>, maps:get(name, Result)),
    Pos = maps:get(pos, Result),
    ?assertEqual(1.5, maps:get(x, Pos)),
    ?assertEqual(2.5, maps:get(y, Pos)),
    ?assertEqual(3.5, maps:get(z, Pos)).

%% Test zero-copy: re-encoding should not create new refc binaries
zero_copy_reencode_test() ->
    {ok, Schema} = flatbuferl_schema:parse_file("test/vectors/test_monster.fbs"),

    %% Create a message with a large string (>64 bytes to be a refc binary)
    LargeString = list_to_binary(lists:duplicate(200, $X)),
    Map = #{name => LargeString, hp => 100, mana => 50},

    %% Encode to flatbuffer
    Buffer = iolist_to_binary(from_map(Map, Schema)),

    %% Run in isolated process to measure binaries accurately
    Result = run_in_isolated_process(fun() ->
        %% Receive buffer - should have 1 refc binary
        erlang:garbage_collect(),
        Bins1 = get_refc_binary_ids(),

        %% Deserialize - should still have same binary (zero-copy decode)
        Ctx = flatbuferl:new(Buffer, Schema),
        DecodedMap = flatbuferl:to_map(Ctx),
        erlang:garbage_collect(),
        Bins2 = get_refc_binary_ids(),

        %% Re-encode to iolist - should NOT create new refc binaries
        ReEncoded = from_map(DecodedMap, Schema),
        erlang:garbage_collect(),
        Bins3 = get_refc_binary_ids(),

        %% Use ReEncoded to prevent it being optimized away before GC check
        _ = iolist_size(ReEncoded),

        {Bins1, Bins2, Bins3}
    end),

    {Bins1, Bins2, Bins3} = Result,

    %% Assertions
    ?assertEqual(1, length(Bins1), "Should have exactly 1 refc binary (the buffer)"),
    ?assertEqual(Bins1, Bins2, "Deserialize should not create new refc binaries"),
    ?assertEqual(Bins1, Bins3, "Re-encode should not create new refc binaries").

get_refc_binary_ids() ->
    {binary, Bins} = erlang:process_info(self(), binary),
    lists:usort([Id || {Id, _, _} <- Bins]).

run_in_isolated_process(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    spawn_link(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Result} -> Result
    after 5000 -> error(timeout)
    end.

-endif.
