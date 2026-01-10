%% @private
%% Internal record definitions for flatbuferl
%% This header is private to the library - do not include in external code

%% Field definition from schema (immutable after parse)
-record(field_def, {
    name :: atom(),
    %% Precomputed binary name for fast map lookup during encode
    binary_name :: binary(),
    id :: non_neg_integer(),
    type :: term(),
    resolved_type :: term(),
    default :: term(),
    required = false :: boolean(),
    deprecated = false :: boolean(),
    inline_size :: non_neg_integer(),
    is_scalar :: boolean(),
    %% true only for primitive scalars (bool, int8-64, uint8-64, float32/64)
    is_primitive :: boolean(),
    %% size * 65536 + id for sorting
    layout_key :: non_neg_integer()
}).

%% Precomputed encoding layout for "all fields present" case
-record(encode_layout, {
    %% Precomputed vtable bytes (iolist) for all fields present
    vtable :: iolist(),
    vtable_size :: non_neg_integer(),
    %% Table size when all fields present
    table_size :: non_neg_integer(),
    %% Map of field_id => {offset, size} for quick adjustment
    %% Offset is from start of table data (after soffset)
    slots :: #{non_neg_integer() => {non_neg_integer(), non_neg_integer()}},
    %% Precomputed offset-only slots for fast path (field_id => offset)
    slot_offsets :: #{non_neg_integer() => non_neg_integer()},
    %% Fields in encoding order (scalars by layout_key, then refs in flatc order)
    scalars_order :: [#field_def{}],
    refs_order :: [#field_def{}],
    %% All field IDs for quick "all present" check
    all_field_ids :: [non_neg_integer()],
    %% Max field ID (for vtable sizing)
    max_id :: integer(),
    %% True if any ref has id=0 (affects flatc sort order)
    has_id_zero_ref = false :: boolean()
}).

%% Table definition with precomputed layout
-record(table_def, {
    %% Original field definitions partitioned and sorted
    scalars :: [#field_def{}],
    refs :: [#field_def{}],
    all_fields :: [#field_def{}],
    %% Field name -> field_def for O(1) lookup
    field_map :: #{atom() => #field_def{}},
    %% Precomputed encoding layout
    encode_layout :: #encode_layout{},
    %% Max field ID
    max_id :: integer()
}).

%% Field with value (used during encoding)
-record(field, {
    id :: non_neg_integer(),
    type :: term(),
    value :: term(),
    size :: non_neg_integer(),
    is_scalar :: boolean(),
    layout_key :: non_neg_integer(),
    offset :: non_neg_integer() | undefined
}).

%% Cache for table layouts during encoding
-record(layout_cache, {
    tables = #{} :: map(),
    vtables = #{} :: map()
}).

%% Struct definition with precomputed layout
%% Fields are enriched maps: #{name => atom(), type => atom(), offset => non_neg_integer(), size => non_neg_integer()}
-record(struct_def, {
    fields :: [map()],
    total_size :: non_neg_integer()
}).

%% Vector definition with precomputed element info
-record(vector_def, {
    element_type :: term(),
    is_primitive :: boolean(),
    element_size :: pos_integer(),
    %% True if element_type is a table (for skipping Defs lookup at decode time)
    is_table_element = false :: boolean()
}).

%% Fixed-size array definition with precomputed info
-record(array_def, {
    element_type :: term(),
    count :: pos_integer(),
    element_size :: pos_integer(),
    total_size :: pos_integer(),
    %% True to return binary instead of list (for byte/ubyte arrays)
    as_binary = false :: boolean()
}).

%% Enum definition (stored in Defs map)
-record(enum_def, {
    base_type :: atom(),
    values :: [atom()],
    %% atom -> index (0-based)
    index_map :: #{atom() => non_neg_integer()},
    %% index -> atom (for fast decode)
    reverse_map :: #{non_neg_integer() => atom()}
}).

%% Resolved enum type (stored in field_def's resolved_type)
-record(enum_resolved, {
    base_type :: atom(),
    %% atom -> index (for encode)
    index_map :: #{atom() => non_neg_integer()},
    %% index -> atom (for decode)
    reverse_map :: #{non_neg_integer() => atom()}
}).

%% Union definition (stored in Defs map)
-record(union_def, {
    members :: [atom()],
    %% atom -> index (1-based)
    index_map :: #{atom() => pos_integer()},
    %% index -> atom (for fast decode)
    reverse_map :: #{pos_integer() => atom()}
}).

%% Union type field (stores the type index as uint8)
-record(union_type_def, {
    name :: atom(),
    index_map :: #{atom() => pos_integer()},
    %% Precomputed reverse map for fast decode (index -> atom)
    reverse_map :: #{pos_integer() => atom()}
}).

%% Union value field - partial (before field ID is known)
%% Created by resolve_type, converted to union_value_def by finalize_resolved_type
-record(union_value_partial, {
    name :: atom(),
    index_map :: #{atom() => pos_integer()},
    reverse_map :: #{pos_integer() => atom()}
}).

%% Union value field - complete (stores reference to the table)
-record(union_value_def, {
    name :: atom(),
    index_map :: #{atom() => pos_integer()},
    %% Precomputed reverse map for fast decode (index -> atom)
    reverse_map :: #{pos_integer() => atom()},
    %% Precomputed type field ID (value_field_id - 1)
    type_field_id :: non_neg_integer(),
    %% Precomputed type field name (e.g., foo_type for union field foo) for encode
    type_name :: atom(),
    type_binary_name :: binary()
}).
