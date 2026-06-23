%% @doc Enum decode/encode symmetry: reads return the schema's atom
%% name (via #enum_resolved.reverse_map) and writes accept atoms,
%% binaries, or raw ordinals (legacy). Catches the class of bug where
%% a stale `#{kind := 6}' pattern silently breaks after the schema
%% grows a new variant and ordinals shift.

-module(enum_atom_tests).

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Fixture
%% =============================================================================

%% Returns {Schema, BinaryWithRed, BinaryWithBlue}. `Color` has 4
%% values so we can also exercise an unknown ordinal by hand-crafting
%% a buffer that references an out-of-schema value.
schema_and_buffers() ->
    {ok, Schema} = flatbuferl:parse_schema(
        "enum Color : ubyte { Red, Green, Blue, Violet }\n"
        "table Paint { color: Color; tint: int; }\n"
        "root_type Paint;\n"
    ),
    Red = iolist_to_binary(
        flatbuferl:from_map(#{color => 'Red', tint => 1}, Schema)
    ),
    Blue = iolist_to_binary(
        flatbuferl:from_map(#{color => 'Blue', tint => 2}, Schema)
    ),
    {Schema, Red, Blue}.

%% =============================================================================
%% Decode
%% =============================================================================

%% to_map/1 surfaces the atom name for enum fields.
to_map_returns_atom_test() ->
    {Schema, Red, _} = schema_and_buffers(),
    Ctx = flatbuferl:new(Red, Schema),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual('Red', maps:get(color, Map)),
    ?assertEqual(1, maps:get(tint, Map)).

%% flatbuferl_fetch:fetch/2 also surfaces the atom name — the path
%% mearumtime uses.
fetch_returns_atom_test() ->
    {Schema, _, Blue} = schema_and_buffers(),
    Ctx = flatbuferl:new(Blue, Schema),
    ?assertEqual('Blue', flatbuferl_fetch:fetch(Ctx, [color])),
    ?assertEqual(2, flatbuferl_fetch:fetch(Ctx, [tint])).

%% An ordinal not in the reverse_map (e.g. a peer running ahead of
%% our schema with a newly added variant) passes through as the raw
%% integer rather than crashing. We hand-craft the buffer by encoding
%% a known value and patching the color slot — its byte offset is
%% addressable through the schema's vtable.
unknown_ordinal_passes_through_test() ->
    {Schema, Red, _} = schema_and_buffers(),
    Ctx0 = flatbuferl:new(Red, Schema),
    %% The color slot is one ubyte; locate it via the field offset
    %% and overwrite with an out-of-schema ordinal (99).
    Patched = overwrite_color_byte(Red, Ctx0, 99),
    Ctx = flatbuferl:new(Patched, Schema),
    ?assertEqual(99, flatbuferl_fetch:fetch(Ctx, [color])),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual(99, maps:get(color, Map)).

overwrite_color_byte(Buf, Ctx, NewVal) ->
    %% The `color' field is field_id 0 on the Paint table; resolve its
    %% absolute byte offset and splice in NewVal.
    Root = flatbuferl_reader:get_root(Buf),
    {table, TableOffset, _} = Root,
    <<_:TableOffset/binary, VTableSOffset:32/little-signed, _/binary>> = Buf,
    VTableOffset = TableOffset - VTableSOffset,
    %% color is the first field (slot offset 4 from vtable start = field_id 0 * 2 + 4)
    <<_:VTableOffset/binary, _Size:16/little-unsigned, _TableSize:16/little-unsigned,
        FieldOffset:16/little-unsigned, _/binary>> = Buf,
    Pos = TableOffset + FieldOffset,
    <<Prefix:Pos/binary, _:8, Rest/binary>> = Buf,
    _ = Ctx,
    <<Prefix/binary, NewVal:8, Rest/binary>>.

%% =============================================================================
%% Encode
%% =============================================================================

%% from_map/2 accepts the atom form and produces a buffer that
%% roundtrips back to the same atom.
encode_atom_roundtrips_test() ->
    {Schema, _, _} = schema_and_buffers(),
    Buf = iolist_to_binary(
        flatbuferl:from_map(#{color => 'Violet', tint => 7}, Schema)
    ),
    Ctx = flatbuferl:new(Buf, Schema),
    ?assertEqual('Violet', flatbuferl_fetch:fetch(Ctx, [color])),
    ?assertEqual(7, flatbuferl_fetch:fetch(Ctx, [tint])).

%% from_map/2 still accepts a raw integer ordinal for back-compat with
%% callers that haven't migrated to atoms yet — and the decode side
%% normalises both inputs to the same atom on read.
encode_integer_decodes_as_atom_test() ->
    {Schema, _, _} = schema_and_buffers(),
    Buf = iolist_to_binary(
        flatbuferl:from_map(#{color => 2, tint => 9}, Schema)
    ),
    Ctx = flatbuferl:new(Buf, Schema),
    ?assertEqual('Blue', flatbuferl_fetch:fetch(Ctx, [color])).

%% from_map/2 also accepts the binary-name form (the JSON-bridge
%% path) and decodes back to the matching atom.
encode_binary_decodes_as_atom_test() ->
    {Schema, _, _} = schema_and_buffers(),
    Buf = iolist_to_binary(
        flatbuferl:from_map(#{color => <<"Green">>, tint => 11}, Schema)
    ),
    Ctx = flatbuferl:new(Buf, Schema),
    ?assertEqual('Green', flatbuferl_fetch:fetch(Ctx, [color])).
