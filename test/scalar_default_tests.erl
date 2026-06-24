%% @doc Tests that absent scalar fields return their type-natural default
%% per the flatbuffers spec.
%%
%% Background: flatbuffers wire-omits scalar fields when they hold the type's
%% default value (false for bool, 0 for int/uint, 0.0 for float/double). The
%% reader is responsible for substituting the default when a field is absent
%% on the wire — readers must not surface "absent" to callers for scalars,
%% because there's no way to distinguish "absent" from "explicitly the
%% default value" on the wire.
%%
%% Two flavours of default to check:
%%
%%   1. Explicit defaults declared in the schema: `hp: int = 100`. Schema
%%      parser captures these into field_def.default; existing tests cover.
%%
%%   2. Implicit type-natural defaults when the schema declares NO explicit
%%      default: `valid: bool` semantically defaults to `false` per
%%      flatbuffers spec. This is what this file exercises.
%%
%% The bug being verified-fixed: when a schema declared no explicit default
%% on a scalar, absent fields were surfacing as `undefined` to callers
%% instead of the type-natural default. That `undefined` then broke
%% pattern matches in downstream Erlang code (mearumtime sidecar
%% case_clause on `valid := undefined`).
-module(scalar_default_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Fixtures
%% =============================================================================

%% all_my_scalars.fbs declares every primitive scalar with NO explicit default.
%% A buffer where every field is absent on the wire should read back as the
%% type-natural default for each field.
empty_scalars_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/all_my_scalars.fbs"),
    %% from_map with an empty map: every field omitted on the wire
    Buffer = iolist_to_binary(flatbuferl:from_map(#{}, Schema)),
    flatbuferl:new(Buffer, Schema, #{root_type => scalars}).

%% defaults.fbs declares every scalar WITH an explicit default. A buffer with
%% all fields absent should read back as the SCHEMA-DECLARED default for each
%% field, not the type-natural default.
empty_explicit_defaults_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/defaults.fbs"),
    Buffer = iolist_to_binary(flatbuferl:from_map(#{}, Schema)),
    flatbuferl:new(Buffer, Schema, #{root_type => scalars}).

%% =============================================================================
%% Implicit (type-natural) defaults — each primitive scalar in turn.
%% Schema: every field declared with NO explicit default.
%% =============================================================================

implicit_default_bool_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(false, flatbuferl:get(Ctx, [my_bool])).

implicit_default_byte_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_byte])).

implicit_default_ubyte_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_ubyte])).

implicit_default_short_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_short])).

implicit_default_ushort_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_ushort])).

implicit_default_int_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_int])).

implicit_default_uint_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_uint])).

implicit_default_long_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_long])).

implicit_default_ulong_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl:get(Ctx, [my_ulong])).

implicit_default_float_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0.0, flatbuferl:get(Ctx, [my_float])).

implicit_default_double_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0.0, flatbuferl:get(Ctx, [my_double])).

%% fetch returns the same defaults (covers the lower-level API path too).
implicit_default_fetch_bool_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(false, flatbuferl_fetch:fetch(Ctx, [my_bool])).

implicit_default_fetch_int_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [my_int])).

implicit_default_fetch_float_test() ->
    Ctx = empty_scalars_ctx(),
    ?assertEqual(0.0, flatbuferl_fetch:fetch(Ctx, [my_float])).

%% =============================================================================
%% to_map should produce a complete map with every scalar at its default.
%% =============================================================================

implicit_default_to_map_test() ->
    Ctx = empty_scalars_ctx(),
    Map = flatbuferl:to_map(Ctx),
    ?assertEqual(false, maps:get(my_bool, Map)),
    ?assertEqual(0, maps:get(my_byte, Map)),
    ?assertEqual(0, maps:get(my_ubyte, Map)),
    ?assertEqual(0, maps:get(my_short, Map)),
    ?assertEqual(0, maps:get(my_ushort, Map)),
    ?assertEqual(0, maps:get(my_int, Map)),
    ?assertEqual(0, maps:get(my_uint, Map)),
    ?assertEqual(0, maps:get(my_long, Map)),
    ?assertEqual(0, maps:get(my_ulong, Map)),
    ?assertEqual(0.0, maps:get(my_float, Map)),
    ?assertEqual(0.0, maps:get(my_double, Map)).

%% =============================================================================
%% Explicit schema defaults still take precedence over natural defaults.
%% defaults.fbs has e.g. `my_bool:bool = true` — absent should read `true`.
%% =============================================================================

explicit_default_bool_test() ->
    Ctx = empty_explicit_defaults_ctx(),
    %% schema says my_bool:bool = true → absent reads as `true`, NOT `false`
    ?assertEqual(true, flatbuferl:get(Ctx, [my_bool])).

explicit_default_byte_test() ->
    Ctx = empty_explicit_defaults_ctx(),
    %% schema says my_byte:byte = -7
    ?assertEqual(-7, flatbuferl:get(Ctx, [my_byte])).

explicit_default_int_test() ->
    Ctx = empty_explicit_defaults_ctx(),
    %% schema says my_int:int = -7
    ?assertEqual(-7, flatbuferl:get(Ctx, [my_int])).

explicit_default_float_test() ->
    Ctx = empty_explicit_defaults_ctx(),
    %% schema says my_float:float = -7.7
    Value = flatbuferl:get(Ctx, [my_float]),
    ?assert(abs(Value - (-7.7)) < 0.0001).

%% =============================================================================
%% Set values still win — natural defaults only apply when truly absent.
%% =============================================================================

set_value_overrides_natural_default_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/all_my_scalars.fbs"),
    Buffer = iolist_to_binary(
        flatbuferl:from_map(#{my_bool => true, my_int => 42}, Schema)
    ),
    Ctx = flatbuferl:new(Buffer, Schema, #{root_type => scalars}),
    ?assertEqual(true, flatbuferl:get(Ctx, [my_bool])),
    ?assertEqual(42, flatbuferl:get(Ctx, [my_int])),
    %% Other fields still get their natural defaults
    ?assertEqual(0, flatbuferl:get(Ctx, [my_byte])),
    ?assertEqual(0.0, flatbuferl:get(Ctx, [my_float])).

%% =============================================================================
%% Reference types (string) still return undefined when absent — no implicit
%% default exists for strings/tables/vectors per the flatbuffers spec.
%% =============================================================================

absent_string_returns_undefined_test() ->
    %% Use a schema with both a scalar and a string, omit both.
    {ok, Schema} = flatbuferl:parse_schema_file(
        "test/schemas/table_bool_string_string.fbs"
    ),
    Buffer = iolist_to_binary(flatbuferl:from_map(#{}, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema, #{root_type => table_a}),
    %% bool gets its natural default (false)
    ?assertEqual(false, flatbuferl:get(Ctx, [my_bool])),
    %% Strings have no implicit default — fetch returns undefined,
    %% while get/2 raises missing_field (strict by design for non-scalar
    %% fields without a default).
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [my_string])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [my_second_string])).

%% =============================================================================
%% Cross-check against flatc.
%%
%% Build an empty buffer with our impl, hand it to flatc to decode as JSON,
%% and verify flatc's output agrees with what our reader returns. This proves
%% we're not just inventing defaults — they match the reference implementation.
%%
%% Skipped when flatc isn't on PATH.
%% =============================================================================

flatc_comparison_test_() ->
    case os:find_executable("flatc") of
        false ->
            %% Skip — flatc not available
            [{"skip flatc comparison (flatc not on PATH)", fun() -> ok end}];
        _ ->
            [
                {"implicit defaults: flatbuferl agrees with flatc",
                    fun flatc_implicit_defaults_agree/0},
                {"explicit defaults: flatbuferl agrees with flatc",
                    fun flatc_explicit_defaults_agree/0}
            ]
    end.

flatc_implicit_defaults_agree() ->
    %% Build an empty buffer with our impl
    SchemaPath = "test/schemas/all_my_scalars.fbs",
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),
    Buffer = iolist_to_binary(flatbuferl:from_map(#{}, Schema)),
    TmpBin = "/tmp/flatbuferl_implicit_defaults.bin",
    TmpJson = "/tmp/flatbuferl_implicit_defaults.json",
    ok = file:write_file(TmpBin, Buffer),

    %% Run flatc to decode → JSON
    Cmd = lists:flatten(
        io_lib:format(
            "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
            [SchemaPath, TmpBin]
        )
    ),
    Output = os:cmd(Cmd),
    ?assertEqual("", Output, {flatc_stderr, Output}),

    %% Read flatc's JSON and our impl's view side by side
    {ok, JsonBin} = file:read_file(TmpJson),
    FlatcDecoded = json:decode(JsonBin),
    Ctx = flatbuferl:new(Buffer, Schema, #{root_type => scalars}),

    %% Every scalar should agree. flatc omits fields at default from its JSON
    %% output OR emits them with the default value — both are "the field
    %% holds its default." Our impl always returns the default. So we
    %% normalise: missing-in-flatc-JSON ≡ default-from-our-impl.
    Fields = [
        {<<"my_bool">>, my_bool, false},
        {<<"my_byte">>, my_byte, 0},
        {<<"my_ubyte">>, my_ubyte, 0},
        {<<"my_short">>, my_short, 0},
        {<<"my_ushort">>, my_ushort, 0},
        {<<"my_int">>, my_int, 0},
        {<<"my_uint">>, my_uint, 0},
        {<<"my_long">>, my_long, 0},
        {<<"my_ulong">>, my_ulong, 0},
        {<<"my_float">>, my_float, 0.0},
        {<<"my_double">>, my_double, 0.0}
    ],
    lists:foreach(
        fun({JsonKey, AtomKey, ExpectedDefault}) ->
            OursVal = flatbuferl:get(Ctx, [AtomKey]),
            ?assertEqual(
                ExpectedDefault,
                OursVal,
                {our_impl_disagrees, AtomKey}
            ),
            %% flatc may either omit the field (because it's at default) OR
            %% include it with the default value — both are valid.
            case maps:find(JsonKey, FlatcDecoded) of
                {ok, FlatcVal} ->
                    %% flatc included it; should match the default
                    ?assertEqual(
                        ExpectedDefault,
                        FlatcVal,
                        {flatc_disagrees, JsonKey}
                    );
                error ->
                    %% flatc omitted it — that's "the field is at its default"
                    ok
            end
        end,
        Fields
    ),

    file:delete(TmpBin),
    file:delete(TmpJson).

flatc_explicit_defaults_agree() ->
    SchemaPath = "test/schemas/defaults.fbs",
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),
    Buffer = iolist_to_binary(flatbuferl:from_map(#{}, Schema)),
    TmpBin = "/tmp/flatbuferl_explicit_defaults.bin",
    TmpJson = "/tmp/flatbuferl_explicit_defaults.json",
    ok = file:write_file(TmpBin, Buffer),

    Cmd = lists:flatten(
        io_lib:format(
            "flatc --json --strict-json --raw-binary --defaults-json -o /tmp ~s -- ~s 2>&1",
            [SchemaPath, TmpBin]
        )
    ),
    Output = os:cmd(Cmd),
    ?assertEqual("", Output, {flatc_stderr, Output}),

    {ok, JsonBin} = file:read_file(TmpJson),
    FlatcDecoded = json:decode(JsonBin),
    Ctx = flatbuferl:new(Buffer, Schema, #{root_type => scalars}),

    %% Schema declared defaults — verify both impls return the schema's
    %% explicit defaults, not the type-natural ones.
    Fields = [
        {<<"my_byte">>, my_byte, -7},
        {<<"my_ubyte">>, my_ubyte, 7},
        {<<"my_bool">>, my_bool, true},
        {<<"my_short">>, my_short, -7},
        {<<"my_ushort">>, my_ushort, 7},
        {<<"my_int">>, my_int, -7},
        {<<"my_uint">>, my_uint, 7},
        {<<"my_long">>, my_long, -7},
        {<<"my_ulong">>, my_ulong, 7}
    ],
    lists:foreach(
        fun({JsonKey, AtomKey, ExpectedDefault}) ->
            OursVal = flatbuferl:get(Ctx, [AtomKey]),
            ?assertEqual(
                ExpectedDefault,
                OursVal,
                {our_impl_disagrees, AtomKey}
            ),
            case maps:find(JsonKey, FlatcDecoded) of
                {ok, FlatcVal} ->
                    ?assertEqual(
                        ExpectedDefault,
                        FlatcVal,
                        {flatc_disagrees, JsonKey}
                    );
                error ->
                    ok
            end
        end,
        Fields
    ),

    %% Floats — check with tolerance since JSON round-trip can introduce
    %% representation noise.
    OursFloat = flatbuferl:get(Ctx, [my_float]),
    ?assert(abs(OursFloat - (-7.7)) < 0.0001, {our_float_disagrees, OursFloat}),
    OursDouble = flatbuferl:get(Ctx, [my_double]),
    ?assert(abs(OursDouble - (-7.7)) < 0.0001, {our_double_disagrees, OursDouble}),

    file:delete(TmpBin),
    file:delete(TmpJson).
