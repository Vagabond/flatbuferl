-module(schema_suite_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flatbuferl_records.hrl").

%% Helper to create properly partitioned table definition
table(Fields) ->
    SortedFields = lists:sort(
        fun(#field_def{layout_key = A}, #field_def{layout_key = B}) -> A > B end,
        Fields
    ),
    {Scalars, Refs} = lists:partition(fun(#field_def{is_scalar = S}) -> S end, SortedFields),
    AllFields = Scalars ++ Refs,
    MaxId =
        case AllFields of
            [] -> -1;
            _ -> lists:max([F#field_def.id || F <- AllFields])
        end,
    FieldMap = maps:from_list([{F#field_def.name, F} || F <- AllFields]),
    EncodeLayout = flatbuferl_schema:precompute_encode_layout(Scalars, Refs, MaxId),
    #table_def{
        scalars = Scalars,
        refs = Refs,
        all_fields = AllFields,
        field_map = FieldMap,
        encode_layout = EncodeLayout,
        max_id = MaxId
    }.

%% Helper to create field records in new format
field(Name, Type) -> field(Name, Type, #{}).
field(Name, Type, Attrs) ->
    NormType = normalize_type(Type),
    Defs = maps:get(defs, Attrs, #{}),
    ResolvedType = resolve_type(NormType, Defs),
    Id = maps:get(id, Attrs, 0),
    InlineSize = maps:get(inline_size, Attrs, type_size(NormType)),
    #field_def{
        name = Name,
        id = Id,
        type = NormType,
        default = extract_default(Type),
        required = maps:get(required, Attrs, false),
        deprecated = maps:get(deprecated, Attrs, false),
        inline_size = InlineSize,
        is_scalar = is_scalar_type(ResolvedType, Defs),
        is_primitive = is_primitive_scalar(ResolvedType),
        resolved_type = ResolvedType,
        layout_key = InlineSize * 65536 + Id
    }.

is_primitive_scalar(bool) -> true;
is_primitive_scalar(int8) -> true;
is_primitive_scalar(uint8) -> true;
is_primitive_scalar(int16) -> true;
is_primitive_scalar(uint16) -> true;
is_primitive_scalar(int32) -> true;
is_primitive_scalar(uint32) -> true;
is_primitive_scalar(int64) -> true;
is_primitive_scalar(uint64) -> true;
is_primitive_scalar(float32) -> true;
is_primitive_scalar(float64) -> true;
is_primitive_scalar({enum, _, _}) -> true;
is_primitive_scalar(#union_type_def{}) -> true;
is_primitive_scalar(_) -> false.

%% Normalize scalar type aliases to canonical forms
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

is_scalar_type(string, _) ->
    false;
is_scalar_type({vector, _}, _) ->
    false;
is_scalar_type({union_value, _}, _) ->
    false;
is_scalar_type(#table_def{}, _) ->
    false;
is_scalar_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        % Table reference is not scalar
        #table_def{} -> false;
        _ -> true
    end;
is_scalar_type(_, _) ->
    true.

resolve_type(Type, Defs) when is_atom(Type) ->
    case maps:get(Type, Defs, undefined) of
        {struct, Fields} -> {struct, Fields};
        % Keep table types as atoms
        #table_def{} -> Type;
        #enum_def{base_type = Base, index_map = IndexMap, reverse_map = ReverseMap} ->
            #enum_resolved{base_type = Base, index_map = IndexMap, reverse_map = ReverseMap};
        _ -> normalize_scalar_type(Type)
    end;
resolve_type({vector, ElemType}, Defs) ->
    ResolvedElem = resolve_type(ElemType, Defs),
    ElemSize = type_size(ResolvedElem),
    #vector_def{
        element_type = ResolvedElem,
        is_primitive = is_primitive_scalar(ResolvedElem),
        element_size = ElemSize
    };
resolve_type({array, ElemType, Count}, Defs) ->
    {array, resolve_type(ElemType, Defs), Count};
resolve_type({union_type, UnionName}, Defs) ->
    case maps:get(UnionName, Defs, undefined) of
        #union_def{index_map = IndexMap, reverse_map = ReverseMap} ->
            #union_type_def{name = UnionName, index_map = IndexMap, reverse_map = ReverseMap};
        {union, Members} ->
            IndexMap = maps:from_list(lists:zip(Members, lists:seq(1, length(Members)))),
            ReverseMap = maps:fold(fun(K, V, M) -> M#{V => K} end, #{}, IndexMap),
            #union_type_def{name = UnionName, index_map = IndexMap, reverse_map = ReverseMap};
        _ ->
            #union_type_def{name = UnionName, index_map = #{}, reverse_map = #{}}
    end;
resolve_type({union_value, UnionName}, Defs) ->
    case maps:get(UnionName, Defs, undefined) of
        #union_def{index_map = IndexMap} ->
            #union_value_def{name = UnionName, index_map = IndexMap};
        {union, Members} ->
            IndexMap = maps:from_list(lists:zip(Members, lists:seq(1, length(Members)))),
            #union_value_def{name = UnionName, index_map = IndexMap};
        _ ->
            #union_value_def{name = UnionName, index_map = #{}}
    end;
resolve_type(Type, _Defs) ->
    Type.

normalize_type({T, _Default}) when
    is_atom(T),
    T /= vector,
    T /= enum,
    T /= struct,
    T /= array,
    T /= union_type,
    T /= union_value
->
    T;
normalize_type(T) ->
    T.

extract_default({_, Default}) when is_number(Default); is_boolean(Default) -> Default;
extract_default(bool) -> false;
extract_default(byte) -> 0;
extract_default(ubyte) -> 0;
extract_default(short) -> 0;
extract_default(ushort) -> 0;
extract_default(int) -> 0;
extract_default(uint) -> 0;
extract_default(long) -> 0;
extract_default(ulong) -> 0;
extract_default(float) -> 0.0;
extract_default(double) -> 0.0;
extract_default(_) -> undefined.

type_size(bool) -> 1;
type_size(byte) -> 1;
type_size(ubyte) -> 1;
type_size(short) -> 2;
type_size(ushort) -> 2;
type_size(int) -> 4;
type_size(uint) -> 4;
type_size(long) -> 8;
type_size(ulong) -> 8;
type_size(float) -> 4;
type_size(double) -> 8;
type_size(string) -> 4;
type_size({vector, _}) -> 4;
type_size({enum, _, _}) -> 1;
type_size({union_type, _}) -> 1;
type_size({union_value, _}) -> 4;
type_size({array, T, N}) -> type_size(T) * N;
type_size(_TableOrStruct) -> 4.

%% =============================================================================
%% Test Case Definitions
%% Each case: {Name, SchemaPath, RootType, FileId, SampleData}
%% =============================================================================

test_cases() ->
    [
        %% === test/vectors/ ===
        {monster, "test/vectors/test_monster.fbs", 'Monster', <<"MONS">>, #{
            name => <<"Goblin">>, hp => 50, mana => 25
        }},

        {nested, "test/vectors/test_nested.fbs", 'Entity', <<"NEST">>, #{
            name => <<"Player">>, hp => 100, pos => #{x => 1.0, y => 2.0, z => 3.0}
        }},

        {vector, "test/vectors/test_vector.fbs", 'Inventory', <<"VECT">>, #{
            counts => [1, 2, 3], items => [<<"sword">>, <<"shield">>]
        }},

        %% === test/schemas/ - basic tables ===
        {simple_table, "test/schemas/simple_table.fbs", table_a, no_file_id, #{
            field_a => 10, field_b => 20
        }},

        {simple_table_plus, "test/schemas/simple_table_plus.fbs", table_a, no_file_id, #{
            field_a => 1, field_b => 2, field_c => 3
        }},

        {nested_table, "test/schemas/nested.fbs", outer, no_file_id, #{
            value_outer => 100, inner => #{value_inner => 200}
        }},

        %% === test/schemas/ - scalars ===
        {all_scalars, "test/schemas/all_my_scalars.fbs", scalars, no_file_id, #{
            my_byte => -10,
            my_ubyte => 200,
            my_bool => true,
            my_short => -1000,
            my_ushort => 60000,
            my_int => -100000,
            my_uint => 100000,
            my_float => 3.14,
            my_long => -9000000000,
            my_ulong => 9000000000,
            my_double => 2.718281828
        }},

        {defaults, "test/schemas/defaults.fbs", scalars, no_file_id, #{my_int => 42}},

        %% === test/schemas/ - strings ===
        {string_table, "test/schemas/string_table.fbs", string_table, no_file_id, #{
            my_string => <<"hello world">>, my_bool => true
        }},

        {bool_string_string, "test/schemas/table_bool_string_string.fbs", table_a, no_file_id, #{
            my_bool => true, my_string => <<"first">>, my_second_string => <<"second">>
        }},

        %% === test/schemas/ - vectors ===
        {int_vector, "test/schemas/int_vector.fbs", int_vector_table, no_file_id, #{
            int_vector => [10, 20, 30, 40]
        }},

        {string_vector, "test/schemas/string_vector.fbs", string_vector_table, no_file_id, #{
            string_vector => [<<"a">>, <<"bb">>, <<"ccc">>]
        }},

        {table_vector, "test/schemas/table_vector.fbs", outer, no_file_id, #{
            inner => [#{value_inner => <<"one">>}, #{value_inner => <<"two">>}]
        }},

        %% === test/schemas/ - file identifiers ===
        {identifier, "test/schemas/identifier.fbs", dummy_table, <<"helo">>, #{}},

        {no_identifier, "test/schemas/no_identifier.fbs", dummy_table, no_file_id, #{}},

        %% === test/schemas/ - enums ===
        {enum_field, "test/schemas/enum_field.fbs", enum_outer, no_file_id,
            %% Green = 1, decoded as atom
            #{enum_field => 'Green'}},

        {vector_of_enums, "test/schemas/vector_of_enums.fbs", vector_table, no_file_id,
            %% Red, Green, Blue - decoded as atoms
            #{enum_fields => ['Red', 'Green', 'Blue']}},

        %% === test/schemas/ - nested tables with complex data ===
        {error_schema, "test/schemas/error.fbs", root_table, no_file_id, #{
            foo => true,
            tables_field => [
                #{bar => 1, string_field => <<"first">>},
                #{bar => 2, string_field => <<"second">>}
            ]
        }},

        %% === test/complex_schemas/ ===
        {config_path, "test/schemas/config_path.fbs", 'TechnologiesRoot', <<"BBBB">>, #{
            technologies => [
                #{category => <<"tech1">>},
                #{category => <<"tech2">>}
            ]
        }},

        %% === unions ===
        {union_hello, "test/schemas/union_field.fbs", command_root, <<"cmnd">>, #{
            data_type => hello,
            data => #{salute => <<"hi">>},
            additions_value => 99
        }},

        {union_bye, "test/schemas/union_field.fbs", command_root, <<"cmnd">>, #{
            data_type => bye, data => #{greeting => 42}
        }},

        %% === comprehensive type tests ===
        {all_types, "test/vectors/test_alltypes.fbs", 'AllTypes', <<"TYPE">>, #{
            f_bool => true,
            f_byte => -10,
            f_ubyte => 200,
            f_short => -1000,
            f_ushort => 50000,
            f_int => -100000,
            f_uint => 100000,
            f_long => -9000000000,
            f_ulong => 9000000000,
            f_float => 3.14,
            f_double => 2.718281828,
            f_string => <<"test string">>,
            f_color => 'Green'
        }},

        {vector_types, "test/vectors/test_vectors2.fbs", 'VectorTypes', <<"VEC2">>, #{
            bytes => [1, 2, 255],
            shorts => [-100, 0, 100],
            longs => [-9000000000, 9000000000],
            doubles => [1.1, 2.2, 3.3],
            bools => [true, false, true]
        }}
    ].

%% =============================================================================
%% Test Generator
%% =============================================================================

schema_suite_test_() ->
    {foreach, fun() -> ok end, fun(_) -> ok end, [generate_tests(Case) || Case <- test_cases()]}.

generate_tests({Name, SchemaPath, RootType, FileId, SampleData}) ->
    {atom_to_list(Name), [
        {atom_to_list(Name) ++ "_parse", fun() -> test_parse(SchemaPath) end},
        {atom_to_list(Name) ++ "_validate", fun() ->
            test_validate(SchemaPath, SampleData)
        end},
        {atom_to_list(Name) ++ "_encode_decode", fun() ->
            test_encode_decode(SchemaPath, RootType, FileId, SampleData)
        end},
        {atom_to_list(Name) ++ "_json_roundtrip", fun() ->
            test_json_roundtrip(SchemaPath, RootType, FileId, SampleData)
        end},
        {atom_to_list(Name) ++ "_flatc_roundtrip", fun() ->
            test_flatc_roundtrip(SchemaPath, RootType, FileId, SampleData)
        end},
        {atom_to_list(Name) ++ "_binary_match", fun() ->
            test_binary_match(SchemaPath, RootType, FileId, SampleData)
        end}
    ]}.

%% =============================================================================
%% Test Implementations
%% =============================================================================

test_parse(SchemaPath) ->
    {ok, {Defs, _Opts}} = flatbuferl:parse_schema_file(SchemaPath),
    ?assert(is_map(Defs)),
    ?assert(maps:size(Defs) > 0).

test_validate(SchemaPath, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),
    ?assertEqual(ok, flatbuferl:validate(SampleData, Schema)).

test_encode_decode(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Encode
    Buffer = iolist_to_binary(flatbuferl:from_map(SampleData, Schema)),
    ?assert(is_binary(Buffer)),

    %% Decode
    Ctx = flatbuferl:new(Buffer, Schema),
    Result = flatbuferl:to_map(Ctx),

    %% Verify all fields match
    verify_maps_equal(SampleData, Result).

test_json_roundtrip(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Encode to flatbuffer
    Buffer = iolist_to_binary(flatbuferl:from_map(SampleData, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    Map = flatbuferl:to_map(Ctx),

    %% JSON roundtrip
    Json = iolist_to_binary(json:encode(Map)),
    Decoded = json:decode(Json),

    %% Re-encode from JSON-decoded map (binary keys)
    Buffer2 = iolist_to_binary(flatbuferl:from_map(Decoded, Schema)),
    Ctx2 = flatbuferl:new(Buffer2, Schema),
    Result = flatbuferl:to_map(Ctx2),

    verify_maps_equal(SampleData, Result).

test_flatc_roundtrip(SchemaPath, RootType, FileId, SampleData) ->
    %% Use --raw-binary for schemas without file identifier
    case FileId of
        no_file_id ->
            test_flatc_roundtrip_raw(SchemaPath, RootType, FileId, SampleData);
        _ ->
            test_flatc_roundtrip_with_id(SchemaPath, RootType, FileId, SampleData)
    end.

test_flatc_roundtrip_with_id(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Build buffer
    Buffer = flatbuferl:from_map(SampleData, Schema),
    TmpBin = "/tmp/flatbuferl_test.bin",
    TmpJson = "/tmp/flatbuferl_test.json",
    ok = file:write_file(TmpBin, Buffer),

    %% Use flatc to decode
    Cmd = lists:flatten(
        io_lib:format(
            "flatc --json --strict-json -o /tmp ~s -- ~s 2>&1",
            [SchemaPath, TmpBin]
        )
    ),
    Result = os:cmd(Cmd),
    ?assertEqual("", Result),

    %% Verify flatc produced valid JSON
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assert(is_map(Decoded)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpJson).

test_flatc_roundtrip_raw(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Build buffer
    Buffer = flatbuferl:from_map(SampleData, Schema),
    TmpBin = "/tmp/flatbuferl_test.bin",
    TmpJson = "/tmp/flatbuferl_test.json",
    ok = file:write_file(TmpBin, Buffer),

    %% Use flatc with --raw-binary
    Cmd = lists:flatten(
        io_lib:format(
            "flatc --json --strict-json --raw-binary -o /tmp ~s -- ~s 2>&1",
            [SchemaPath, TmpBin]
        )
    ),
    Result = os:cmd(Cmd),
    ?assertEqual("", Result),

    %% Verify flatc produced valid JSON
    {ok, JsonBin} = file:read_file(TmpJson),
    Decoded = json:decode(JsonBin),
    ?assert(is_map(Decoded)),

    %% Cleanup
    file:delete(TmpBin),
    file:delete(TmpJson).

%% =============================================================================
%% Binary Match Test - Compare Erlang encoding with flatc encoding
%% =============================================================================

test_binary_match(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Encode sample data to JSON for flatc input
    JsonBin = iolist_to_binary(json:encode(SampleData)),

    TmpJson = "/tmp/flatbuferl_input.json",
    TmpBin = "/tmp/flatbuferl_input.bin",

    ok = file:write_file(TmpJson, JsonBin),

    %% Use flatc to encode JSON to binary
    Cmd = lists:flatten(
        io_lib:format(
            "flatc --binary -o /tmp ~s ~s 2>&1",
            [SchemaPath, TmpJson]
        )
    ),
    Result = os:cmd(Cmd),
    ?assertEqual("", Result, {flatc_error, Result}),

    %% Read flatc output
    {ok, FlatcBuffer} = file:read_file(TmpBin),

    %% Encode with our Erlang builder
    ErlBuffer = iolist_to_binary(flatbuferl:from_map(SampleData, Schema)),

    %% Compare binaries - require exact match
    ?assertEqual(
        FlatcBuffer,
        ErlBuffer,
        {binary_mismatch, #{
            schema => SchemaPath,
            flatc_size => byte_size(FlatcBuffer),
            erlang_size => byte_size(ErlBuffer)
        }}
    ),

    %% Cleanup
    file:delete(TmpJson),
    file:delete(TmpBin).

%% =============================================================================
%% Helpers
%% =============================================================================

verify_maps_equal(Expected, Actual) ->
    %% Verify all expected keys are present with correct values
    %% (Actual may have additional fields with default values)
    lists:foreach(
        fun(Key) ->
            ExpVal = maps:get(Key, Expected),
            ActVal = maps:get(Key, Actual),
            verify_values_equal(Key, ExpVal, ActVal)
        end,
        maps:keys(Expected)
    ).

verify_values_equal(_Key, Exp, Act) when is_map(Exp), is_map(Act) ->
    verify_maps_equal(Exp, Act);
verify_values_equal(Key, Exp, Act) when is_float(Exp); is_float(Act) ->
    ?assert(abs(Exp - Act) < 0.0001, {Key, expected, Exp, got, Act});
verify_values_equal(Key, Exp, Act) ->
    ?assertEqual(Exp, Act, {field, Key}).

%% =============================================================================
%% Zero-Copy Tests
%% Verify that decode/re-encode cycle doesn't create new refc binaries
%% =============================================================================

zero_copy_test_cases() ->
    %% Test cases with large strings (>64 bytes to be refc binaries)
    LargeString = list_to_binary(lists:duplicate(200, $X)),
    [
        {monster_zero_copy, "test/vectors/test_monster.fbs", 'Monster', <<"MONS">>, #{
            name => LargeString, hp => 100, mana => 50
        }},
        {string_table_zero_copy, "test/schemas/string_table.fbs", string_table, no_file_id, #{
            my_string => LargeString, my_bool => true
        }},
        {nested_zero_copy, "test/vectors/test_nested.fbs", 'Entity', <<"NEST">>, #{
            name => LargeString, hp => 100, pos => #{x => 1.0, y => 2.0, z => 3.0}
        }}
    ].

zero_copy_test_() ->
    [
        {atom_to_list(Name), fun() -> test_zero_copy(SchemaPath, RootType, FileId, Data) end}
     || {Name, SchemaPath, RootType, FileId, Data} <- zero_copy_test_cases()
    ].

test_zero_copy(SchemaPath, RootType, FileId, SampleData) ->
    {ok, Schema} = flatbuferl:parse_schema_file(SchemaPath),

    %% Encode initial buffer
    Buffer = iolist_to_binary(flatbuferl:from_map(SampleData, Schema)),

    %% Run decode/re-encode in isolated process to measure binaries
    Result = run_in_isolated_process(fun() ->
        erlang:garbage_collect(),
        Bins1 = get_refc_binary_ids(),

        %% Decode
        Ctx = flatbuferl:new(Buffer, Schema),
        DecodedMap = flatbuferl:to_map(Ctx),
        erlang:garbage_collect(),
        Bins2 = get_refc_binary_ids(),

        %% Re-encode - keep reference to iolist to prevent GC of sub-binaries
        ReEncoded = flatbuferl:from_map(DecodedMap, Schema),
        erlang:garbage_collect(),
        Bins3 = get_refc_binary_ids(),

        %% Use ReEncoded to ensure it's not optimized away
        _ = iolist_size(ReEncoded),

        {Bins1, Bins2, Bins3}
    end),

    {Bins1, Bins2, Bins3} = Result,

    %% Should have exactly 1 refc binary throughout (the original buffer)
    ?assertEqual(1, length(Bins1), "Should start with 1 refc binary"),
    ?assertEqual(Bins1, Bins2, "Decode should not create new refc binaries"),
    ?assertEqual(Bins1, Bins3, "Re-encode should not create new refc binaries").

get_refc_binary_ids() ->
    {binary, Bins} = erlang:process_info(self(), binary),
    lists:usort([Id || {Id, _, _} <- Bins]).

run_in_isolated_process(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    spawn_link(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Res} -> Res
    after 5000 -> error(timeout)
    end.

%% =============================================================================
%% Required and Deprecated Field Tests
%% =============================================================================

required_field_test_() ->
    Schema = {
        #{
            'TestTable' =>
                table([
                    field(name, string, #{required => true}),
                    field(value, int, #{id => 1})
                ])
        },
        #{root_type => 'TestTable'}
    },
    [
        {"required field present passes", fun() ->
            Map = #{name => <<"test">>, value => 42},
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(<<"test">>, maps:get(name, Result)),
            ?assertEqual(42, maps:get(value, Result))
        end},
        {"required field missing errors", fun() ->
            Map = #{value => 42},
            ?assertError(
                {required_field_missing, 'TestTable', name},
                flatbuferl_builder:from_map(Map, Schema)
            )
        end},
        {"required field empty string still valid", fun() ->
            Map = #{name => <<>>, value => 10},
            Data = flatbuferl_builder:from_map(Map, Schema),
            ?assert(is_list(Data) orelse is_binary(Data))
        end}
    ].

deprecated_encode_test_() ->
    Schema = {
        #{
            'TestTable' =>
                table([
                    field(name, string),
                    field(old_field, int, #{id => 1, deprecated => true}),
                    field(new_field, int, #{id => 2})
                ])
        },
        #{root_type => 'TestTable'}
    },
    [
        {"deprecated field skipped by default", fun() ->
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            %% Field wasn't encoded, so won't appear even with allow
            Result = flatbuferl:to_map(Ctx, #{deprecated => allow}),
            ?assertEqual(false, maps:is_key(old_field, Result)),
            ?assertEqual(10, maps:get(new_field, Result))
        end},
        {"deprecated field allowed with option", fun() ->
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema, #{deprecated => allow}),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx, #{deprecated => allow}),
            ?assertEqual(42, maps:get(old_field, Result))
        end},
        {"deprecated field errors with option", fun() ->
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            ?assertError(
                {deprecated_field_set, 'TestTable', old_field},
                flatbuferl_builder:from_map(Map, Schema, #{deprecated => error})
            )
        end},
        {"deprecated field not set passes error option", fun() ->
            Map = #{name => <<"test">>, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema, #{deprecated => error}),
            ?assert(is_list(Data) orelse is_binary(Data))
        end}
    ].

deprecated_decode_test_() ->
    Schema = {
        #{
            'TestTable' =>
                table([
                    field(name, string),
                    field(old_field, int, #{id => 1, deprecated => true}),
                    field(new_field, int, #{id => 2})
                ])
        },
        #{root_type => 'TestTable'}
    },
    [
        {"deprecated field skipped by default", fun() ->
            %% First encode with deprecated field (allow it)
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema, #{deprecated => allow}),
            Bin = iolist_to_binary(Data),
            %% Now decode with default options - should skip
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(false, maps:is_key(old_field, Result)),
            ?assertEqual(10, maps:get(new_field, Result))
        end},
        {"deprecated field allowed with option", fun() ->
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema, #{deprecated => allow}),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx, #{deprecated => allow}),
            ?assertEqual(42, maps:get(old_field, Result)),
            ?assertEqual(10, maps:get(new_field, Result))
        end},
        {"deprecated field errors on decode if present", fun() ->
            Map = #{name => <<"test">>, old_field => 42, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema, #{deprecated => allow}),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            ?assertError(
                {deprecated_field_present, 'TestTable', old_field},
                flatbuferl:to_map(Ctx, #{deprecated => error})
            )
        end},
        {"deprecated field not present passes error option", fun() ->
            Map = #{name => <<"test">>, new_field => 10},
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx, #{deprecated => error}),
            ?assertEqual(10, maps:get(new_field, Result))
        end}
    ].

%% =============================================================================
%% Union Vector Tests
%% =============================================================================

union_vector_test_() ->
    %% Use raw tuples to ensure proper schema processing of union types
    Schema = flatbuferl_schema:process(
        {
            #{
                'Event' => {union, ['Login', 'Logout']},
                'Login' => {table, [{user, string}]},
                'Logout' => {table, [{user, string}]},
                'EventLog' => {table, [
                    {events_type, {vector, {union_type, 'Event'}}},
                    {events, {vector, {union_value, 'Event'}}, #{id => 1}}
                ]}
            },
            #{root_type => 'EventLog'}
        }
    ),
    [
        {"union vector encode/decode roundtrip", fun() ->
            Map = #{
                events_type => ['Login', 'Logout', 'Login'],
                events => [
                    #{user => <<"alice">>},
                    #{user => <<"bob">>},
                    #{user => <<"charlie">>}
                ]
            },
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(['Login', 'Logout', 'Login'], maps:get(events_type, Result)),
            Events = maps:get(events, Result),
            ?assertEqual(3, length(Events)),
            ?assertEqual(#{user => <<"alice">>}, lists:nth(1, Events)),
            ?assertEqual(#{user => <<"bob">>}, lists:nth(2, Events)),
            ?assertEqual(#{user => <<"charlie">>}, lists:nth(3, Events))
        end},
        {"union vector with binary type names", fun() ->
            Map = #{
                events_type => [<<"Login">>, <<"Logout">>],
                events => [#{user => <<"x">>}, #{user => <<"y">>}]
            },
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(['Login', 'Logout'], maps:get(events_type, Result))
        end},
        {"empty union vector", fun() ->
            Map = #{events_type => [], events => []},
            Data = flatbuferl_builder:from_map(Map, Schema),
            Bin = iolist_to_binary(Data),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual([], maps:get(events_type, Result)),
            ?assertEqual([], maps:get(events, Result))
        end},
        {"union vector from parsed .fbs file", fun() ->
            {ok, ParsedSchema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
            Map = #{
                data_type => ['StringData', 'IntData'],
                data => [
                    #{data => [<<"hello">>, <<"world">>]},
                    #{data => [1, 2, 3]}
                ]
            },
            Encoded = flatbuferl_builder:from_map(Map, ParsedSchema),
            Bin = iolist_to_binary(Encoded),
            ?assertEqual(<<"UVEC">>, flatbuferl:file_id(Bin)),
            Ctx = flatbuferl:new(Bin, ParsedSchema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(['StringData', 'IntData'], maps:get(data_type, Result)),
            DataVals = maps:get(data, Result),
            ?assertEqual(2, length(DataVals)),
            ?assertEqual([<<"hello">>, <<"world">>], maps:get(data, lists:nth(1, DataVals))),
            ?assertEqual([1, 2, 3], maps:get(data, lists:nth(2, DataVals)))
        end}
    ].

%% =============================================================================
%% Optional Scalar Tests
%% =============================================================================

optional_scalar_test_() ->
    Schema = {
        #{
            'TestTable' =>
                table([
                    field(opt_int, {int, undefined}),
                    field(opt_bool, {bool, undefined}, #{id => 1}),
                    field(reg_int, {int, 0}, #{id => 2}),
                    field(reg_int_default, {int, 100}, #{id => 3})
                ])
        },
        #{root_type => 'TestTable'}
    },
    [
        {"optional scalar set to value", fun() ->
            Map = #{opt_int => 42, reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(42, maps:get(opt_int, Result)),
            ?assertEqual(10, maps:get(reg_int, Result))
        end},
        {"optional scalar set to zero is distinguishable from not set", fun() ->
            Map = #{opt_int => 0, reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(0, maps:get(opt_int, Result)),
            ?assert(maps:is_key(opt_int, Result))
        end},
        {"optional scalar not set - absent from result", fun() ->
            Map = #{reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(false, maps:is_key(opt_int, Result)),
            ?assertEqual(10, maps:get(reg_int, Result))
        end},
        {"optional scalar explicit undefined - absent from result", fun() ->
            Map = #{opt_int => undefined, reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(false, maps:is_key(opt_int, Result))
        end},
        {"optional bool set to false is written", fun() ->
            Map = #{opt_bool => false, reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(false, maps:get(opt_bool, Result)),
            ?assert(maps:is_key(opt_bool, Result))
        end},
        {"regular scalar with default - returns default when missing", fun() ->
            Map = #{reg_int => 10},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(100, maps:get(reg_int_default, Result))
        end},
        {"validation allows undefined for optional scalar", fun() ->
            Map = #{opt_int => undefined, reg_int => 10},
            ?assertEqual(ok, flatbuferl:validate(Map, Schema))
        end},
        {"validation allows omitted optional scalar", fun() ->
            Map = #{reg_int => 10},
            ?assertEqual(ok, flatbuferl:validate(Map, Schema))
        end}
    ].

%% =============================================================================
%% Include Directive Tests
%% =============================================================================

include_test_() ->
    [
        {"basic include works", fun() ->
            {ok, {Defs, Opts}} = flatbuferl:parse_schema_file("test/schemas/with_include.fbs"),
            ?assert(maps:is_key('Entity', Defs)),
            ?assert(maps:is_key('Vec3', Defs)),
            ?assert(maps:is_key('Color', Defs)),
            ?assertEqual('Entity', maps:get(root_type, Opts))
        end},
        {"include encode/decode roundtrip", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/with_include.fbs"),
            Map = #{name => <<"Test">>, pos => #{x => 1.0, y => 2.0, z => 3.0}, color => 1},
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            ?assertEqual(<<"Test">>, maps:get(name, Result)),
            Pos = maps:get(pos, Result),
            ?assert(abs(maps:get(x, Pos) - 1.0) < 0.001)
        end},
        {"circular include detected", fun() ->
            Result = flatbuferl:parse_schema_file("test/schemas/circular_a.fbs"),
            ?assertMatch({error, {circular_include, _}}, Result)
        end},
        {"duplicate type detected", fun() ->
            Result = flatbuferl:parse_schema_file("test/schemas/duplicate.fbs"),
            ?assertMatch({error, {duplicate_types, ['Vec3']}}, Result)
        end},
        {"missing include file", fun() ->
            ok = file:write_file(
                "/tmp/missing_include.fbs",
                <<"include \"nonexistent.fbs\";\ntable T { x: int; }\nroot_type T;">>
            ),
            Result = flatbuferl:parse_schema_file("/tmp/missing_include.fbs"),
            ?assertMatch({error, enoent}, Result),
            file:delete("/tmp/missing_include.fbs")
        end}
    ].

%% =============================================================================
%% Fixed-Size Array Tests
%% =============================================================================

fixed_array_test_() ->
    [
        {"array schema parses", fun() ->
            {ok, {Defs, _Opts}} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            ?assert(maps:is_key('ArrayTable', Defs)),
            #table_def{scalars = Scalars} = maps:get('ArrayTable', Defs),
            %% Fields are pre-sorted by layout_key (size desc, id desc)
            [
                #field_def{name = ints, type = {array, int, 4}},
                #field_def{name = floats, type = {array, float, 3}},
                #field_def{name = bytes, type = {array, byte, 2}}
            ] = Scalars
        end},
        {"array encode/decode roundtrip", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            Map = #{
                floats => [1.0, 2.0, 3.0],
                ints => [10, 20, 30, 40],
                bytes => [-1, 127]
            },
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            [F1, F2, F3] = maps:get(floats, Result),
            ?assert(abs(F1 - 1.0) < 0.001),
            ?assert(abs(F2 - 2.0) < 0.001),
            ?assert(abs(F3 - 3.0) < 0.001),
            ?assertEqual([10, 20, 30, 40], maps:get(ints, Result)),
            ?assertEqual([-1, 127], maps:get(bytes, Result))
        end},
        {"array validation - correct length", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            Map = #{floats => [1.0, 2.0, 3.0], ints => [1, 2, 3, 4], bytes => [0, 0]},
            ?assertEqual(ok, flatbuferl:validate(Map, Schema))
        end},
        {"array validation - wrong length", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            Map = #{floats => [1.0, 2.0], ints => [1, 2, 3, 4], bytes => [0, 0]},
            {error, Errors} = flatbuferl:validate(Map, Schema),
            ?assertMatch([{array_length_mismatch, floats, 3, 2}], Errors)
        end},
        {"array validation - wrong element type", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            Map = #{
                floats => [<<"not">>, <<"floats">>, <<"here">>],
                ints => [1, 2, 3, 4],
                bytes => [0, 0]
            },
            {error, Errors} = flatbuferl:validate(Map, Schema),
            %% All invalid elements are reported
            ?assert(length(Errors) >= 1),
            [{invalid_array_element, floats, 0, _} | _] = Errors
        end},
        {"array encoding error on wrong length", fun() ->
            {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
            Map = #{floats => [1.0, 2.0], ints => [1, 2, 3, 4], bytes => [0, 0]},
            ?assertError(
                {array_length_mismatch, expected, 3, got, 2},
                flatbuferl:from_map(Map, Schema)
            )
        end},
        {"inline schema array roundtrip", fun() ->
            Schema = {
                #{
                    'Test' =>
                        table([
                            field(vec3, {array, float, 3}),
                            field(matrix, {array, int, 9}, #{id => 1})
                        ])
                },
                #{root_type => 'Test'}
            },
            Map = #{
                vec3 => [1.5, 2.5, 3.5],
                matrix => [1, 0, 0, 0, 1, 0, 0, 0, 1]
            },
            Bin = iolist_to_binary(flatbuferl:from_map(Map, Schema)),
            Ctx = flatbuferl:new(Bin, Schema),
            Result = flatbuferl:to_map(Ctx),
            [V1, V2, V3] = maps:get(vec3, Result),
            ?assert(abs(V1 - 1.5) < 0.001),
            ?assert(abs(V2 - 2.5) < 0.001),
            ?assert(abs(V3 - 3.5) < 0.001),
            ?assertEqual([1, 0, 0, 0, 1, 0, 0, 0, 1], maps:get(matrix, Result))
        end}
    ].
