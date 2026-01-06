-module(schema_suite_tests).
-include_lib("eunit/include/eunit.hrl").

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
            %% Green = 1
            #{enum_field => 1}},

        {vector_of_enums, "test/schemas/vector_of_enums.fbs", vector_table, no_file_id,
            %% Red, Green, Blue
            #{enum_fields => [0, 1, 2]}},

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
            f_color => 1
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
    {ok, {Defs, _Opts}} = schema:parse_file(SchemaPath),
    ?assert(is_map(Defs)),
    ?assert(maps:size(Defs) > 0).

test_encode_decode(SchemaPath, RootType, FileId, SampleData) ->
    {ok, {Defs, _}} = schema:parse_file(SchemaPath),

    %% Encode
    Buffer = iolist_to_binary(eflatbuffers:from_map(SampleData, Defs, RootType, FileId)),
    ?assert(is_binary(Buffer)),

    %% Decode
    Ctx = eflatbuffers:new(Buffer, Defs, RootType),
    Result = eflatbuffers:to_map(Ctx),

    %% Verify all fields match
    verify_maps_equal(SampleData, Result).

test_json_roundtrip(SchemaPath, RootType, FileId, SampleData) ->
    {ok, {Defs, _}} = schema:parse_file(SchemaPath),

    %% Encode to flatbuffer
    Buffer = iolist_to_binary(eflatbuffers:from_map(SampleData, Defs, RootType, FileId)),
    Ctx = eflatbuffers:new(Buffer, Defs, RootType),
    Map = eflatbuffers:to_map(Ctx),

    %% JSON roundtrip
    Json = iolist_to_binary(json:encode(Map)),
    Decoded = json:decode(Json),

    %% Re-encode from JSON-decoded map (binary keys)
    Buffer2 = iolist_to_binary(eflatbuffers:from_map(Decoded, Defs, RootType, FileId)),
    Ctx2 = eflatbuffers:new(Buffer2, Defs, RootType),
    Result = eflatbuffers:to_map(Ctx2),

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
    {ok, {Defs, _}} = schema:parse_file(SchemaPath),

    %% Build buffer
    Buffer = eflatbuffers:from_map(SampleData, Defs, RootType, FileId),
    TmpBin = "/tmp/eflatbuffers_test.bin",
    TmpJson = "/tmp/eflatbuffers_test.json",
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
    {ok, {Defs, _}} = schema:parse_file(SchemaPath),

    %% Build buffer
    Buffer = eflatbuffers:from_map(SampleData, Defs, RootType, FileId),
    TmpBin = "/tmp/eflatbuffers_test.bin",
    TmpJson = "/tmp/eflatbuffers_test.json",
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
    {ok, {Defs, _}} = schema:parse_file(SchemaPath),

    %% Encode sample data to JSON for flatc input
    JsonBin = iolist_to_binary(json:encode(SampleData)),

    TmpJson = "/tmp/eflatbuffers_input.json",
    TmpBin = "/tmp/eflatbuffers_input.bin",

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
    ErlBuffer = iolist_to_binary(eflatbuffers:from_map(SampleData, Defs, RootType, FileId)),

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
