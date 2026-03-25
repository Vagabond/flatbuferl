-module(alignment_verification_tests).
-include_lib("eunit/include/eunit.hrl").

%% Suppress warnings for helper functions kept for future test expansion
-compile(
    {nowarn_unused_function, [
        verify_vector_alignment/2,
        character_id_fields/0,
        time_interval_fields/0,
        building_fields/0
    ]}
).

%% =============================================================================
%% Alignment Verification Tests for flatbuferl
%% =============================================================================
%%
%% These tests verify that flatbuferl produces FlatBuffer binaries conforming
%% to the FlatBuffer alignment specification. The Rust strict verifier rejects
%% buffers with misaligned fields, causing production failures.
%%
%% FlatBuffer alignment requirements (from the spec):
%%   - Scalars aligned to their natural size: u16->2, u32/i32->4, u64/i64/f64->8
%%   - Table soffset (vtable back-reference) must be 4-byte aligned
%%   - Vector length prefix must be 4-byte aligned
%%   - Nested table offsets (uoffsets) must be 4-byte aligned
%%   - Vtable entries are 2-byte aligned (always true since vtable is u16 array)
%%
%% EXPECTED: These tests FAIL on the current codebase, exposing the alignment
%% bug. They serve as a regression suite for when the fix is applied.
%% =============================================================================

%% =============================================================================
%% Schema paths
%% =============================================================================

protocol_schema_path() ->
    "test/complex_schemas/protocol.fbs".

game_state_schema_path() ->
    "test/complex_schemas/game_state.fbs".

%% =============================================================================
%% Type size / alignment helpers
%% =============================================================================

%% Returns the natural alignment for a FlatBuffer scalar type.
type_alignment(bool) -> 1;
type_alignment(byte) -> 1;
type_alignment(ubyte) -> 1;
type_alignment(int8) -> 1;
type_alignment(uint8) -> 1;
type_alignment(short) -> 2;
type_alignment(ushort) -> 2;
type_alignment(int16) -> 2;
type_alignment(uint16) -> 2;
type_alignment(int) -> 4;
type_alignment(uint) -> 4;
type_alignment(int32) -> 4;
type_alignment(uint32) -> 4;
type_alignment(float) -> 4;
type_alignment(float32) -> 4;
type_alignment(long) -> 8;
type_alignment(ulong) -> 8;
type_alignment(int64) -> 8;
type_alignment(uint64) -> 8;
type_alignment(double) -> 8;
type_alignment(float64) -> 8;
% uoffset
type_alignment(string) -> 4;
% table refs, vectors are uoffsets (4 bytes)
type_alignment(_) -> 4.

%% type_size/1 kept for reference; may be used by future alignment checks.
%% -compile({nowarn_unused_function, type_size/1}).
%% type_size(Type) -> type_alignment(Type).

%% =============================================================================
%% FlatBuffer binary walker
%% =============================================================================

%% Read a little-endian u32 at byte position Pos
read_u32(Buffer, Pos) ->
    <<_:Pos/binary, V:32/little-unsigned, _/binary>> = Buffer,
    V.

%% Read a little-endian i32 at byte position Pos
read_i32(Buffer, Pos) ->
    <<_:Pos/binary, V:32/little-signed, _/binary>> = Buffer,
    V.

%% Read a little-endian u16 at byte position Pos
read_u16(Buffer, Pos) ->
    <<_:Pos/binary, V:16/little-unsigned, _/binary>> = Buffer,
    V.

%% Verify alignment of a single table given its absolute position and schema info.
%% Returns a list of {FieldName, AbsolutePos, RequiredAlign, ActualMod} violations.
verify_table_alignment(Buffer, TablePos, FieldDefs) ->
    %% The table itself must be 4-byte aligned (soffset is i32)
    TableViolation =
        case TablePos rem 4 of
            0 -> [];
            R -> [{table_start, TablePos, 4, R}]
        end,

    %% Read soffset to find vtable
    SOffset = read_i32(Buffer, TablePos),
    VTablePos = TablePos - SOffset,

    %% Read vtable metadata
    VTableSize = read_u16(Buffer, VTablePos),
    _TableDataSize = read_u16(Buffer, VTablePos + 2),

    %% Number of field slots in vtable
    NumSlots = (VTableSize - 4) div 2,

    %% Walk each field slot
    FieldViolations = lists:foldl(
        fun({FieldIndex, FieldName, FieldType}, Acc) ->
            case FieldIndex < NumSlots of
                false ->
                    Acc;
                true ->
                    SlotPos = VTablePos + 4 + FieldIndex * 2,
                    FieldOffset = read_u16(Buffer, SlotPos),
                    case FieldOffset of
                        % field not present
                        0 ->
                            Acc;
                        _ ->
                            AbsFieldPos = TablePos + FieldOffset,
                            RequiredAlign = type_alignment(FieldType),
                            case AbsFieldPos rem RequiredAlign of
                                0 -> Acc;
                                Mod -> [{FieldName, AbsFieldPos, RequiredAlign, Mod} | Acc]
                            end
                    end
            end
        end,
        [],
        FieldDefs
    ),
    TableViolation ++ lists:reverse(FieldViolations).

%% Verify vector alignment: the vector length prefix must be 4-byte aligned
verify_vector_alignment(Buffer, VectorOffsetPos) ->
    UOffset = read_u32(Buffer, VectorOffsetPos),
    VectorPos = VectorOffsetPos + UOffset,
    case VectorPos rem 4 of
        0 -> [];
        R -> [{vector_length, VectorPos, 4, R}]
    end.

%% Follow a uoffset at position Pos, return the absolute target position
follow_uoffset(Buffer, Pos) ->
    UOffset = read_u32(Buffer, Pos),
    Pos + UOffset.

%% =============================================================================
%% Protocol schema field definitions (manually extracted from protocol.fbs)
%% =============================================================================

%% Packet fields (indices match FlatBuffer field order in schema):
%%   0: version (uint16)
%%   1: request_id (uint64)
%%   2: trace_id (string)
%%   3: timestamp_ms (uint64)
%%   4: payload_type (union type byte - implicit)
%%   5: payload (union value - uoffset)
packet_fields() ->
    [
        {0, version, uint16},
        {1, request_id, uint64},
        {2, trace_id, string},
        {3, timestamp_ms, uint64}
    ].
%% Union type/value handled separately

%% BattleRoundRequest fields:
%%   0: player_id (table ref -> uoffset)
%%   1: round (uint64)
%%   2: randomness (vector -> uoffset)
%%   3: timestamp_ms (uint64)
%%   4: actions (vector of tables -> uoffset)
%%   5: checkpoint (bool)
%%   6: effects (table ref -> uoffset)
battle_round_request_fields() ->
    % uoffset to PlayerId table
    [
        {0, player_id, uint32},
        {1, round, uint64},
        % uoffset to vector
        {2, randomness, uint32},
        {3, timestamp_ms, uint64},
        % uoffset to vector
        {4, actions, uint32},
        {5, checkpoint, bool},
        % uoffset to table
        {6, effects, uint32}
    ].

%% SpawnRequest fields:
%%   0: player_id (table ref)
%%   1: randomness (vector)
%%   2: party_key (vector)
%%   3: members (vector of strings)
%%   4: slot_index (uint32)
spawn_request_fields() ->
    [
        {0, player_id, uint32},
        {1, randomness, uint32},
        {2, party_key, uint32},
        {3, members, uint32},
        {4, slot_index, uint32}
    ].

%% CraftRequest fields:
%%   0: character_id (table ref)
%%   1: material_type (union type byte)
%%   2: material (union value -> uoffset)
%%   3: init_seed (vector)
%%   4: crafter (string)
%%   5: crafter_signature (vector)
craft_request_fields() ->
    [
        {0, character_id, uint32},
        {1, material_type, ubyte},
        {2, material, uint32},
        {3, init_seed, uint32},
        {4, crafter, string},
        {5, crafter_signature, uint32}
    ].

%% PlayerId fields:
%%   0: id (string -> uoffset)
player_id_fields() ->
    [{0, id, string}].

%% Action fields:
%%   0: id (vector -> uoffset)
%%   1: sender (string -> uoffset)
%%   2: data (vector -> uoffset)
%%   3: tick (uint64)
%%   4: signature (vector -> uoffset)
action_fields() ->
    [
        {0, id, uint32},
        {1, sender, string},
        {2, data, uint32},
        {3, tick, uint64},
        {4, signature, uint32}
    ].

%% CharacterId fields:
%%   0: player_id (string)
%%   1: character_id (string)
character_id_fields() ->
    [
        {0, player_id, string},
        {1, character_id, string}
    ].

%% =============================================================================
%% Game state schema field definitions
%% =============================================================================

%% GameStateRoot fields:
%%   0: version (int = int32)
%%   1: gameData1 (table ref)
game_state_root_fields() ->
    [
        {0, version, int32},
        {1, 'gameData1', uint32}
    ].

%% GameData fields (selected - the ones with long/i64):
%%   6: nextPirateAttack (long)
%%   13: merchantVisits ([long])  - vector
%%   20: lastPvpAttackTime (long)
game_data_fields() ->
    [
        {0, workers, uint32},
        {1, trophies, int32},
        {2, 'academyTechnologies', uint32},
        {3, 'arsenalTechnologies', uint32},
        {4, ships, uint32},
        {5, 'currentQuests', uint32},
        {6, 'completedQuests', uint32},
        {7, 'playerResources', uint32},
        {8, 'nextPirateAttack', int64},
        {9, 'leftOverResources', uint32},
        {10, islands, uint32},
        {11, 'fortressLevel', byte},
        {12, defences, uint32},
        {13, buildings, uint32},
        {14, 'islandSectors', uint32},
        {15, 'townSectors', uint32},
        {16, 'merchantVisits', uint32},
        {17, 'merchantShips', uint32},
        {18, 'playerName', string},
        {19, 'reservedResources', uint32},
        {20, 'battleLogsAttack', uint32},
        {21, 'battleLogsDefence', uint32},
        {22, 'lastPvpAttackTime', int64},
        {23, 'raidPlayers', uint32}
    ].

%% Workers fields:
%%   0: productionStartTime (long)
%%   1: assignedWorkers (int)
%%   2: productionEndTime (long)
workers_fields() ->
    [
        {0, 'productionStartTime', int64},
        {1, 'assignedWorkers', int32},
        {2, 'productionEndTime', int64}
    ].

%% TimeInterval fields:
%%   0: start (long)
%%   1: end_field (long)
time_interval_fields() ->
    [
        {0, start, int64},
        {1, 'end', int64}
    ].

%% Building fields (selected for i64):
%%   8: currentCraftingStartTime (long)
%%   11: pause (long)
building_fields() ->
    [
        {0, type, string},
        {1, level, byte},
        {2, position, uint32},
        %% 3 = paused (deprecated)
        %% 4 = production (deprecated)
        {5, storage, uint32},
        {6, upgrading, bool},
        {7, 'startEndTime', uint32},
        {8, construction, string},
        %% 9 = currentCraftingItem (deprecated)
        {10, 'currentCraftingStartTime', int64},
        {11, 'craftingQueue', uint32},
        {12, research, uint32},
        {13, pause, int64},
        {14, producing, bool},
        {15, 'userPaused', bool},
        {16, 'defenceUpgrading', bool}
    ].

%% =============================================================================
%% Test: Packet wrapping BattleRoundRequest
%% =============================================================================
%% Tests the core message pattern: root envelope with union dispatch wrapping
%% a table that has nested tables, u64 fields, and vectors of tables.
%% The strict verifier rejects misaligned u64 fields.

packet_battle_round_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 42,
            timestamp_ms => 1710000000000,
            trace_id => <<"trace-abc-123">>,
            payload_type => 'BattleRoundRequest',
            payload => #{
                player_id => #{id => <<"player-alpha">>},
                round => 100,
                randomness => crypto:strong_rand_bytes(32),
                timestamp_ms => 1710000000000,
                actions => [
                    #{
                        id => <<"act-001">>,
                        sender => <<"player-1234">>,
                        data => <<"attack">>,
                        tick => 1
                    },
                    #{
                        id => <<"act-002">>,
                        sender => <<"player-5678">>,
                        data => <<"defend">>,
                        tick => 2
                    }
                ],
                checkpoint => true
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Verify roundtrip works (Erlang is tolerant of misalignment)
        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('BattleRoundRequest', maps:get(payload_type, Decoded)),

        %% Now check actual byte-level alignment
        Violations = verify_packet_envelope(Buffer, battle_round_request_fields()),

        io:format("~n=== BattleRoundRequest Packet Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        case Violations of
            [] ->
                io:format("All fields properly aligned.~n");
            _ ->
                io:format("ALIGNMENT VIOLATIONS (~p):~n", [length(Violations)]),
                lists:foreach(
                    fun({Name, Pos, Req, Mod}) ->
                        io:format(
                            "  ~p at byte ~p: requires ~p-byte alignment, position mod ~p = ~p~n",
                            [Name, Pos, Req, Req, Mod]
                        )
                    end,
                    Violations
                )
        end,
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "BattleRoundRequest has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Packet wrapping SpawnRequest
%% =============================================================================

packet_spawn_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 1,
            timestamp_ms => 1710000000000,
            payload_type => 'SpawnRequest',
            payload => #{
                player_id => #{id => <<"player-one">>},
                randomness => crypto:strong_rand_bytes(32),
                party_key => crypto:strong_rand_bytes(48),
                members => [<<"ally-1">>, <<"ally-2">>, <<"ally-3">>],
                slot_index => 0
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('SpawnRequest', maps:get(payload_type, Decoded)),

        Violations = verify_packet_envelope(Buffer, spawn_request_fields()),

        io:format("~n=== SpawnRequest Packet Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "SpawnRequest has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Packet wrapping CraftRequest (with InlineMaterial)
%% =============================================================================

packet_craft_inline_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 7,
            timestamp_ms => 1710000000000,
            payload_type => 'CraftRequest',
            payload => #{
                character_id => #{
                    player_id => <<"player-1">>,
                    character_id => <<"char-1">>
                },
                material_type => 'InlineMaterial',
                material => #{
                    bytes => crypto:strong_rand_bytes(64)
                },
                init_seed => crypto:strong_rand_bytes(32),
                crafter => <<"admin-crafter">>
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('CraftRequest', maps:get(payload_type, Decoded)),

        Violations = verify_packet_envelope(Buffer, craft_request_fields()),

        io:format("~n=== CraftRequest Packet Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "CraftRequest has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: CraftRequest with RecipeRef + Checksum (struct array sizing)
%% =============================================================================
%%
%% Regression test for primitive_type_size/1 returning 4 for {array, uint8, 32}.
%% RecipeRef contains Checksum (struct with bytes:[uint8:32]). Before the fix,
%% the struct was sized as 4 bytes instead of 32, corrupting all subsequent
%% offsets and causing the strict verifier to panic with garbage table offsets.

packet_craft_recipe_ref_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        HashBytes = lists:seq(1, 32),
        Map = #{
            version => 1,
            request_id => 42,
            timestamp_ms => 1710000000000,
            payload_type => 'CraftRequest',
            payload => #{
                character_id => #{
                    player_id => <<"player-1">>,
                    character_id => <<"char-1">>
                },
                material_type => 'RecipeRef',
                material => #{
                    name => <<"iron-sword">>,
                    version => <<"1.0.0">>,
                    content_hash => #{bytes => HashBytes}
                },
                crafter => <<"master-smith">>
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Roundtrip decode must recover all fields including Checksum bytes
        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('CraftRequest', maps:get(payload_type, Decoded)),
        Msg = maps:get(payload, Decoded),
        ?assertEqual('RecipeRef', maps:get(material_type, Msg)),
        Mat = maps:get(material, Msg),
        ?assertEqual(<<"iron-sword">>, maps:get(name, Mat)),
        ?assertEqual(<<"1.0.0">>, maps:get(version, Mat)),
        Hash = maps:get(content_hash, Mat),
        ?assertEqual(HashBytes, maps:get(bytes, Hash)),

        %% Verify alignment of the outer packet
        Violations = verify_packet_envelope(Buffer, craft_request_fields()),
        io:format("~n=== CraftRequest+RecipeRef Packet Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "CraftRequest+RecipeRef has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Game state nested alignment
%% =============================================================================

game_state_nested_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(game_state_schema_path()),
        Map = #{
            version => 1,
            'gameData1' => #{
                workers => #{
                    'productionStartTime' => 1710000000000,
                    'assignedWorkers' => 5,
                    'productionEndTime' => 1710003600000
                },
                trophies => 1500,
                'academyTechnologies' => [
                    #{type => <<"shields">>, level => 3},
                    #{type => <<"weapons">>, level => 2}
                ],
                'arsenalTechnologies' => [
                    #{type => <<"cannons">>, level => 4}
                ],
                ships => [
                    #{
                        type => <<"frigate">>,
                        status => 1.0,
                        order => 1,
                        level => 5,
                        cargo => [#{type => <<"gold">>, amount => 100}],
                        'armyCargo' => [],
                        upgrading => false,
                        repairing => false,
                        'startEndTime' => #{start => 0, 'end' => 0}
                    }
                ],
                'currentQuests' => [
                    #{
                        name => <<"build_farm">>,
                        'taskAmountToComplete' => 1,
                        'taskAmountDone' => 0,
                        done => false
                    }
                ],
                'completedQuests' => [<<"tutorial">>],
                'playerResources' => [
                    #{type => <<"gold">>, amount => 5000},
                    #{type => <<"wood">>, amount => 3000}
                ],
                'nextPirateAttack' => 1710086400000,
                'leftOverResources' => [],
                islands => [],
                'fortressLevel' => 3,
                defences => [],
                buildings => [
                    #{
                        type => <<"farm">>,
                        level => 2,
                        position => #{x => 5, y => 10},
                        storage => [#{type => <<"food">>, amount => 200}],
                        upgrading => false,
                        'startEndTime' => #{start => 0, 'end' => 0},
                        construction => <<"">>,
                        'currentCraftingStartTime' => 1710000000000,
                        'craftingQueue' => [],
                        pause => 0,
                        producing => true,
                        'userPaused' => false,
                        'defenceUpgrading' => false
                    }
                ],
                'islandSectors' => [],
                'townSectors' => [],
                'merchantVisits' => [1710000000000, 1710043200000],
                'merchantShips' => [],
                'playerName' => <<"TestPlayer">>,
                'reservedResources' => [],
                'battleLogsAttack' => [],
                'battleLogsDefence' => [],
                'lastPvpAttackTime' => 1709913600000,
                'raidPlayers' => []
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Roundtrip verification
        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual(1, maps:get(version, Decoded)),

        %% Walk the buffer and check alignment of all reachable tables
        AllViolations = verify_game_state_buffer(Buffer),

        io:format("~n=== GameState Nested Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "GameState has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Scalar alignment with u64/i64 fields
%% =============================================================================
%% This test focuses purely on scalar alignment of 8-byte fields.

scalar_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table Inner {\n"
                "                big_val: uint64;\n"
                "                small_val: uint16;\n"
                "                another_big: int64;\n"
                "            }\n"
                "            table ScalarTest {\n"
                "                a_byte: ubyte;\n"
                "                a_u64: uint64;\n"
                "                a_string: string;\n"
                "                a_i64: int64;\n"
                "                inner: Inner;\n"
                "                a_u32: uint32;\n"
                "                another_u64: uint64;\n"
                "            }\n"
                "            root_type ScalarTest;\n"
                "        "
            >>
        ),
        Map = #{
            a_byte => 42,
            % max u64
            a_u64 => 18446744073709551615,
            a_string => <<"hello">>,
            % min i64
            a_i64 => -9223372036854775808,
            inner => #{
                big_val => 123456789012345,
                small_val => 65535,
                another_big => -1
            },
            a_u32 => 4294967295,
            another_u64 => 999999999999
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Verify roundtrip
        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual(42, maps:get(a_byte, Decoded)),
        ?assertEqual(18446744073709551615, maps:get(a_u64, Decoded)),

        %% Check root table scalar alignment
        RootFields = [
            {0, a_byte, ubyte},
            {1, a_u64, uint64},
            {2, a_string, string},
            {3, a_i64, int64},
            % uoffset
            {4, inner, uint32},
            {5, a_u32, uint32},
            {6, another_u64, uint64}
        ],

        InnerFields = [
            {0, big_val, uint64},
            {1, small_val, uint16},
            {2, another_big, int64}
        ],

        RootPos = read_u32(Buffer, 0),
        RootViolations = verify_table_alignment(Buffer, RootPos, RootFields),

        %% Find and check the inner table
        SOffset = read_i32(Buffer, RootPos),
        VTablePos = RootPos - SOffset,
        VTableSize = read_u16(Buffer, VTablePos),
        % field index 4
        InnerSlotPos = VTablePos + 4 + 4 * 2,
        InnerViolations =
            case InnerSlotPos + 2 =< VTablePos + VTableSize of
                true ->
                    InnerFieldOffset = read_u16(Buffer, InnerSlotPos),
                    case InnerFieldOffset of
                        0 ->
                            [];
                        _ ->
                            InnerRefPos = RootPos + InnerFieldOffset,
                            InnerTablePos = follow_uoffset(Buffer, InnerRefPos),
                            InnerTableViol = verify_table_alignment(
                                Buffer, InnerTablePos, InnerFields
                            ),
                            %% Also check the inner table uoffset itself is 4-aligned
                            UOffsetViol =
                                case InnerRefPos rem 4 of
                                    0 -> [];
                                    R -> [{inner_uoffset, InnerRefPos, 4, R}]
                                end,
                            UOffsetViol ++ InnerTableViol
                    end;
                false ->
                    []
            end,

        AllViolations = RootViolations ++ InnerViolations,

        io:format("~n=== Scalar Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "ScalarTest has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Multiple u64 fields in sequence (stress test for padding)
%% =============================================================================

multi_u64_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table MultiU64 {\n"
                "                flag: bool;\n"
                "                val1: uint64;\n"
                "                val2: uint64;\n"
                "                val3: uint64;\n"
                "                small: uint16;\n"
                "                val4: uint64;\n"
                "            }\n"
                "            root_type MultiU64;\n"
                "        "
            >>
        ),
        Map = #{
            flag => true,
            val1 => 1,
            val2 => 2,
            val3 => 3,
            small => 99,
            val4 => 4
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Fields = [
            {0, flag, bool},
            {1, val1, uint64},
            {2, val2, uint64},
            {3, val3, uint64},
            {4, small, uint16},
            {5, val4, uint64}
        ],

        RootPos = read_u32(Buffer, 0),
        Violations = verify_table_alignment(Buffer, RootPos, Fields),

        io:format("~n=== Multi-u64 Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "MultiU64 has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Vector of u64 (long) — vector data must be 8-byte aligned
%% =============================================================================

vector_u64_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table VecTest {\n"
                "                label: string;\n"
                "                timestamps: [uint64];\n"
                "                count: uint32;\n"
                "            }\n"
                "            root_type VecTest;\n"
                "        "
            >>
        ),
        Map = #{
            label => <<"test">>,
            timestamps => [1710000000000, 1710000001000, 1710000002000],
            count => 42
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Find the timestamps vector
        RootPos = read_u32(Buffer, 0),
        SOffset = read_i32(Buffer, RootPos),
        VTablePos = RootPos - SOffset,

        %% timestamps is field index 1
        TimestampsSlotPos = VTablePos + 4 + 1 * 2,
        TimestampsFieldOffset = read_u16(Buffer, TimestampsSlotPos),

        Violations =
            case TimestampsFieldOffset of
                0 ->
                    [{timestamps_missing, 0, 0, 0}];
                _ ->
                    TimestampsRefPos = RootPos + TimestampsFieldOffset,
                    VecPos = follow_uoffset(Buffer, TimestampsRefPos),
                    %% Vector length prefix must be 4-byte aligned
                    LenViol =
                        case VecPos rem 4 of
                            0 -> [];
                            R1 -> [{timestamps_vec_length, VecPos, 4, R1}]
                        end,
                    %% Vector data (u64 elements) starts at VecPos + 4
                    %% Each u64 element should be 8-byte aligned
                    DataStart = VecPos + 4,
                    DataViol =
                        case DataStart rem 8 of
                            0 -> [];
                            R2 -> [{timestamps_vec_data, DataStart, 8, R2}]
                        end,
                    LenViol ++ DataViol
            end,

        io:format("~n=== Vector u64 Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "VecTest has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Deeply nested tables (3+ levels deep)
%% =============================================================================

deep_nesting_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table Level3 {\n"
                "                value: uint64;\n"
                "                tag: string;\n"
                "            }\n"
                "            table Level2 {\n"
                "                child: Level3;\n"
                "                counter: uint64;\n"
                "                name: string;\n"
                "            }\n"
                "            table Level1 {\n"
                "                inner: Level2;\n"
                "                timestamp: uint64;\n"
                "                flag: bool;\n"
                "            }\n"
                "            table Root {\n"
                "                level1: Level1;\n"
                "                id: uint64;\n"
                "            }\n"
                "            root_type Root;\n"
                "        "
            >>
        ),
        Map = #{
            level1 => #{
                inner => #{
                    child => #{
                        value => 9999999999999,
                        tag => <<"deep">>
                    },
                    counter => 42,
                    name => <<"mid">>
                },
                timestamp => 1710000000000,
                flag => true
            },
            id => 12345678901234
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Walk all 4 nesting levels and collect violations
        AllViolations = verify_nested_tables(Buffer, [
            {root, [{0, level1, uint32}, {1, id, uint64}]},
            {level1, [{0, inner, uint32}, {1, timestamp, uint64}, {2, flag, bool}]},
            {level2, [{0, child, uint32}, {1, counter, uint64}, {2, name, string}]},
            {level3, [{0, value, uint64}, {1, tag, string}]}
        ]),

        io:format("~n=== Deep Nesting Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "DeepNesting has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: BattleRoundRequest direct root (no union wrapping) with specific
%% byte position checks for nested tables and vectors of tables.
%% =============================================================================

battle_round_direct_known_positions_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table PlayerId { id: string (required); }\n"
                "            table Action {\n"
                "                id: [ubyte] (required);\n"
                "                sender: string (required);\n"
                "                data: [ubyte] (required);\n"
                "                tick: uint64 = 0;\n"
                "                signature: [ubyte];\n"
                "            }\n"
                "            table BattleRoundRequest {\n"
                "                player_id: PlayerId (required);\n"
                "                round: uint64;\n"
                "                randomness: [ubyte] (required);\n"
                "                timestamp_ms: uint64;\n"
                "                actions: [Action] (required);\n"
                "                checkpoint: bool = true;\n"
                "            }\n"
                "            root_type BattleRoundRequest;\n"
                "        "
            >>
        ),
        Map = #{
            player_id => #{id => <<"test_player">>},
            round => 1,
            randomness => <<0:256>>,
            timestamp_ms => 1000,
            actions => [
                #{id => <<"act1">>, sender => <<"test">>, data => <<"hello">>, tick => 0}
            ],
            checkpoint => true
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Check all fields in BattleRoundRequest
        RootPos = read_u32(Buffer, 0),
        BRRViolations = verify_table_alignment(
            Buffer,
            RootPos,
            battle_round_request_fields()
        ),

        %% Also check the PlayerId table that BRR points to
        SOffset = read_i32(Buffer, RootPos),
        VTablePos = RootPos - SOffset,
        % field 0
        PlayerIdSlotPos = VTablePos + 4,
        PlayerIdFieldOffset = read_u16(Buffer, PlayerIdSlotPos),
        PlayerIdViolations =
            case PlayerIdFieldOffset of
                0 ->
                    [{player_id_missing, 0, 0, 0}];
                _ ->
                    PlayerIdRefPos = RootPos + PlayerIdFieldOffset,
                    PlayerIdTablePos = follow_uoffset(Buffer, PlayerIdRefPos),
                    %% Check uoffset alignment
                    UOffViol =
                        case PlayerIdRefPos rem 4 of
                            0 -> [];
                            R -> [{player_id_uoffset, PlayerIdRefPos, 4, R}]
                        end,
                    %% Check table alignment
                    TableViol = verify_table_alignment(
                        Buffer,
                        PlayerIdTablePos,
                        player_id_fields()
                    ),
                    UOffViol ++ TableViol
            end,

        %% Check Action tables

        % field 4 = actions
        ActSlotPos = VTablePos + 4 + 4 * 2,
        ActFieldOffset = read_u16(Buffer, ActSlotPos),
        ActViolations =
            case ActFieldOffset of
                0 ->
                    [];
                _ ->
                    ActRefPos = RootPos + ActFieldOffset,
                    ActVecPos = follow_uoffset(Buffer, ActRefPos),
                    NumActs = read_u32(Buffer, ActVecPos),
                    lists:foldl(
                        fun(I, Acc) ->
                            ActOffsetPos = ActVecPos + 4 + I * 4,
                            ActTablePos = follow_uoffset(Buffer, ActOffsetPos),
                            ActViol = verify_table_alignment(
                                Buffer,
                                ActTablePos,
                                action_fields()
                            ),
                            TaggedViol = [
                                {
                                    list_to_atom(
                                        "act_" ++ integer_to_list(I) ++ "_" ++
                                            atom_to_list(N)
                                    ),
                                    P,
                                    A,
                                    M
                                }
                             || {N, P, A, M} <- ActViol
                            ],
                            Acc ++ TaggedViol
                        end,
                        [],
                        lists:seq(0, NumActs - 1)
                    )
            end,

        AllViolations = BRRViolations ++ PlayerIdViolations ++ ActViolations,

        io:format("~n=== BattleRoundRequest Direct (Known Positions) ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "BattleRoundRequest direct has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Packet with small inner message (tests union padding interaction)
%% =============================================================================

packet_quit_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 99,
            timestamp_ms => 1710000000000,
            payload_type => 'QuitRequest',
            payload => #{
                player_id => #{id => <<"player-quitting">>},
                timeout_ms => 10000
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('QuitRequest', maps:get(payload_type, Decoded)),

        %% Check Packet fields
        RootPos = read_u32(Buffer, 0),
        PacketViolations = verify_table_alignment(Buffer, RootPos, packet_fields()),

        %% Check Packet table position itself
        RootPosViol =
            case RootPos rem 4 of
                0 -> [];
                R -> [{root_table_pos, RootPos, 4, R}]
            end,

        AllViolations = RootPosViol ++ PacketViolations,

        io:format("~n=== Quit Packet Alignment ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "Quit packet has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Helper: Verify packet envelope + inner message
%% =============================================================================

verify_packet_envelope(Buffer, InnerFields) ->
    RootPos = read_u32(Buffer, 0),

    %% Check root (Packet) table alignment
    PacketViolations = verify_table_alignment(Buffer, RootPos, packet_fields()),

    %% Navigate to the union payload (field index 4 = type, field index 5 = value)
    SOffset = read_i32(Buffer, RootPos),
    VTablePos = RootPos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    %% Union value is at vtable slot 5 (0-indexed from field 0)
    %% Packet: version(0), request_id(1), trace_id(2), timestamp_ms(3),
    %%         payload_type(4), payload(5)
    MsgSlotPos = VTablePos + 4 + 5 * 2,
    InnerViolations =
        case MsgSlotPos + 2 =< VTablePos + VTableSize of
            false ->
                [];
            true ->
                MsgFieldOffset = read_u16(Buffer, MsgSlotPos),
                case MsgFieldOffset of
                    0 ->
                        [];
                    _ ->
                        MsgRefPos = RootPos + MsgFieldOffset,
                        InnerTablePos = follow_uoffset(Buffer, MsgRefPos),

                        %% Check uoffset alignment
                        UOffViol =
                            case MsgRefPos rem 4 of
                                0 -> [];
                                R -> [{payload_uoffset, MsgRefPos, 4, R}]
                            end,

                        %% Check inner table alignment
                        InnerViol = verify_table_alignment(Buffer, InnerTablePos, InnerFields),

                        %% Also check nested tables (player_id, character_id)
                        NestedViol = verify_inner_nested_tables(
                            Buffer, InnerTablePos, InnerFields
                        ),

                        UOffViol ++ InnerViol ++ NestedViol
                end
        end,

    PacketViolations ++ InnerViolations.

%% Check nested tables inside the inner message
verify_inner_nested_tables(Buffer, InnerTablePos, InnerFields) ->
    SOffset = read_i32(Buffer, InnerTablePos),
    VTablePos = InnerTablePos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    lists:foldl(
        fun({FieldIdx, FieldName, FieldType}, Acc) ->
            %% Only check uoffset fields that point to tables
            case FieldType of
                uint32 when
                    FieldName =:= player_id;
                    FieldName =:= character_id
                ->
                    SlotPos = VTablePos + 4 + FieldIdx * 2,
                    case SlotPos + 2 =< VTablePos + VTableSize of
                        false ->
                            Acc;
                        true ->
                            FieldOffset = read_u16(Buffer, SlotPos),
                            case FieldOffset of
                                0 ->
                                    Acc;
                                _ ->
                                    RefPos = InnerTablePos + FieldOffset,
                                    TargetPos = follow_uoffset(Buffer, RefPos),
                                    Violations =
                                        case TargetPos rem 4 of
                                            0 ->
                                                [];
                                            R ->
                                                [
                                                    {
                                                        list_to_atom(
                                                            atom_to_list(FieldName) ++ "_table"
                                                        ),
                                                        TargetPos,
                                                        4,
                                                        R
                                                    }
                                                ]
                                        end,
                                    Acc ++ Violations
                            end
                    end;
                _ ->
                    Acc
            end
        end,
        [],
        InnerFields
    ).

%% =============================================================================
%% Helper: Verify game state buffer (walk known structure)
%% =============================================================================

verify_game_state_buffer(Buffer) ->
    RootPos = read_u32(Buffer, 0),

    %% Check GameStateRoot
    RootViol = verify_table_alignment(Buffer, RootPos, game_state_root_fields()),

    %% Navigate to GameData (field 1)
    SOffset = read_i32(Buffer, RootPos),
    VTablePos = RootPos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    % field 1
    GameDataSlotPos = VTablePos + 4 + 1 * 2,
    GameDataViol =
        case GameDataSlotPos + 2 =< VTablePos + VTableSize of
            false ->
                [];
            true ->
                GameDataFieldOffset = read_u16(Buffer, GameDataSlotPos),
                case GameDataFieldOffset of
                    0 ->
                        [];
                    _ ->
                        GameDataRefPos = RootPos + GameDataFieldOffset,
                        GameDataTablePos = follow_uoffset(Buffer, GameDataRefPos),

                        %% Dump GameData field positions for diagnosis
                        dump_table_fields(
                            Buffer,
                            GameDataTablePos,
                            game_data_fields(),
                            "GameData"
                        ),

                        GDViol = verify_table_alignment(
                            Buffer,
                            GameDataTablePos,
                            game_data_fields()
                        ),

                        %% Check Workers table (field 0 of GameData)
                        WorkersViol = verify_nested_field(
                            Buffer,
                            GameDataTablePos,
                            0,
                            workers_fields(),
                            workers
                        ),

                        GDViol ++ WorkersViol
                end
        end,

    RootViol ++ GameDataViol.

%% Verify a nested table field given parent position and field index
verify_nested_field(Buffer, ParentPos, FieldIdx, FieldDefs, Label) ->
    SOffset = read_i32(Buffer, ParentPos),
    VTablePos = ParentPos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    SlotPos = VTablePos + 4 + FieldIdx * 2,
    case SlotPos + 2 =< VTablePos + VTableSize of
        false ->
            [];
        true ->
            FieldOffset = read_u16(Buffer, SlotPos),
            case FieldOffset of
                0 ->
                    [];
                _ ->
                    RefPos = ParentPos + FieldOffset,
                    TablePos = follow_uoffset(Buffer, RefPos),
                    Violations = verify_table_alignment(Buffer, TablePos, FieldDefs),
                    %% Tag violations with the nesting label
                    [
                        {list_to_atom(atom_to_list(Label) ++ "." ++ atom_to_list(N)), P, A, M}
                     || {N, P, A, M} <- Violations
                    ]
            end
    end.

%% =============================================================================
%% Helper: Walk nested tables by following uoffset chain
%% =============================================================================

verify_nested_tables(Buffer, [{_RootLabel, RootFields} | RestLevels]) ->
    RootPos = read_u32(Buffer, 0),
    RootViol = verify_table_alignment(Buffer, RootPos, RootFields),
    verify_nested_tables_level(Buffer, RootPos, RootFields, RestLevels, RootViol).

verify_nested_tables_level(_Buffer, _ParentPos, _ParentFields, [], Acc) ->
    Acc;
verify_nested_tables_level(Buffer, ParentPos, ParentFields, [{Label, Fields} | Rest], Acc) ->
    %% Find the first table-ref field (uoffset) in parent to follow
    TableRefField = lists:foldl(
        fun
            ({Idx, _Name, uint32}, none) -> Idx;
            (_, Found) -> Found
        end,
        none,
        ParentFields
    ),

    case TableRefField of
        none ->
            Acc;
        FieldIdx ->
            SOffset = read_i32(Buffer, ParentPos),
            VTablePos = ParentPos - SOffset,
            VTableSize = read_u16(Buffer, VTablePos),

            SlotPos = VTablePos + 4 + FieldIdx * 2,
            case SlotPos + 2 =< VTablePos + VTableSize of
                false ->
                    Acc;
                true ->
                    FieldOffset = read_u16(Buffer, SlotPos),
                    case FieldOffset of
                        0 ->
                            Acc;
                        _ ->
                            RefPos = ParentPos + FieldOffset,
                            ChildPos = follow_uoffset(Buffer, RefPos),
                            ChildViol = verify_table_alignment(Buffer, ChildPos, Fields),
                            Tagged = [
                                {
                                    list_to_atom(
                                        atom_to_list(Label) ++ "." ++
                                            atom_to_list(N)
                                    ),
                                    P,
                                    A,
                                    M
                                }
                             || {N, P, A, M} <- ChildViol
                            ],
                            verify_nested_tables_level(
                                Buffer, ChildPos, Fields, Rest, Acc ++ Tagged
                            )
                    end
            end
    end.

%% =============================================================================
%% Test: Many refs then i64 (mimics GameData pattern that triggers the bug)
%% =============================================================================
%% The game_state_nested_alignment_test exposes the bug because GameData has
%% ~15 uoffset (4-byte) ref fields followed by i64 fields. If the builder
%% doesn't pad to 8-byte boundaries before placing i64 fields, they land at
%% 4-mod-8 positions.

many_refs_then_i64_alignment_test_() ->
    {timeout, 30, fun() ->
        %% This schema mimics GameData exactly: many table/vector ref fields
        %% interspersed with i64 fields, all with the exact same field count
        %% and type pattern that triggers the bug in the real game_state schema.
        %%
        %% GameData has 24 fields:
        %%   - workers (table ref), trophies (int), 6x vector refs,
        %%     nextPirateAttack (long), 6x more vector refs, byte,
        %%     4x more vector refs, long (merchantVisits is [long]),
        %%     2x more vector refs, string, 3x more vector refs,
        %%     lastPvpAttackTime (long), 1x vector ref
        %%
        %% Reproduced here with the same mix of types.
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table Item { name: string; value: int; }\n"
                "            table BigTable {\n"
                "                workers: Item;\n"
                "                trophies: int;\n"
                "                academy_techs: [Item];\n"
                "                arsenal_techs: [Item];\n"
                "                ships: [Item];\n"
                "                current_quests: [Item];\n"
                "                completed_quests: [string];\n"
                "                player_resources: [Item];\n"
                "                next_pirate_attack: long;\n"
                "                left_over_resources: [Item];\n"
                "                islands: [Item];\n"
                "                fortress_level: byte;\n"
                "                defences: [Item];\n"
                "                buildings: [Item];\n"
                "                island_sectors: [Item];\n"
                "                town_sectors: [Item];\n"
                "                merchant_visits: [long];\n"
                "                merchant_ships: [Item];\n"
                "                player_name: string;\n"
                "                reserved_resources: [Item];\n"
                "                battle_logs_attack: [Item];\n"
                "                battle_logs_defence: [Item];\n"
                "                last_pvp_attack_time: long;\n"
                "                raid_players: [Item];\n"
                "            }\n"
                "            root_type BigTable;\n"
                "        "
            >>
        ),
        Map = #{
            workers => #{name => <<"w">>, value => 5},
            trophies => 1500,
            academy_techs => [#{name => <<"shields">>, value => 3}],
            arsenal_techs => [#{name => <<"cannons">>, value => 4}],
            ships => [#{name => <<"frigate">>, value => 5}],
            current_quests => [#{name => <<"quest">>, value => 1}],
            completed_quests => [<<"tutorial">>],
            player_resources => [#{name => <<"gold">>, value => 5000}],
            next_pirate_attack => 1710086400000,
            left_over_resources => [],
            islands => [],
            fortress_level => 3,
            defences => [],
            buildings => [#{name => <<"farm">>, value => 2}],
            island_sectors => [],
            town_sectors => [],
            merchant_visits => [1710000000000, 1710043200000],
            merchant_ships => [],
            player_name => <<"TestPlayer">>,
            reserved_resources => [],
            battle_logs_attack => [],
            battle_logs_defence => [],
            last_pvp_attack_time => 1709913600000,
            raid_players => []
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Fields = [
            {0, workers, uint32},
            {1, trophies, int32},
            {2, academy_techs, uint32},
            {3, arsenal_techs, uint32},
            {4, ships, uint32},
            {5, current_quests, uint32},
            {6, completed_quests, uint32},
            {7, player_resources, uint32},
            {8, next_pirate_attack, int64},
            {9, left_over_resources, uint32},
            {10, islands, uint32},
            {11, fortress_level, byte},
            {12, defences, uint32},
            {13, buildings, uint32},
            {14, island_sectors, uint32},
            {15, town_sectors, uint32},
            {16, merchant_visits, uint32},
            {17, merchant_ships, uint32},
            {18, player_name, string},
            {19, reserved_resources, uint32},
            {20, battle_logs_attack, uint32},
            {21, battle_logs_defence, uint32},
            {22, last_pvp_attack_time, int64},
            {23, raid_players, uint32}
        ],

        RootPos = read_u32(Buffer, 0),
        Violations = verify_table_alignment(Buffer, RootPos, Fields),

        io:format("~n=== Many Refs + i64 (GameData pattern) Alignment Test ===~n"),
        io:format(
            "Buffer size: ~p bytes, root at: ~p (mod8=~p)~n",
            [byte_size(Buffer), RootPos, RootPos rem 8]
        ),
        dump_table_fields(Buffer, RootPos, Fields, "BigTable"),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "BigTable has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Odd number of small fields before u64 (forces misalignment)
%% =============================================================================
%% With 3 bool fields (3 bytes) + soffset (4 bytes) = 7 bytes from table start,
%% a u64 at field offset 7 would be at tablePos+7 which is misaligned unless
%% the builder inserts 1 byte of padding.

odd_small_fields_before_u64_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table OddSmall {\n"
                "                flag1: bool;\n"
                "                flag2: bool;\n"
                "                flag3: bool;\n"
                "                big: uint64;\n"
                "                flag4: bool;\n"
                "                another_big: int64;\n"
                "            }\n"
                "            root_type OddSmall;\n"
                "        "
            >>
        ),
        Map = #{
            flag1 => true,
            flag2 => false,
            flag3 => true,
            big => 1234567890123456789,
            flag4 => true,
            another_big => -42
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Fields = [
            {0, flag1, bool},
            {1, flag2, bool},
            {2, flag3, bool},
            {3, big, uint64},
            {4, flag4, bool},
            {5, another_big, int64}
        ],

        RootPos = read_u32(Buffer, 0),
        Violations = verify_table_alignment(Buffer, RootPos, Fields),

        io:format("~n=== Odd Small Fields + u64 Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "OddSmall has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: u16 + u64 interleaving (common in protocol messages)
%% =============================================================================

u16_u64_interleave_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table Interleaved {\n"
                "                version: uint16;\n"
                "                request_id: uint64;\n"
                "                trace: string;\n"
                "                timestamp_ms: uint64;\n"
                "                status: uint16;\n"
                "                round: uint64;\n"
                "            }\n"
                "            root_type Interleaved;\n"
                "        "
            >>
        ),
        Map = #{
            version => 1,
            request_id => 42,
            trace => <<"trace-123">>,
            timestamp_ms => 1710000000000,
            status => 200,
            round => 100
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Fields = [
            {0, version, uint16},
            {1, request_id, uint64},
            {2, trace, string},
            {3, timestamp_ms, uint64},
            {4, status, uint16},
            {5, round, uint64}
        ],

        RootPos = read_u32(Buffer, 0),
        Violations = verify_table_alignment(Buffer, RootPos, Fields),

        io:format("~n=== u16/u64 Interleave Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "Interleaved has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Single u32 field before u64 (simplest misalignment case)
%% =============================================================================
%% Table layout: soffset(4) + u32(4) = 8 bytes. u64 at offset 8 from table
%% start. If table is at 4-mod-8 position, the u32 is fine but u64 is at
%% (4+8)=12 which is 4 mod 8. This tests whether the builder aligns the
%% table start itself to accommodate 8-byte fields.

single_u32_before_u64_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table SimpleCase {\n"
                "                count: uint32;\n"
                "                timestamp: uint64;\n"
                "            }\n"
                "            root_type SimpleCase;\n"
                "        "
            >>
        ),
        Map = #{
            count => 42,
            timestamp => 1710000000000
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Fields = [
            {0, count, uint32},
            {1, timestamp, uint64}
        ],

        RootPos = read_u32(Buffer, 0),
        Violations = verify_table_alignment(Buffer, RootPos, Fields),

        io:format("~n=== Simple u32+u64 Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(Violations),
        ?assertEqual(
            [],
            Violations,
            lists:flatten(
                io_lib:format(
                    "SimpleCase has ~p alignment violations: ~p",
                    [length(Violations), Violations]
                )
            )
        )
    end}.

%% =============================================================================
%% Test: Nested table containing u64 after wrapper with odd-size fields
%% =============================================================================
%% This tests whether nested table positions inherit alignment requirements.
%% The outer table's layout affects where the inner table lands in the buffer.

nested_u64_after_odd_wrapper_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(
            <<
                "\n"
                "            table Inner {\n"
                "                ts1: uint64;\n"
                "                ts2: uint64;\n"
                "                label: string;\n"
                "            }\n"
                "            table Outer {\n"
                "                flag: bool;\n"
                "                name: string;\n"
                "                inner: Inner;\n"
                "                tag: uint16;\n"
                "            }\n"
                "            root_type Outer;\n"
                "        "
            >>
        ),
        Map = #{
            flag => true,
            name => <<"test">>,
            inner => #{
                ts1 => 1710000000000,
                ts2 => 1710003600000,
                label => <<"inner_label">>
            },
            tag => 99
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Check outer table
        OuterFields = [
            {0, flag, bool},
            {1, name, string},
            {2, inner, uint32},
            {3, tag, uint16}
        ],
        InnerFields = [
            {0, ts1, uint64},
            {1, ts2, uint64},
            {2, label, string}
        ],

        RootPos = read_u32(Buffer, 0),
        OuterViol = verify_table_alignment(Buffer, RootPos, OuterFields),

        %% Find inner table
        InnerViol = verify_nested_field(Buffer, RootPos, 2, InnerFields, inner),

        AllViolations = OuterViol ++ InnerViol,

        io:format("~n=== Nested u64 After Odd Wrapper Test ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(AllViolations),
        ?assertEqual(
            [],
            AllViolations,
            lists:flatten(
                io_lib:format(
                    "Nested has ~p alignment violations: ~p",
                    [length(AllViolations), AllViolations]
                )
            )
        )
    end}.

%% =============================================================================
%% Reporting helper
%% =============================================================================

%% Dump field positions for a table (diagnostic output)
dump_table_fields(Buffer, TablePos, FieldDefs, Label) ->
    io:format("~s table at ~p (mod8=~p)~n", [Label, TablePos, TablePos rem 8]),
    SOffset = read_i32(Buffer, TablePos),
    VTablePos = TablePos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),
    io:format("  vtable at ~p, size ~p~n", [VTablePos, VTableSize]),
    lists:foreach(
        fun({Idx, Name, _Type}) ->
            SlotP = VTablePos + 4 + Idx * 2,
            case SlotP + 2 =< VTablePos + VTableSize of
                true ->
                    FO = read_u16(Buffer, SlotP),
                    case FO of
                        0 ->
                            io:format("  field ~p (~p): absent~n", [Idx, Name]);
                        _ ->
                            io:format(
                                "  field ~p (~p): offset=~p, abs=~p (mod8=~p)~n",
                                [Idx, Name, FO, TablePos + FO, (TablePos + FO) rem 8]
                            )
                    end;
                false ->
                    io:format("  field ~p (~p): beyond vtable~n", [Idx, Name])
            end
        end,
        FieldDefs
    ).

report_violations([]) ->
    io:format("All fields properly aligned.~n");
report_violations(Violations) ->
    io:format("ALIGNMENT VIOLATIONS (~p):~n", [length(Violations)]),
    lists:foreach(
        fun({Name, Pos, Req, Mod}) ->
            io:format(
                "  ~p at byte ~p: requires ~p-byte alignment, "
                "position mod ~p = ~p~n",
                [Name, Pos, Req, Req, Mod]
            )
        end,
        Violations
    ).
