-module(alignment_verification_tests).
-include_lib("eunit/include/eunit.hrl").

%% Suppress warnings for helper functions kept for future test expansion
-compile({nowarn_unused_function, [
    verify_vector_alignment/2,
    deployment_id_fields/0,
    time_interval_fields/0,
    building_fields/0
]}).

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
type_alignment(bool)    -> 1;
type_alignment(byte)    -> 1;
type_alignment(ubyte)   -> 1;
type_alignment(int8)    -> 1;
type_alignment(uint8)   -> 1;
type_alignment(short)   -> 2;
type_alignment(ushort)  -> 2;
type_alignment(int16)   -> 2;
type_alignment(uint16)  -> 2;
type_alignment(int)     -> 4;
type_alignment(uint)    -> 4;
type_alignment(int32)   -> 4;
type_alignment(uint32)  -> 4;
type_alignment(float)   -> 4;
type_alignment(float32) -> 4;
type_alignment(long)    -> 8;
type_alignment(ulong)   -> 8;
type_alignment(int64)   -> 8;
type_alignment(uint64)  -> 8;
type_alignment(double)  -> 8;
type_alignment(float64) -> 8;
type_alignment(string)  -> 4;  % uoffset
type_alignment(_)       -> 4.  % table refs, vectors are uoffsets (4 bytes)

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
                false -> Acc;
                true ->
                    SlotPos = VTablePos + 4 + FieldIndex * 2,
                    FieldOffset = read_u16(Buffer, SlotPos),
                    case FieldOffset of
                        0 -> Acc;  % field not present
                        _ ->
                            AbsFieldPos = TablePos + FieldOffset,
                            RequiredAlign = type_alignment(FieldType),
                            case AbsFieldPos rem RequiredAlign of
                                0 -> Acc;
                                Mod ->
                                    [{FieldName, AbsFieldPos, RequiredAlign, Mod} | Acc]
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

%% Envelope fields (indices match FlatBuffer field order in schema):
%%   0: version (uint16)
%%   1: request_id (uint64)
%%   2: trace_id (string)
%%   3: timestamp_ms (uint64)
%%   4: message_type (union type byte - implicit)
%%   5: message (union value - uoffset)
envelope_fields() ->
    [{0, version, uint16},
     {1, request_id, uint64},
     {2, trace_id, string},
     {3, timestamp_ms, uint64}].
     %% Union type/value handled separately

%% CommitRoundRequest fields:
%%   0: service_id (table ref -> uoffset)
%%   1: round (uint64)
%%   2: randomness (vector -> uoffset)
%%   3: timestamp_ms (uint64)
%%   4: transactions (vector of tables -> uoffset)
%%   5: checkpoint (bool)
%%   6: epoch_transition (table ref -> uoffset)
commit_round_request_fields() ->
    [{0, service_id, uint32},      % uoffset to ServiceId table
     {1, round, uint64},
     {2, randomness, uint32},      % uoffset to vector
     {3, timestamp_ms, uint64},
     {4, transactions, uint32},    % uoffset to vector
     {5, checkpoint, bool},
     {6, epoch_transition, uint32}]. % uoffset to table

%% InitServiceRequest fields:
%%   0: service_id (table ref)
%%   1: randomness (vector)
%%   2: group_pubkey (vector)
%%   3: members (vector of strings)
%%   4: node_index (uint32)
init_service_request_fields() ->
    [{0, service_id, uint32},
     {1, randomness, uint32},
     {2, group_pubkey, uint32},
     {3, members, uint32},
     {4, node_index, uint32}].

%% DeployRequest fields:
%%   0: deployment_id (table ref)
%%   1: component_type (union type byte)
%%   2: component (union value -> uoffset)
%%   3: init_randomness (vector)
%%   4: deployer (string)
%%   5: deployer_signature (vector)
deploy_request_fields() ->
    [{0, deployment_id, uint32},
     {1, component_type, ubyte},
     {2, component, uint32},
     {3, init_randomness, uint32},
     {4, deployer, string},
     {5, deployer_signature, uint32}].

%% ServiceId fields:
%%   0: id (string -> uoffset)
service_id_fields() ->
    [{0, id, string}].

%% Transaction fields:
%%   0: id (vector -> uoffset)
%%   1: sender (string -> uoffset)
%%   2: data (vector -> uoffset)
%%   3: nonce (uint64)
%%   4: signature (vector -> uoffset)
transaction_fields() ->
    [{0, id, uint32},
     {1, sender, string},
     {2, data, uint32},
     {3, nonce, uint64},
     {4, signature, uint32}].

%% DeploymentId fields:
%%   0: service_id (string)
%%   1: deployment_id (string)
deployment_id_fields() ->
    [{0, service_id, string},
     {1, deployment_id, string}].

%% =============================================================================
%% Game state schema field definitions
%% =============================================================================

%% GameStateRoot fields:
%%   0: version (int = int32)
%%   1: gameData1 (table ref)
game_state_root_fields() ->
    [{0, version, int32},
     {1, 'gameData1', uint32}].

%% GameData fields (selected - the ones with long/i64):
%%   6: nextPirateAttack (long)
%%   13: merchantVisits ([long])  - vector
%%   20: lastPvpAttackTime (long)
game_data_fields() ->
    [{0, workers, uint32},
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
     {23, 'raidPlayers', uint32}].

%% Workers fields:
%%   0: productionStartTime (long)
%%   1: assignedWorkers (int)
%%   2: productionEndTime (long)
workers_fields() ->
    [{0, 'productionStartTime', int64},
     {1, 'assignedWorkers', int32},
     {2, 'productionEndTime', int64}].

%% TimeInterval fields:
%%   0: start (long)
%%   1: end_field (long)
time_interval_fields() ->
    [{0, start, int64},
     {1, 'end', int64}].

%% Building fields (selected for i64):
%%   8: currentCraftingStartTime (long)
%%   11: pause (long)
building_fields() ->
    [{0, type, string},
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
     {16, 'defenceUpgrading', bool}].

%% =============================================================================
%% Test: Protocol Envelope wrapping CommitRoundRequest
%% =============================================================================
%% This is the exact message pattern that fails in production.
%% The Rust strict verifier rejects it due to misaligned u64 fields.

protocol_envelope_commit_round_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 42,
            timestamp_ms => 1710000000000,
            trace_id => <<"trace-abc-123">>,
            message_type => 'CommitRoundRequest',
            message => #{
                service_id => #{id => <<"test-service-alpha">>},
                round => 100,
                randomness => crypto:strong_rand_bytes(32),
                timestamp_ms => 1710000000000,
                transactions => [
                    #{
                        id => <<"txn-001">>,
                        sender => <<"peer-1234">>,
                        data => <<"increment">>,
                        nonce => 1
                    },
                    #{
                        id => <<"txn-002">>,
                        sender => <<"peer-5678">>,
                        data => <<"transfer">>,
                        nonce => 2
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
        ?assertEqual('CommitRoundRequest', maps:get(message_type, Decoded)),

        %% Now check actual byte-level alignment
        Violations = verify_protocol_envelope(Buffer, commit_round_request_fields()),

        io:format("~n=== CommitRoundRequest Envelope Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        case Violations of
            [] ->
                io:format("All fields properly aligned.~n");
            _ ->
                io:format("ALIGNMENT VIOLATIONS (~p):~n", [length(Violations)]),
                lists:foreach(fun({Name, Pos, Req, Mod}) ->
                    io:format("  ~p at byte ~p: requires ~p-byte alignment, position mod ~p = ~p~n",
                              [Name, Pos, Req, Req, Mod])
                end, Violations)
        end,
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "CommitRoundRequest has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Protocol Envelope wrapping InitServiceRequest
%% =============================================================================

protocol_envelope_init_service_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 1,
            timestamp_ms => 1710000000000,
            message_type => 'InitServiceRequest',
            message => #{
                service_id => #{id => <<"my-service">>},
                randomness => crypto:strong_rand_bytes(32),
                group_pubkey => crypto:strong_rand_bytes(48),
                members => [<<"peer-1">>, <<"peer-2">>, <<"peer-3">>],
                node_index => 0
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('InitServiceRequest', maps:get(message_type, Decoded)),

        Violations = verify_protocol_envelope(Buffer, init_service_request_fields()),

        io:format("~n=== InitServiceRequest Envelope Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "InitServiceRequest has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Protocol Envelope wrapping DeployRequest
%% =============================================================================

protocol_envelope_deploy_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 7,
            timestamp_ms => 1710000000000,
            message_type => 'DeployRequest',
            message => #{
                deployment_id => #{
                    service_id => <<"svc-1">>,
                    deployment_id => <<"deploy-1">>
                },
                component_type => 'InlineComponent',
                component => #{
                    bytes => crypto:strong_rand_bytes(64)
                },
                init_randomness => crypto:strong_rand_bytes(32),
                deployer => <<"admin-peer">>
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('DeployRequest', maps:get(message_type, Decoded)),

        Violations = verify_protocol_envelope(Buffer, deploy_request_fields()),

        io:format("~n=== DeployRequest Envelope Alignment ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "DeployRequest has ~p alignment violations: ~p",
                [length(Violations), Violations])))
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
                    #{name => <<"build_farm">>, 'taskAmountToComplete' => 1,
                      'taskAmountDone' => 0, done => false}
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
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "GameState has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
    end}.

%% =============================================================================
%% Test: Scalar alignment with u64/i64 fields
%% =============================================================================
%% This test focuses purely on scalar alignment of 8-byte fields.

scalar_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table Inner {
                big_val: uint64;
                small_val: uint16;
                another_big: int64;
            }
            table ScalarTest {
                a_byte: ubyte;
                a_u64: uint64;
                a_string: string;
                a_i64: int64;
                inner: Inner;
                a_u32: uint32;
                another_u64: uint64;
            }
            root_type ScalarTest;
        ">>),
        Map = #{
            a_byte => 42,
            a_u64 => 18446744073709551615,  % max u64
            a_string => <<"hello">>,
            a_i64 => -9223372036854775808,   % min i64
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
            {4, inner, uint32},    % uoffset
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
        InnerSlotPos = VTablePos + 4 + 4 * 2,  % field index 4
        InnerViolations =
            case InnerSlotPos + 2 =< VTablePos + VTableSize of
                true ->
                    InnerFieldOffset = read_u16(Buffer, InnerSlotPos),
                    case InnerFieldOffset of
                        0 -> [];
                        _ ->
                            InnerRefPos = RootPos + InnerFieldOffset,
                            InnerTablePos = follow_uoffset(Buffer, InnerRefPos),
                            InnerTableViol = verify_table_alignment(Buffer, InnerTablePos, InnerFields),
                            %% Also check the inner table uoffset itself is 4-aligned
                            UOffsetViol = case InnerRefPos rem 4 of
                                0 -> [];
                                R -> [{inner_uoffset, InnerRefPos, 4, R}]
                            end,
                            UOffsetViol ++ InnerTableViol
                    end;
                false -> []
            end,

        AllViolations = RootViolations ++ InnerViolations,

        io:format("~n=== Scalar Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(AllViolations),
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "ScalarTest has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
    end}.

%% =============================================================================
%% Test: Multiple u64 fields in sequence (stress test for padding)
%% =============================================================================

multi_u64_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table MultiU64 {
                flag: bool;
                val1: uint64;
                val2: uint64;
                val3: uint64;
                small: uint16;
                val4: uint64;
            }
            root_type MultiU64;
        ">>),
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
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "MultiU64 has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Vector of u64 (long) — vector data must be 8-byte aligned
%% =============================================================================

vector_u64_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table VecTest {
                label: string;
                timestamps: [uint64];
                count: uint32;
            }
            root_type VecTest;
        ">>),
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

        Violations = case TimestampsFieldOffset of
            0 -> [{timestamps_missing, 0, 0, 0}];
            _ ->
                TimestampsRefPos = RootPos + TimestampsFieldOffset,
                VecPos = follow_uoffset(Buffer, TimestampsRefPos),
                %% Vector length prefix must be 4-byte aligned
                LenViol = case VecPos rem 4 of
                    0 -> [];
                    R1 -> [{timestamps_vec_length, VecPos, 4, R1}]
                end,
                %% Vector data (u64 elements) starts at VecPos + 4
                %% Each u64 element should be 8-byte aligned
                DataStart = VecPos + 4,
                DataViol = case DataStart rem 8 of
                    0 -> [];
                    R2 -> [{timestamps_vec_data, DataStart, 8, R2}]
                end,
                LenViol ++ DataViol
        end,

        io:format("~n=== Vector u64 Alignment Test ===~n"),
        io:format("Buffer size: ~p bytes~n", [byte_size(Buffer)]),
        report_violations(Violations),
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "VecTest has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Deeply nested tables (3+ levels deep)
%% =============================================================================

deep_nesting_alignment_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table Level3 {
                value: uint64;
                tag: string;
            }
            table Level2 {
                child: Level3;
                counter: uint64;
                name: string;
            }
            table Level1 {
                inner: Level2;
                timestamp: uint64;
                flag: bool;
            }
            table Root {
                level1: Level1;
                id: uint64;
            }
            root_type Root;
        ">>),
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
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "DeepNesting has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
    end}.

%% =============================================================================
%% Test: CommitRoundRequest direct root (no union wrapping) with specific
%% byte position checks matching the known production failures.
%% =============================================================================

commit_round_direct_known_positions_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table ServiceId { id: string (required); }
            table Transaction {
                id: [ubyte] (required);
                sender: string (required);
                data: [ubyte] (required);
                nonce: uint64 = 0;
                signature: [ubyte];
            }
            table CommitRoundRequest {
                service_id: ServiceId (required);
                round: uint64;
                randomness: [ubyte] (required);
                timestamp_ms: uint64;
                transactions: [Transaction] (required);
                checkpoint: bool = true;
            }
            root_type CommitRoundRequest;
        ">>),
        Map = #{
            service_id => #{id => <<"test_svc">>},
            round => 1,
            randomness => <<0:256>>,
            timestamp_ms => 1000,
            transactions => [
                #{id => <<"txn1">>, sender => <<"test">>, data => <<"hello">>, nonce => 0}
            ],
            checkpoint => true
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        %% Check all fields in CommitRoundRequest
        RootPos = read_u32(Buffer, 0),
        CRRViolations = verify_table_alignment(Buffer, RootPos,
            commit_round_request_fields()),

        %% Also check the ServiceId table that CRR points to
        SOffset = read_i32(Buffer, RootPos),
        VTablePos = RootPos - SOffset,
        SvcIdSlotPos = VTablePos + 4,  % field 0
        SvcIdFieldOffset = read_u16(Buffer, SvcIdSlotPos),
        SvcIdViolations = case SvcIdFieldOffset of
            0 -> [{service_id_missing, 0, 0, 0}];
            _ ->
                SvcIdRefPos = RootPos + SvcIdFieldOffset,
                SvcIdTablePos = follow_uoffset(Buffer, SvcIdRefPos),
                %% Check uoffset alignment
                UOffViol = case SvcIdRefPos rem 4 of
                    0 -> [];
                    R -> [{service_id_uoffset, SvcIdRefPos, 4, R}]
                end,
                %% Check table alignment
                TableViol = verify_table_alignment(Buffer, SvcIdTablePos,
                    service_id_fields()),
                UOffViol ++ TableViol
        end,

        %% Check Transaction tables
        TxnSlotPos = VTablePos + 4 + 4 * 2,  % field 4 = transactions
        TxnFieldOffset = read_u16(Buffer, TxnSlotPos),
        TxnViolations = case TxnFieldOffset of
            0 -> [];
            _ ->
                TxnRefPos = RootPos + TxnFieldOffset,
                TxnVecPos = follow_uoffset(Buffer, TxnRefPos),
                NumTxns = read_u32(Buffer, TxnVecPos),
                lists:foldl(fun(I, Acc) ->
                    TxnOffsetPos = TxnVecPos + 4 + I * 4,
                    TxnTablePos = follow_uoffset(Buffer, TxnOffsetPos),
                    TxnViol = verify_table_alignment(Buffer, TxnTablePos,
                        transaction_fields()),
                    TaggedViol = [{list_to_atom("txn_" ++ integer_to_list(I) ++ "_" ++
                                   atom_to_list(N)), P, A, M}
                                  || {N, P, A, M} <- TxnViol],
                    Acc ++ TaggedViol
                end, [], lists:seq(0, NumTxns - 1))
        end,

        AllViolations = CRRViolations ++ SvcIdViolations ++ TxnViolations,

        io:format("~n=== CommitRoundRequest Direct (Known Positions) ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(AllViolations),
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "CommitRoundRequest direct has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
    end}.

%% =============================================================================
%% Test: Envelope with small inner message (tests union padding interaction)
%% =============================================================================

protocol_envelope_shutdown_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema_file(protocol_schema_path()),
        Map = #{
            version => 1,
            request_id => 99,
            timestamp_ms => 1710000000000,
            message_type => 'ShutdownRequest',
            message => #{
                service_id => #{id => <<"svc-shutdown">>},
                timeout_ms => 10000
            }
        },
        IoData = flatbuferl:from_map(Map, Schema),
        Buffer = iolist_to_binary(IoData),

        Ctx = flatbuferl:new(Buffer, Schema),
        Decoded = flatbuferl:to_map(Ctx),
        ?assertEqual('ShutdownRequest', maps:get(message_type, Decoded)),

        %% Check Envelope fields
        RootPos = read_u32(Buffer, 0),
        EnvelopeViolations = verify_table_alignment(Buffer, RootPos, envelope_fields()),

        %% Check Envelope table position itself
        RootPosViol = case RootPos rem 4 of
            0 -> [];
            R -> [{root_table_pos, RootPos, 4, R}]
        end,

        AllViolations = RootPosViol ++ EnvelopeViolations,

        io:format("~n=== Shutdown Envelope Alignment ===~n"),
        io:format("Buffer size: ~p bytes, root at: ~p~n", [byte_size(Buffer), RootPos]),
        report_violations(AllViolations),
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "Shutdown envelope has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
    end}.

%% =============================================================================
%% Helper: Verify protocol envelope + inner message
%% =============================================================================

verify_protocol_envelope(Buffer, InnerFields) ->
    RootPos = read_u32(Buffer, 0),

    %% Check root (Envelope) table alignment
    EnvelopeViolations = verify_table_alignment(Buffer, RootPos, envelope_fields()),

    %% Navigate to the union message (field index 4 = type, field index 5 = value)
    SOffset = read_i32(Buffer, RootPos),
    VTablePos = RootPos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    %% Union value is at vtable slot 5 (0-indexed from field 0)
    %% Envelope: version(0), request_id(1), trace_id(2), timestamp_ms(3),
    %%           message_type(4), message(5)
    MsgSlotPos = VTablePos + 4 + 5 * 2,
    InnerViolations = case MsgSlotPos + 2 =< VTablePos + VTableSize of
        false -> [];
        true ->
            MsgFieldOffset = read_u16(Buffer, MsgSlotPos),
            case MsgFieldOffset of
                0 -> [];
                _ ->
                    MsgRefPos = RootPos + MsgFieldOffset,
                    InnerTablePos = follow_uoffset(Buffer, MsgRefPos),

                    %% Check uoffset alignment
                    UOffViol = case MsgRefPos rem 4 of
                        0 -> [];
                        R -> [{message_uoffset, MsgRefPos, 4, R}]
                    end,

                    %% Check inner table alignment
                    InnerViol = verify_table_alignment(Buffer, InnerTablePos, InnerFields),

                    %% For CommitRoundRequest, also check nested ServiceId
                    NestedViol = verify_inner_nested_tables(
                        Buffer, InnerTablePos, InnerFields),

                    UOffViol ++ InnerViol ++ NestedViol
            end
    end,

    EnvelopeViolations ++ InnerViolations.

%% Check nested tables inside the inner message
verify_inner_nested_tables(Buffer, InnerTablePos, InnerFields) ->
    SOffset = read_i32(Buffer, InnerTablePos),
    VTablePos = InnerTablePos - SOffset,
    VTableSize = read_u16(Buffer, VTablePos),

    lists:foldl(
        fun({FieldIdx, FieldName, FieldType}, Acc) ->
            %% Only check uoffset fields that point to tables
            case FieldType of
                uint32 when FieldName =:= service_id;
                             FieldName =:= deployment_id ->
                    SlotPos = VTablePos + 4 + FieldIdx * 2,
                    case SlotPos + 2 =< VTablePos + VTableSize of
                        false -> Acc;
                        true ->
                            FieldOffset = read_u16(Buffer, SlotPos),
                            case FieldOffset of
                                0 -> Acc;
                                _ ->
                                    RefPos = InnerTablePos + FieldOffset,
                                    TargetPos = follow_uoffset(Buffer, RefPos),
                                    Violations = case TargetPos rem 4 of
                                        0 -> [];
                                        R ->
                                            [{list_to_atom(atom_to_list(FieldName) ++ "_table"),
                                              TargetPos, 4, R}]
                                    end,
                                    Acc ++ Violations
                            end
                    end;
                _ -> Acc
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

    GameDataSlotPos = VTablePos + 4 + 1 * 2,  % field 1
    GameDataViol = case GameDataSlotPos + 2 =< VTablePos + VTableSize of
        false -> [];
        true ->
            GameDataFieldOffset = read_u16(Buffer, GameDataSlotPos),
            case GameDataFieldOffset of
                0 -> [];
                _ ->
                    GameDataRefPos = RootPos + GameDataFieldOffset,
                    GameDataTablePos = follow_uoffset(Buffer, GameDataRefPos),

                    %% Dump GameData field positions for diagnosis
                    dump_table_fields(Buffer, GameDataTablePos,
                        game_data_fields(), "GameData"),

                    GDViol = verify_table_alignment(Buffer, GameDataTablePos,
                        game_data_fields()),

                    %% Check Workers table (field 0 of GameData)
                    WorkersViol = verify_nested_field(Buffer, GameDataTablePos, 0,
                        workers_fields(), workers),

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
        false -> [];
        true ->
            FieldOffset = read_u16(Buffer, SlotPos),
            case FieldOffset of
                0 -> [];
                _ ->
                    RefPos = ParentPos + FieldOffset,
                    TablePos = follow_uoffset(Buffer, RefPos),
                    Violations = verify_table_alignment(Buffer, TablePos, FieldDefs),
                    %% Tag violations with the nesting label
                    [{list_to_atom(atom_to_list(Label) ++ "." ++ atom_to_list(N)),
                      P, A, M} || {N, P, A, M} <- Violations]
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
    TableRefField = lists:foldl(fun
        ({Idx, _Name, uint32}, none) -> Idx;
        (_, Found) -> Found
    end, none, ParentFields),

    case TableRefField of
        none -> Acc;
        FieldIdx ->
            SOffset = read_i32(Buffer, ParentPos),
            VTablePos = ParentPos - SOffset,
            VTableSize = read_u16(Buffer, VTablePos),

            SlotPos = VTablePos + 4 + FieldIdx * 2,
            case SlotPos + 2 =< VTablePos + VTableSize of
                false -> Acc;
                true ->
                    FieldOffset = read_u16(Buffer, SlotPos),
                    case FieldOffset of
                        0 -> Acc;
                        _ ->
                            RefPos = ParentPos + FieldOffset,
                            ChildPos = follow_uoffset(Buffer, RefPos),
                            ChildViol = verify_table_alignment(Buffer, ChildPos, Fields),
                            Tagged = [{list_to_atom(atom_to_list(Label) ++ "." ++
                                       atom_to_list(N)), P, A, M}
                                      || {N, P, A, M} <- ChildViol],
                            verify_nested_tables_level(
                                Buffer, ChildPos, Fields, Rest, Acc ++ Tagged)
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
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table Item { name: string; value: int; }
            table BigTable {
                workers: Item;
                trophies: int;
                academy_techs: [Item];
                arsenal_techs: [Item];
                ships: [Item];
                current_quests: [Item];
                completed_quests: [string];
                player_resources: [Item];
                next_pirate_attack: long;
                left_over_resources: [Item];
                islands: [Item];
                fortress_level: byte;
                defences: [Item];
                buildings: [Item];
                island_sectors: [Item];
                town_sectors: [Item];
                merchant_visits: [long];
                merchant_ships: [Item];
                player_name: string;
                reserved_resources: [Item];
                battle_logs_attack: [Item];
                battle_logs_defence: [Item];
                last_pvp_attack_time: long;
                raid_players: [Item];
            }
            root_type BigTable;
        ">>),
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
        io:format("Buffer size: ~p bytes, root at: ~p (mod8=~p)~n",
                  [byte_size(Buffer), RootPos, RootPos rem 8]),
        dump_table_fields(Buffer, RootPos, Fields, "BigTable"),
        report_violations(Violations),
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "BigTable has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Odd number of small fields before u64 (forces misalignment)
%% =============================================================================
%% With 3 bool fields (3 bytes) + soffset (4 bytes) = 7 bytes from table start,
%% a u64 at field offset 7 would be at tablePos+7 which is misaligned unless
%% the builder inserts 1 byte of padding.

odd_small_fields_before_u64_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table OddSmall {
                flag1: bool;
                flag2: bool;
                flag3: bool;
                big: uint64;
                flag4: bool;
                another_big: int64;
            }
            root_type OddSmall;
        ">>),
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
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "OddSmall has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: u16 + u64 interleaving (common in protocol messages)
%% =============================================================================

u16_u64_interleave_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table Interleaved {
                version: uint16;
                request_id: uint64;
                trace: string;
                timestamp_ms: uint64;
                status: uint16;
                round: uint64;
            }
            root_type Interleaved;
        ">>),
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
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "Interleaved has ~p alignment violations: ~p",
                [length(Violations), Violations])))
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
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table SimpleCase {
                count: uint32;
                timestamp: uint64;
            }
            root_type SimpleCase;
        ">>),
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
        ?assertEqual([], Violations,
            lists:flatten(io_lib:format(
                "SimpleCase has ~p alignment violations: ~p",
                [length(Violations), Violations])))
    end}.

%% =============================================================================
%% Test: Nested table containing u64 after wrapper with odd-size fields
%% =============================================================================
%% This tests whether nested table positions inherit alignment requirements.
%% The outer table's layout affects where the inner table lands in the buffer.

nested_u64_after_odd_wrapper_test_() ->
    {timeout, 30, fun() ->
        {ok, Schema} = flatbuferl:parse_schema(<<"
            table Inner {
                ts1: uint64;
                ts2: uint64;
                label: string;
            }
            table Outer {
                flag: bool;
                name: string;
                inner: Inner;
                tag: uint16;
            }
            root_type Outer;
        ">>),
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
        ?assertEqual([], AllViolations,
            lists:flatten(io_lib:format(
                "Nested has ~p alignment violations: ~p",
                [length(AllViolations), AllViolations])))
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
    lists:foreach(fun({Idx, Name, _Type}) ->
        SlotP = VTablePos + 4 + Idx * 2,
        case SlotP + 2 =< VTablePos + VTableSize of
            true ->
                FO = read_u16(Buffer, SlotP),
                case FO of
                    0 -> io:format("  field ~p (~p): absent~n", [Idx, Name]);
                    _ -> io:format("  field ~p (~p): offset=~p, abs=~p (mod8=~p)~n",
                                   [Idx, Name, FO, TablePos + FO, (TablePos + FO) rem 8])
                end;
            false ->
                io:format("  field ~p (~p): beyond vtable~n", [Idx, Name])
        end
    end, FieldDefs).

report_violations([]) ->
    io:format("All fields properly aligned.~n");
report_violations(Violations) ->
    io:format("ALIGNMENT VIOLATIONS (~p):~n", [length(Violations)]),
    lists:foreach(fun({Name, Pos, Req, Mod}) ->
        io:format("  ~p at byte ~p: requires ~p-byte alignment, "
                  "position mod ~p = ~p~n",
                  [Name, Pos, Req, Req, Mod])
    end, Violations).
