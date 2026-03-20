-module(alignment_tests).
-include_lib("eunit/include/eunit.hrl").

%% Test that nested table encoding produces properly aligned structures.

%% Reproduce the actual bug: encode CommitRoundRequest via Envelope
protocol_nested_table_alignment_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file(
        "../runner/priv/protocol.fbs"
    ),
    Map = #{
        version => 1,
        request_id => 1,
        timestamp_ms => 1000,
        message_type => 'CommitRoundRequest',
        message => #{
            service_id => #{id => <<"test_svc">>},
            round => 1,
            randomness => <<0:256>>,
            timestamp_ms => 1000,
            transactions => [
                #{
                    id => <<"txn1">>,
                    sender => <<"test">>,
                    data => <<"hello">>,
                    nonce => 0
                }
            ],
            checkpoint => true
        }
    },
    IoData = flatbuferl:from_map(Map, Schema),
    Buffer = iolist_to_binary(IoData),

    %% Verify roundtrip decode works
    Ctx = flatbuferl:new(Buffer, Schema),
    Decoded = flatbuferl:to_map(Ctx),
    ?assertEqual('CommitRoundRequest', maps:get(message_type, Decoded)),

    check_all_tables_aligned(Buffer).

check_all_tables_aligned(Buffer) ->
    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    RootPos = RootOffset,
    ?assertEqual(0, RootPos rem 4, "Root table must be 4-byte aligned"),

    %% Read root table vtable
    <<_:RootPos/binary, RootSOffset:32/little-signed, _/binary>> = Buffer,
    RootVTablePos = RootPos - RootSOffset,

    %% message is field 5 in Envelope
    MsgSlotPos = RootVTablePos + 4 + 5 * 2,
    <<_:MsgSlotPos/binary, MsgFieldOffset:16/little-unsigned, _/binary>> = Buffer,
    MsgFieldPos = RootPos + MsgFieldOffset,

    %% Follow uoffset to CommitRoundRequest table
    <<_:MsgFieldPos/binary, MsgUOffset:32/little-unsigned, _/binary>> = Buffer,
    CRRPos = MsgFieldPos + MsgUOffset,
    ?assertEqual(0, CRRPos rem 4,
        io_lib:format("CommitRoundRequest at ~p must be 4-byte aligned", [CRRPos])),

    %% Read CRR vtable
    <<_:CRRPos/binary, CRRSOffset:32/little-signed, _/binary>> = Buffer,
    CRRVTablePos = CRRPos - CRRSOffset,

    %% service_id is field 0 in CommitRoundRequest
    SvcIdSlotPos = CRRVTablePos + 4,
    <<_:SvcIdSlotPos/binary, SvcIdFieldOffset:16/little-unsigned, _/binary>> = Buffer,
    SvcIdFieldPos = CRRPos + SvcIdFieldOffset,

    %% Follow uoffset to ServiceId table
    <<_:SvcIdFieldPos/binary, SvcIdUOffset:32/little-unsigned, _/binary>> = Buffer,
    ServiceIdPos = SvcIdFieldPos + SvcIdUOffset,

    io:format("~n=== Debug ===~n"),
    io:format("RootPos=~p RootVTablePos=~p~n", [RootPos, RootVTablePos]),
    io:format("MsgFieldPos=~p MsgUOffset=~p CRRPos=~p~n", [MsgFieldPos, MsgUOffset, CRRPos]),
    io:format("CRRSOffset=~p CRRVTablePos=~p~n", [CRRSOffset, CRRVTablePos]),
    io:format("SvcIdFieldOffset=~p SvcIdFieldPos=~p~n", [SvcIdFieldOffset, SvcIdFieldPos]),
    io:format("SvcIdUOffset=~p ServiceIdPos=~p~n", [SvcIdUOffset, ServiceIdPos]),
    io:format("ServiceIdPos rem 4 = ~p~n", [ServiceIdPos rem 4]),
    io:format("Buffer size = ~p~n", [byte_size(Buffer)]),
    io:format("Buffer hex (first 200): ~s~n", [binary_to_hex(binary:part(Buffer, 0, min(200, byte_size(Buffer))))]),

    ?assertEqual(0, ServiceIdPos rem 4,
        lists:flatten(io_lib:format(
            "ServiceId table at position ~p must be 4-byte aligned (rem=~p)", [ServiceIdPos, ServiceIdPos rem 4]))),

    ok.

binary_to_hex(Bin) ->
    lists:flatten([io_lib:format("~2.16.0B ", [B]) || <<B>> <= Bin]).

%% Test with simpler nested table (no union) - should pass
simple_nested_alignment_test() ->
    {ok, Schema} = flatbuferl:parse_schema(<<"
        table ServiceId { id: string (required); }
        table Request {
            service_id: ServiceId (required);
            round: uint64;
            data: [ubyte] (required);
            ts: uint64;
        }
        root_type Request;
    ">>),
    Map = #{
        service_id => #{id => <<"test_svc">>},
        round => 1,
        data => <<0:256>>,
        ts => 1000
    },
    IoData = flatbuferl:from_map(Map, Schema),
    Buffer = iolist_to_binary(IoData),

    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    RootPos = RootOffset,

    <<_:RootPos/binary, RSOffset:32/little-signed, _/binary>> = Buffer,
    RVTablePos = RootPos - RSOffset,

    SvcSlotPos = RVTablePos + 4,
    <<_:SvcSlotPos/binary, SvcFieldOffset:16/little-unsigned, _/binary>> = Buffer,
    SvcFieldPos = RootPos + SvcFieldOffset,

    <<_:SvcFieldPos/binary, SvcUOffset:32/little-unsigned, _/binary>> = Buffer,
    ServiceIdPos = SvcFieldPos + SvcUOffset,
    ?assertEqual(0, ServiceIdPos rem 4,
        lists:flatten(io_lib:format(
            "ServiceId table at ~p must be 4-byte aligned", [ServiceIdPos]))),
    ok.

%% Test: CommitRoundRequest as direct root (no union wrapping)
crr_direct_alignment_test() ->
    {ok, Schema} = flatbuferl:parse_schema(<<"
        table ServiceId { id: string (required); }
        table Transaction {
            id: [ubyte] (required);
            sender: string (required);
            data: [ubyte] (required);
            nonce: uint64 = 0;
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

    <<RootOffset:32/little-unsigned, _/binary>> = Buffer,
    RootPos = RootOffset,

    <<_:RootPos/binary, RSOffset:32/little-signed, _/binary>> = Buffer,
    RVTablePos = RootPos - RSOffset,

    %% service_id is field 0
    SvcSlotPos = RVTablePos + 4,
    <<_:SvcSlotPos/binary, SvcFieldOffset:16/little-unsigned, _/binary>> = Buffer,
    SvcFieldPos = RootPos + SvcFieldOffset,

    <<_:SvcFieldPos/binary, SvcUOffset:32/little-unsigned, _/binary>> = Buffer,
    ServiceIdPos = SvcFieldPos + SvcUOffset,

    io:format("~n=== CRR Direct ===~n"),
    io:format("RootPos=~p RVTablePos=~p~n", [RootPos, RVTablePos]),
    io:format("SvcFieldOffset=~p SvcFieldPos=~p~n", [SvcFieldOffset, SvcFieldPos]),
    io:format("SvcUOffset=~p ServiceIdPos=~p rem4=~p~n", [SvcUOffset, ServiceIdPos, ServiceIdPos rem 4]),

    ?assertEqual(0, ServiceIdPos rem 4,
        lists:flatten(io_lib:format(
            "ServiceId table at ~p must be 4-byte aligned (rem=~p)", [ServiceIdPos, ServiceIdPos rem 4]))),
    ok.
