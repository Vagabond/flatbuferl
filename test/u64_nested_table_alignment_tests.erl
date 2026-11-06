-module(u64_nested_table_alignment_tests).
-include_lib("eunit/include/eunit.hrl").

%% Regression: a table with u64 fields nested directly in another table could
%% land with those fields off an 8-byte boundary, depending on the sizes of
%% what came before it. Walks the buffer and asserts each is 8-aligned.

schema_path() -> "test/complex_schemas/u64_nested_table.fbs".

envelope(NameLen, Members) ->
    #{
        id => 7,
        body_type => 'Req',
        body => #{
            name => binary:copy(<<"n">>, NameLen),
            txn => #{inner => 3},
            fuel => 5,
            round => #{
                seed => #{inner => binary:copy(<<9>>, 32)},
                timestamp => 1000 + NameLen,
                number => 2000 + Members,
                members => [#{bytes => <<I>>} || I <- lists:seq(1, Members)],
                leaders => [],
                heard => []
            }
        }
    }.

alignment_test_() ->
    {ok, Schema} = flatbuferl:parse_schema_file(schema_path()),
    [
        {lists:flatten(io_lib:format("name_len=~b members=~b", [L, M])), fun() ->
            check(Schema, L, M)
        end}
     || L <- lists:seq(0, 9), M <- [0, 1, 2, 3]
    ].

check(Schema, NameLen, Members) ->
    Buffer = iolist_to_binary(
        flatbuferl:from_map(envelope(NameLen, Members), Schema, #{root_type => 'Envelope'})
    ),
    ?assertEqual([], u64_alignment_violations(Buffer)),
    Decoded = flatbuferl:to_map(flatbuferl:new(Buffer, Schema, #{root_type => 'Envelope'})),
    Round = maps:get(round, maps:get(body, Decoded)),
    ?assertEqual(
        {1000 + NameLen, 2000 + Members},
        {maps:get(timestamp, Round), maps:get(number, Round)}
    ).

%% Walk root -> body -> round, and collect any u64 field whose position is
%% not 8-byte aligned.
u64_alignment_violations(B) ->
    Root = u32(B, 0),
    %% Envelope field 2 = body (field 1 is the union type).
    Body = deref(B, field(B, Root, 2)),
    %% Req field 3 = round.
    Round = deref(B, field(B, Body, 3)),
    %% Req field 2 = fuel, Round fields 1 and 2 = timestamp and number.
    [{req, 2, at, P} || P <- [field(B, Body, 2)], P rem 8 =/= 0] ++
        [{round, F, at, P} || F <- [1, 2], P <- [field(B, Round, F)], P rem 8 =/= 0].

field(B, Table, Id) ->
    VTable = Table - i32(B, Table),
    Table + u16(B, VTable + 4 + Id * 2).

deref(B, P) ->
    P + u32(B, P).

u32(B, P) ->
    <<_:P/binary, V:32/little, _/binary>> = B,
    V.

i32(B, P) ->
    <<_:P/binary, V:32/little-signed, _/binary>> = B,
    V.

u16(B, P) ->
    <<_:P/binary, V:16/little, _/binary>> = B,
    V.
