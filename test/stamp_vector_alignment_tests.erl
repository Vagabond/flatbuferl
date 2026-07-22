-module(stamp_vector_alignment_tests).
-include_lib("eunit/include/eunit.hrl").

%% Regression: in a vector of same-vtable tables, element 0's byte-vector
%% used to push element 1's name-string length prefix off a 4-byte
%% boundary. Walks the buffer and asserts each is 4-aligned.

schema_path() -> "test/complex_schemas/stamp_vector.fbs".

stamp() ->
    #{
        sender => [
            #{producer => <<>>, name => <<"source">>, value => <<"peer">>},
            #{producer => <<>>, name => <<"peer">>, value => list_to_binary(lists:seq(0, 37))}
        ],
        value => <<>>
    }.

roundtrip_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file(schema_path()),
    Buffer = iolist_to_binary(flatbuferl:from_map(stamp(), Schema, #{root_type => 'Cmd'})),
    Ctx = flatbuferl:new(Buffer, Schema, #{root_type => 'Cmd'}),
    Decoded = flatbuferl:to_map(Ctx),
    [S0, S1] = maps:get(sender, Decoded),
    ?assertEqual(<<"source">>, maps:get(name, S0)),
    ?assertEqual(<<"peer">>, maps:get(name, S1)),
    %% Without the flatbuferl_binary attribute a [ubyte] decodes to a list.
    ?assertEqual(list_to_binary(lists:seq(0, 37)), iolist_to_binary(maps:get(value, S1))).

element_string_alignment_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file(schema_path()),
    Buffer = iolist_to_binary(flatbuferl:from_map(stamp(), Schema, #{root_type => 'Cmd'})),
    ?assertEqual([], name_alignment_violations(Buffer)).

%% Walk root -> sender vector -> each Stamp table -> its `name` string, and
%% collect any whose length-prefix position is not 4-byte aligned.
name_alignment_violations(B) ->
    Root = u32(B, 0),
    RootVt = Root - i32(B, Root),
    %% Cmd field 0 = sender: voffset slot at vtable + 4.
    SenderFieldPos = Root + u16(B, RootVt + 4),
    Vec = SenderFieldPos + u32(B, SenderFieldPos),
    Count = u32(B, Vec),
    lists:filtermap(
        fun(I) ->
            ElemOffPos = Vec + 4 + I * 4,
            Table = ElemOffPos + u32(B, ElemOffPos),
            StampVt = Table - i32(B, Table),
            %% Stamp field 1 = name: voffset slot at vtable + 4 + (1 * 2).
            NameFieldPos = Table + u16(B, StampVt + 6),
            StrPos = NameFieldPos + u32(B, NameFieldPos),
            case StrPos rem 4 of
                0 -> false;
                R -> {true, {element, I, name_len_at, StrPos, mod4, R}}
            end
        end,
        lists:seq(0, Count - 1)
    ).

u32(B, P) ->
    <<_:P/binary, V:32/little, _/binary>> = B,
    V.

i32(B, P) ->
    <<_:P/binary, V:32/little-signed, _/binary>> = B,
    V.

u16(B, P) ->
    <<_:P/binary, V:16/little, _/binary>> = B,
    V.
