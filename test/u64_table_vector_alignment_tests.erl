-module(u64_table_vector_alignment_tests).
-include_lib("eunit/include/eunit.hrl").

%% Regression: in a vector of tables with u64 fields, every other element's
%% u64 fields landed 4 bytes off an 8-byte boundary. Walks the buffer and
%% asserts each is 8-aligned, across vector lengths and string sizes.

schema_path() -> "test/complex_schemas/u64_table_vector.fbs".

envelope(N, NameLen) ->
    #{
        id => 7,
        body_type => 'Bulk',
        body => #{
            tag => <<"bulk">>,
            items => [
                #{
                    name => binary:copy(<<"n">>, NameLen + I),
                    data => lists:seq(1, I),
                    a => 1000 + I,
                    b => 2000 + I
                }
             || I <- lists:seq(1, N)
            ]
        }
    }.

alignment_test_() ->
    {ok, Schema} = flatbuferl:parse_schema_file(schema_path()),
    [
        {lists:flatten(io_lib:format("n=~b name_len=~b", [N, L])), fun() ->
            check(Schema, N, L)
        end}
     || N <- lists:seq(1, 8), L <- [0, 1, 3, 5]
    ].

check(Schema, N, NameLen) ->
    Buffer = iolist_to_binary(
        flatbuferl:from_map(envelope(N, NameLen), Schema, #{root_type => 'Envelope'})
    ),
    ?assertEqual([], u64_alignment_violations(Buffer)),
    Decoded = flatbuferl:to_map(flatbuferl:new(Buffer, Schema, #{root_type => 'Envelope'})),
    Items = maps:get(items, maps:get(body, Decoded)),
    ?assertEqual(
        [{1000 + I, 2000 + I} || I <- lists:seq(1, N)],
        [{maps:get(a, It), maps:get(b, It)} || It <- Items]
    ).

%% Walk root -> body -> items -> each Item, and collect any u64 field whose
%% position is not 8-byte aligned.
u64_alignment_violations(B) ->
    Root = u32(B, 0),
    %% Envelope field 2 = body (field 1 is the union type).
    Body = deref(B, field(B, Root, 2)),
    %% Bulk field 1 = items.
    Vec = deref(B, field(B, Body, 1)),
    Count = u32(B, Vec),
    lists:flatmap(
        fun(I) ->
            Table = deref(B, Vec + 4 + I * 4),
            %% Item fields 2 and 3 = a and b.
            [
                {element, I, field, F, at, P}
             || F <- [2, 3], P <- [field(B, Table, F)], P rem 8 =/= 0
            ]
        end,
        lists:seq(0, Count - 1)
    ).

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
