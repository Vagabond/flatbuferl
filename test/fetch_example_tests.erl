-module(fetch_example_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Example: Game State with Players, Monsters, and Inventory
%%
%% This module demonstrates the fetch/2 API for path-based FlatBuffer access.
%% =============================================================================

%% Create a sample game state for testing
game_state_ctx() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Eldoria">>,
        tick => 123456,
        players => [
            #{
                name => <<"Alice">>,
                pos => #{x => 10.0, y => 0.0, z => 20.0},
                stats => #{hp => 95, max_hp => 100, level => 15, strength => 25, defense => 18},
                inventory => [
                    #{
                        item_type => 'Weapon',
                        item => #{name => <<"Excalibur">>, damage => 50, rarity => 'Legendary'},
                        quantity => 1
                    },
                    #{
                        item_type => 'Armor',
                        item => #{name => <<"Dragon Scale">>, defense => 30, rarity => 'Epic'},
                        quantity => 1
                    },
                    #{
                        item_type => 'Consumable',
                        item => #{
                            name => <<"Health Potion">>, effect => <<"Restore 50 HP">>, charges => 3
                        },
                        quantity => 5
                    }
                ],
                gold => 1500,
                guild => <<"Knights of Dawn">>
            },
            #{
                name => <<"Bob">>,
                pos => #{x => -5.0, y => 0.0, z => 15.0},
                stats => #{hp => 80, max_hp => 80, level => 10, strength => 18, defense => 22},
                inventory => [
                    #{
                        item_type => 'Weapon',
                        item => #{name => <<"Steel Sword">>, damage => 25, rarity => 'Uncommon'},
                        quantity => 1
                    },
                    #{
                        item_type => 'Consumable',
                        item => #{
                            name => <<"Mana Potion">>, effect => <<"Restore 30 MP">>, charges => 2
                        },
                        quantity => 3
                    }
                ],
                gold => 500,
                guild => <<"Knights of Dawn">>
            },
            #{
                name => <<"Charlie">>,
                pos => #{x => 100.0, y => 5.0, z => 100.0},
                stats => #{hp => 120, max_hp => 120, level => 20, strength => 30, defense => 25},
                inventory => [
                    #{
                        item_type => 'Weapon',
                        item => #{name => <<"Shadowblade">>, damage => 45, rarity => 'Epic'},
                        quantity => 1
                    }
                ],
                gold => 3000,
                guild => <<"Shadow Guild">>
            }
        ],
        monsters => [
            #{
                name => <<"Goblin Scout">>,
                pos => #{x => 50.0, y => 0.0, z => 50.0},
                stats => #{hp => 30, max_hp => 30, level => 3, strength => 8, defense => 5},
                loot_table => [
                    #{
                        item_type => 'Consumable',
                        item => #{
                            name => <<"Minor Potion">>, effect => <<"Restore 10 HP">>, charges => 1
                        },
                        quantity => 1
                    }
                ],
                aggro_radius => 10.0,
                is_boss => false
            },
            #{
                name => <<"Dragon Lord">>,
                pos => #{x => 200.0, y => 50.0, z => 200.0},
                stats => #{hp => 5000, max_hp => 5000, level => 50, strength => 100, defense => 80},
                loot_table => [
                    #{
                        item_type => 'Weapon',
                        item => #{name => <<"Dragon Fang">>, damage => 75, rarity => 'Legendary'},
                        quantity => 1
                    },
                    #{
                        item_type => 'Armor',
                        item => #{name => <<"Dragon Heart">>, defense => 50, rarity => 'Legendary'},
                        quantity => 1
                    }
                ],
                aggro_radius => 100.0,
                is_boss => true
            }
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    flatbuferl:new(Buffer, Schema).

%% =============================================================================
%% Basic Field Access
%% =============================================================================

basic_field_test() ->
    Ctx = game_state_ctx(),
    %% Simple field access
    ?assertEqual(<<"Eldoria">>, flatbuferl_fetch:fetch(Ctx, [world_name])),
    ?assertEqual(123456, flatbuferl_fetch:fetch(Ctx, [tick])).

%% =============================================================================
%% Vector Indexing
%% =============================================================================

vector_index_test() ->
    Ctx = game_state_ctx(),
    %% First player's name
    ?assertEqual(<<"Alice">>, flatbuferl_fetch:fetch(Ctx, [players, 0, name])),
    %% Last player (negative index)
    ?assertEqual(<<"Charlie">>, flatbuferl_fetch:fetch(Ctx, [players, -1, name])),
    %% Second monster
    ?assertEqual(<<"Dragon Lord">>, flatbuferl_fetch:fetch(Ctx, [monsters, 1, name])).

%% =============================================================================
%% Nested Struct Access
%% =============================================================================

nested_struct_test() ->
    Ctx = game_state_ctx(),
    %% Access nested struct fields
    ?assertEqual(10.0, flatbuferl_fetch:fetch(Ctx, [players, 0, pos, x])),
    ?assertEqual(20.0, flatbuferl_fetch:fetch(Ctx, [players, 0, pos, z])),
    %% Nested table field
    ?assertEqual(95, flatbuferl_fetch:fetch(Ctx, [players, 0, stats, hp])),
    ?assertEqual(15, flatbuferl_fetch:fetch(Ctx, [players, 0, stats, level])).

%% =============================================================================
%% Wildcards - Iterate All Elements
%% =============================================================================

wildcard_test() ->
    Ctx = game_state_ctx(),
    %% All player names
    ?assertEqual(
        [<<"Alice">>, <<"Bob">>, <<"Charlie">>],
        flatbuferl_fetch:fetch(Ctx, [players, '*', name])
    ),
    %% All player levels
    ?assertEqual(
        [15, 10, 20],
        flatbuferl_fetch:fetch(Ctx, [players, '*', stats, level])
    ),
    %% All monster boss flags
    ?assertEqual(
        [false, true],
        flatbuferl_fetch:fetch(Ctx, [monsters, '*', is_boss])
    ).

%% =============================================================================
%% Multi-Field Extraction
%% =============================================================================

multi_field_test() ->
    Ctx = game_state_ctx(),
    %% Extract multiple fields at once
    ?assertEqual(
        [<<"Alice">>, 1500],
        flatbuferl_fetch:fetch(Ctx, [players, 0, [name, gold]])
    ),
    %% Extract from all players
    ?assertEqual(
        [[<<"Alice">>, 15], [<<"Bob">>, 10], [<<"Charlie">>, 20]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [name, [stats, level]]])
    ).

%% =============================================================================
%% Nested Paths in Extraction
%% =============================================================================

nested_extraction_test() ->
    Ctx = game_state_ctx(),
    %% Extract nested paths
    ?assertEqual(
        [<<"Alice">>, 10.0, 20.0],
        flatbuferl_fetch:fetch(Ctx, [players, 0, [name, [pos, x], [pos, z]]])
    ),
    %% Extract multiple nested stats
    ?assertEqual(
        [[95, 100], [80, 80], [120, 120]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [[stats, hp], [stats, max_hp]]])
    ).

%% =============================================================================
%% _size Pseudo-field
%% =============================================================================

size_test() ->
    Ctx = game_state_ctx(),
    %% Number of players
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [players, '_size'])),
    %% Number of monsters
    ?assertEqual(2, flatbuferl_fetch:fetch(Ctx, [monsters, '_size'])),
    %% Inventory size per player (with name for context)
    ?assertEqual(
        [[<<"Alice">>, 3], [<<"Bob">>, 2], [<<"Charlie">>, 1]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [name, [inventory, '_size']]])
    ),
    %% String length

    %% "Alice" = 5 bytes
    ?assertEqual(5, flatbuferl_fetch:fetch(Ctx, [players, 0, name, '_size'])).

%% =============================================================================
%% Equality Guards - Filter Elements
%% =============================================================================

equality_guard_test() ->
    Ctx = game_state_ctx(),
    %% Find players in a specific guild
    ?assertEqual(
        [[<<"Alice">>], [<<"Bob">>]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{guild, <<"Knights of Dawn">>}, name]])
    ),
    %% Find boss monsters
    ?assertEqual(
        [[<<"Dragon Lord">>]],
        flatbuferl_fetch:fetch(Ctx, [monsters, '*', [{is_boss, true}, name]])
    ).

%% =============================================================================
%% Comparison Guards
%% =============================================================================

comparison_guard_test() ->
    Ctx = game_state_ctx(),
    %% Players with level > 12
    ?assertEqual(
        [[<<"Alice">>, 15], [<<"Charlie">>, 20]],
        flatbuferl_fetch:fetch(Ctx, [
            players, '*', [{[stats, level], '>', 12}, name, [stats, level]]
        ])
    ),
    %% Players with gold >= 1000
    ?assertEqual(
        [[<<"Alice">>, 1500], [<<"Charlie">>, 3000]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{gold, '>=', 1000}, name, gold]])
    ),
    %% Monsters with hp between 100 and 10000
    ?assertEqual(
        [[<<"Dragon Lord">>, 5000]],
        flatbuferl_fetch:fetch(Ctx, [
            monsters, '*', [{[stats, hp], '>', 100}, {[stats, hp], '<', 10000}, name, [stats, hp]]
        ])
    ),
    %% Players with level =< 15 (tests the =< operator)
    ?assertEqual(
        [[<<"Alice">>, 15], [<<"Bob">>, 10]],
        flatbuferl_fetch:fetch(Ctx, [
            players, '*', [{[stats, level], '=<', 15}, name, [stats, level]]
        ])
    ),
    %% Players with exact gold == 1500
    ?assertEqual(
        [[<<"Alice">>]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{gold, '==', 1500}, name]])
    ),
    %% Players with gold /= 1500
    ?assertEqual(
        [[<<"Bob">>], [<<"Charlie">>]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{gold, '/=', 1500}, name]])
    ).

%% =============================================================================
%% Membership Guards (in / not_in)
%% =============================================================================

membership_guard_test() ->
    Ctx = game_state_ctx(),
    %% Players in specific guilds
    ?assertEqual(
        [[<<"Charlie">>]],
        flatbuferl_fetch:fetch(Ctx, [
            players, '*', [{guild, in, [<<"Shadow Guild">>, <<"Dark Brotherhood">>]}, name]
        ])
    ),
    %% Players NOT in Shadow Guild
    ?assertEqual(
        [[<<"Alice">>], [<<"Bob">>]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{guild, not_in, [<<"Shadow Guild">>]}, name]])
    ).

%% =============================================================================
%% Unions and _type Pseudo-field
%% =============================================================================

union_test() ->
    Ctx = game_state_ctx(),
    %% Get the type of first item in Alice's inventory
    ?assertEqual('Weapon', flatbuferl_fetch:fetch(Ctx, [players, 0, inventory, 0, item, '_type'])),
    %% Get type of all items in Alice's inventory
    ?assertEqual(
        ['Weapon', 'Armor', 'Consumable'],
        flatbuferl_fetch:fetch(Ctx, [players, 0, inventory, '*', item, '_type'])
    ).

%% =============================================================================
%% Union Type Guards
%% =============================================================================

union_type_guard_test() ->
    Ctx = game_state_ctx(),
    %% Find all weapons in Alice's inventory
    ?assertEqual(
        [[<<"Excalibur">>, 50]],
        flatbuferl_fetch:fetch(Ctx, [
            players, 0, inventory, '*', item, [{'_type', 'Weapon'}, name, damage]
        ])
    ),
    %% Find all consumables across all players
    AllConsumables = flatbuferl_fetch:fetch(Ctx, [
        players, '*', inventory, '*', item, [{'_type', 'Consumable'}, name]
    ]),
    %% Flatten nested results - each player's consumables are in a sublist
    ?assertEqual(
        [[[<<"Health Potion">>]], [[<<"Mana Potion">>]], []],
        AllConsumables
    ).

%% =============================================================================
%% Complex Queries
%% =============================================================================

complex_query_test() ->
    Ctx = game_state_ctx(),
    %% Find high-level players (level > 12) with their position and HP percentage
    HighLevelPlayers = flatbuferl_fetch:fetch(Ctx, [
        players,
        '*',
        [
            {[stats, level], '>', 12},
            name,
            [pos, x],
            [pos, z],
            [stats, hp],
            [stats, max_hp]
        ]
    ]),
    ?assertEqual(
        [
            [<<"Alice">>, 10.0, 20.0, 95, 100],
            [<<"Charlie">>, 100.0, 100.0, 120, 120]
        ],
        HighLevelPlayers
    ),

    %% Find legendary items from boss loot tables
    BossLoot = flatbuferl_fetch:fetch(Ctx, [
        monsters,
        '*',
        [
            {is_boss, true},
            name,
            [loot_table, '_size']
        ]
    ]),
    ?assertEqual([[<<"Dragon Lord">>, 2]], BossLoot).

%% =============================================================================
%% Struct Wildcard
%% =============================================================================

struct_wildcard_test() ->
    Ctx = game_state_ctx(),
    %% Get entire position struct as map
    Pos = flatbuferl_fetch:fetch(Ctx, [players, 0, pos, '*']),
    ?assertEqual(#{x => 10.0, y => 0.0, z => 20.0}, Pos),
    %% Get entire stats table as map
    Stats = flatbuferl_fetch:fetch(Ctx, [players, 0, stats, '*']),
    ?assertEqual(#{hp => 95, max_hp => 100, level => 15, strength => 25, defense => 18}, Stats).

%% =============================================================================
%% Error Paths
%% =============================================================================

error_unknown_field_test() ->
    Ctx = game_state_ctx(),
    ?assertError({unknown_field, nonexistent}, flatbuferl_fetch:fetch(Ctx, [nonexistent])),
    ?assertError({unknown_field, bogus}, flatbuferl_fetch:fetch(Ctx, [players, 0, bogus])).

error_not_a_table_string_test() ->
    Ctx = game_state_ctx(),
    %% Trying to traverse into a string field raises not_a_table
    ?assertError({not_a_table, world_name, string}, flatbuferl_fetch:fetch(Ctx, [world_name, 0])),
    ?assertError(
        {not_a_table, world_name, string}, flatbuferl_fetch:fetch(Ctx, [world_name, something])
    ).

error_not_a_table_test() ->
    Ctx = game_state_ctx(),
    %% Trying to traverse into a scalar field
    ?assertError({not_a_table, tick, ulong}, flatbuferl_fetch:fetch(Ctx, [tick, something])).

error_index_on_table_test() ->
    Ctx = game_state_ctx(),
    %% Trying to use integer index directly on a table (terminal position)
    ?assertError({not_a_vector, 'GameState', 0}, flatbuferl_fetch:fetch(Ctx, [0])).

%% =============================================================================
%% Missing Fields and Out-of-Bounds
%% =============================================================================

missing_field_test() ->
    Ctx = game_state_ctx(),
    %% Out of bounds index returns undefined
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, 100])),
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, 100, name])),
    %% Negative out of bounds
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, -100])).

empty_wildcard_result_test() ->
    Ctx = game_state_ctx(),
    %% Wildcard with guard that matches nothing returns empty list
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{gold, '>', 999999}, name]])
    ).

%% =============================================================================
%% Additional Edge Cases for Coverage
%% =============================================================================

negative_index_test() ->
    Ctx = game_state_ctx(),
    %% Negative index from end of vector
    ?assertEqual(<<"Charlie">>, flatbuferl_fetch:fetch(Ctx, [players, -1, name])),
    ?assertEqual(<<"Bob">>, flatbuferl_fetch:fetch(Ctx, [players, -2, name])),
    ?assertEqual(<<"Alice">>, flatbuferl_fetch:fetch(Ctx, [players, -3, name])).

union_vector_negative_index_test() ->
    Ctx = game_state_ctx(),
    %% Negative index into inventory (vector containing unions)
    ?assertEqual(
        <<"Health Potion">>,
        flatbuferl_fetch:fetch(Ctx, [players, 0, inventory, -1, item, name])
    ).

error_invalid_path_element_test() ->
    Ctx = game_state_ctx(),
    %% Invalid path element (not atom, integer, '*', or list)
    ?assertError(
        {invalid_path_element, {bad, tuple}},
        flatbuferl_fetch:fetch(Ctx, [players, {bad, tuple}])
    ).

error_cannot_traverse_type_test() ->
    Ctx = game_state_ctx(),
    %% Cannot traverse past '_type' pseudo-field
    ?assertError(
        {cannot_traverse, '_type', [something]},
        flatbuferl_fetch:fetch(Ctx, [players, 0, inventory, 0, item, '_type', something])
    ).

missing_nested_table_test() ->
    %% Create a context with missing nested table
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    %% Player without stats (stats is optional)
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [
            #{name => <<"NoStats">>, gold => 100}
        ],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Accessing missing nested table returns undefined
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, 0, stats, hp])).

missing_vector_test() ->
    %% Create a context with missing vector
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Empty vector returns empty list for wildcard
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [players, '*', name])),
    %% Empty vector returns 0 for _size
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [players, '_size'])).

%% =============================================================================
%% Fixed Array Tests
%% =============================================================================

array_size_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [1, 2, 3, 4], bytes => [1, 2]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% _size on fixed arrays returns the count
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [floats, '_size'])),
    ?assertEqual(4, flatbuferl_fetch:fetch(Ctx, [ints, '_size'])).

array_wildcard_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [10, 20, 30, 40], bytes => [1, 2]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Wildcard over fixed array
    ?assertEqual([1.0, 2.0, 3.0], flatbuferl_fetch:fetch(Ctx, [floats, '*'])),
    ?assertEqual([10, 20, 30, 40], flatbuferl_fetch:fetch(Ctx, [ints, '*'])).

array_negative_index_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [10, 20, 30, 40], bytes => [1, 2]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Negative index from end
    ?assertEqual(3.0, flatbuferl_fetch:fetch(Ctx, [floats, -1])),
    ?assertEqual(40, flatbuferl_fetch:fetch(Ctx, [ints, -1])).

%% =============================================================================
%% Struct Path Tests
%% =============================================================================

struct_multi_field_extraction_test() ->
    Ctx = game_state_ctx(),
    %% Multi-field extraction from struct
    ?assertEqual(
        [10.0, 20.0],
        flatbuferl_fetch:fetch(Ctx, [players, 0, pos, [x, z]])
    ).

struct_missing_field_test() ->
    Ctx = game_state_ctx(),
    %% Accessing non-existent field in struct returns undefined
    %% Note: structs have fixed fields, so this tests the error path
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, 0, pos, w])).

%% =============================================================================
%% Union Edge Cases
%% =============================================================================

union_traverse_deeper_test() ->
    Ctx = game_state_ctx(),
    %% Traverse deeper into union value (tests continue_from_union_element -> do_fetch)
    %% Get the charges field from a consumable
    ?assertEqual(
        3,
        flatbuferl_fetch:fetch(Ctx, [players, 0, inventory, 2, item, charges])
    ).

union_wildcard_with_nested_path_test() ->
    Ctx = game_state_ctx(),
    %% Wildcard over unions with continuation into nested fields
    %% This tests the recursive wildcard_over_union_vector paths
    %% Filter to Weapons only and extract name + damage
    WeaponDamages = flatbuferl_fetch:fetch(Ctx, [
        players, 0, inventory, '*', item, [{'_type', 'Weapon'}, name, damage]
    ]),
    ?assertEqual([[<<"Excalibur">>, 50]], WeaponDamages).

%% =============================================================================
%% More Error Paths
%% =============================================================================

error_index_mid_path_test() ->
    Ctx = game_state_ctx(),
    %% Integer index in middle of path on a non-vector (table)
    %% After players, 0 we're at a Player table, can't use another integer
    ?assertError({not_a_vector, 'Player', 5}, flatbuferl_fetch:fetch(Ctx, [players, 0, 5])).

error_traverse_scalar_in_vector_test() ->
    Ctx = game_state_ctx(),
    %% Try to traverse into scalar elements of a vector
    %% gold is an int, can't traverse into it
    ?assertError(
        {not_a_table, gold, int}, flatbuferl_fetch:fetch(Ctx, [players, 0, gold, something])
    ).

%% =============================================================================
%% Guard Edge Cases
%% =============================================================================

guard_on_missing_field_test() ->
    %% Guard on field that exists in schema but is missing in data - should filter out
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [
            #{name => <<"HasGuild">>, gold => 100, guild => <<"TestGuild">>},
            %% guild field missing
            #{name => <<"NoGuild">>, gold => 200}
        ],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Guard on guild field - only player with guild should match
    ?assertEqual(
        [[<<"HasGuild">>]],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{guild, <<"TestGuild">>}, name]])
    ).

guard_nested_path_missing_test() ->
    %% Guard on nested path where intermediate is missing
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [#{name => <<"NoStats">>, gold => 100}],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Guard on [stats, hp] where stats is missing
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{[stats, hp], '>', 50}, name]])
    ).

%% =============================================================================
%% Union NONE Type Tests
%% =============================================================================

union_none_test() ->
    %% Create data with union field not set (NONE)
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_field.fbs"),
    %% command_root with data union not set
    Data = #{additions_value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Accessing union type when NONE
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, '_type'])),
    %% Accessing union value when NONE
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, salute])).

union_none_with_extraction_test() ->
    %% Union NONE with extraction spec
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_field.fbs"),
    Data = #{additions_value => 42},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Extraction spec on NONE union
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, ['_type', salute]])).

%% =============================================================================
%% Missing Union Vector Tests
%% =============================================================================

missing_union_vector_test() ->
    %% Create data with union vector field not set
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Wildcard on missing union vector
    ?assertEqual([], flatbuferl_fetch:fetch(Ctx, [data, '*'])),
    %% _size on missing union vector
    ?assertEqual(0, flatbuferl_fetch:fetch(Ctx, [data, '_size'])),
    %% Index on missing union vector
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, 0])).

union_vector_negative_index_out_of_bounds_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData'],
        data => [#{data => [<<"hello">>]}]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Out of bounds negative index
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [data, -10])),
    %% Valid negative index
    ?assertEqual('StringData', flatbuferl_fetch:fetch(Ctx, [data, -1, '_type'])).

union_vector_invalid_path_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData'],
        data => [#{data => [<<"hello">>]}]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Invalid path element on union vector
    ?assertError({invalid_path_element, {bad}}, flatbuferl_fetch:fetch(Ctx, [data, {bad}])).

%% =============================================================================
%% Additional Array Edge Cases
%% =============================================================================

array_out_of_bounds_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [1, 2, 3, 4], bytes => [1, 2]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Out of bounds index on array
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [floats, 100])),
    %% Out of bounds negative index
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [floats, -100])).

array_invalid_path_test() ->
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/array_table.fbs"),
    Data = #{floats => [1.0, 2.0, 3.0], ints => [1, 2, 3, 4], bytes => [1, 2]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Invalid path element
    ?assertError(
        {invalid_path_element, <<"bad">>}, flatbuferl_fetch:fetch(Ctx, [floats, <<"bad">>])
    ).

%% =============================================================================
%% Union Vector Wildcard Return Types
%% =============================================================================

union_vector_wildcard_list_result_test() ->
    %% Test wildcard over union vector returning nested lists
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData', 'IntData'],
        data => [
            #{data => [<<"a">>, <<"b">>]},
            #{data => [1, 2, 3]}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Wildcard into union vector elements (data field)
    Result = flatbuferl_fetch:fetch(Ctx, [data, '*', data, '*']),
    ?assertEqual([[<<"a">>, <<"b">>], [1, 2, 3]], Result).

union_vector_continue_to_map_test() ->
    %% Test wildcard over union vector returning maps
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData', 'IntData'],
        data => [
            #{data => [<<"hello">>]},
            #{data => [42]}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Wildcard with no continuation - returns maps
    Result = flatbuferl_fetch:fetch(Ctx, [data, '*']),
    ?assertEqual(2, length(Result)),
    ?assert(is_map(hd(Result))).

%% =============================================================================
%% Nested Path Guard Additional Tests
%% =============================================================================

nested_path_guard_equality_not_match_test() ->
    Ctx = game_state_ctx(),
    %% Guard on nested path where value doesn't match expected
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{[stats, level], 999}, name]])
    ).

nested_path_guard_comparison_missing_test() ->
    %% Comparison guard where field is missing
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [#{name => <<"NoStats">>, gold => 100}],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Comparison guard on missing nested field
    ?assertEqual(
        [],
        flatbuferl_fetch:fetch(Ctx, [players, '*', [{[stats, hp], '>=', 50}, name]])
    ).

%% =============================================================================
%% Extraction Subpath Tests
%% =============================================================================

extraction_subpath_returns_list_test() ->
    Ctx = game_state_ctx(),
    %% Extract a subpath that returns a list (wildcard result)
    Result = flatbuferl_fetch:fetch(Ctx, [players, 0, [name, [inventory, '*', quantity]]]),
    ?assertEqual([<<"Alice">>, [1, 1, 5]], Result).

extraction_subpath_missing_test() ->
    %% Extract a subpath where intermediate is missing
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [#{name => <<"NoStats">>, gold => 100}],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Extraction with missing subpath
    ?assertEqual(
        [<<"NoStats">>, undefined], flatbuferl_fetch:fetch(Ctx, [players, 0, [name, [stats, hp]]])
    ).

%% =============================================================================
%% Error Path: Index Mid Path On Table (line 108)
%% =============================================================================

error_index_mid_path_on_table_test() ->
    Ctx = game_state_ctx(),
    %% Integer index in traversal position on a table
    %% This goes through fetch_traverse with integer Index
    ?assertError(
        {not_a_vector, index_on_table}, flatbuferl_fetch:fetch(Ctx, [players, 0, 5, name])
    ).

%% =============================================================================
%% Struct Nested Traversal (lines 425-431)
%% =============================================================================

struct_nested_missing_test() ->
    Ctx = game_state_ctx(),
    %% Try to traverse into non-existent nested field in struct
    %% pos.w doesn't exist, then try to continue
    ?assertEqual(undefined, flatbuferl_fetch:fetch(Ctx, [players, 0, pos, w, something])).

struct_nested_not_a_struct_test() ->
    Ctx = game_state_ctx(),
    %% Try to traverse deeper when field is not a struct
    %% pos.x is a float, can't traverse into it
    ?assertError({not_a_struct, x}, flatbuferl_fetch:fetch(Ctx, [players, 0, pos, x, something])).

%% =============================================================================
%% Additional Coverage Tests
%% =============================================================================

extraction_missing_field_test() ->
    %% Extract a field that doesn't exist - goes to extract_one -> fetch_field -> missing
    %% guild field might be missing on some players
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [#{name => <<"NoGuild">>, gold => 100}],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx2 = flatbuferl:new(Buffer, Schema),
    %% Extract guild which is missing - should return undefined in extraction list
    ?assertEqual(
        [<<"NoGuild">>, undefined], flatbuferl_fetch:fetch(Ctx2, [players, 0, [name, guild]])
    ).

nested_path_guard_value_mismatch_test() ->
    Ctx = game_state_ctx(),
    %% Nested path equality guard where value exists but doesn't match
    Result = flatbuferl_fetch:fetch(Ctx, [players, '*', [{[stats, hp], 1}, name]]),
    ?assertEqual([], Result).

nested_path_comparison_value_mismatch_test() ->
    Ctx = game_state_ctx(),
    %% Nested path comparison guard where comparison fails
    Result = flatbuferl_fetch:fetch(Ctx, [players, '*', [{[stats, hp], '<', 1}, name]]),
    ?assertEqual([], Result).

union_vector_wildcard_unknown_field_filter_test() ->
    %% Test that wildcard over union vector filters out unknown fields
    %% StringData and IntData have different field names
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/union_vector.fbs"),
    Data = #{
        data_type => ['StringData', 'IntData'],
        data => [
            #{data => [<<"hello">>]},
            #{data => [42]}
        ]
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Both StringData and IntData have 'data' field, so this works
    %% but if we had different fields, they would be filtered
    Result = flatbuferl_fetch:fetch(Ctx, [data, '*', data]),
    ?assertEqual([[<<"hello">>], [42]], Result).

regular_vector_wildcard_with_missing_test() ->
    %% Wildcard over vector where some elements return missing
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [
            #{name => <<"WithGuild">>, gold => 100, guild => <<"TestGuild">>},
            #{name => <<"NoGuild">>, gold => 200}
        ],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Wildcard accessing guild - missing guilds should be filtered out
    Result = flatbuferl_fetch:fetch(Ctx, [players, '*', guild]),
    ?assertEqual([<<"TestGuild">>], Result).

struct_extraction_error_undefined_test() ->
    Ctx = game_state_ctx(),
    %% Extraction from struct where field doesn't exist
    %% Vec3 only has x, y, z - asking for 'w' should give undefined
    Result = flatbuferl_fetch:fetch(Ctx, [players, 0, pos, [x, y, w]]),
    ?assertEqual([10.0, 0.0, undefined], Result).

struct_nested_traversal_test() ->
    %% Test fetch_from_struct with nested struct traversal
    %% This needs a struct containing another struct
    %% Vec3 is a simple struct, but stats is a table
    Ctx = game_state_ctx(),
    %% Access through struct extraction
    Result = flatbuferl_fetch:fetch(Ctx, [players, 0, [name, [pos, '*']]]),
    ?assertEqual([<<"Alice">>, #{x => 10.0, y => 0.0, z => 20.0}], Result).

vector_element_scalar_continue_test() ->
    %% Test continue_from_element where element is a scalar (not table)
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/int_vector.fbs"),
    Data = #{int_vector => [1, 2, 3, 4, 5]},
    Buffer = iolist_to_binary(flatbuferl:from_map(Data, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Just accessing element - hits continue_from_element with scalar
    ?assertEqual(3, flatbuferl_fetch:fetch(Ctx, [int_vector, 2])).

wildcard_over_vector_missing_continuation_test() ->
    %% Test wildcard where continuation returns missing for some elements
    %% This tests the 'missing' branch in wildcard_over_vector
    {ok, Schema} = flatbuferl:parse_schema_file("test/schemas/game_example.fbs"),
    GameState = #{
        world_name => <<"Test">>,
        tick => 1,
        players => [
            #{
                name => <<"P1">>,
                gold => 100,
                stats => #{hp => 50, max_hp => 100, level => 5, strength => 10, defense => 8}
            },
            #{name => <<"P2">>, gold => 200}
        ],
        monsters => []
    },
    Buffer = iolist_to_binary(flatbuferl:from_map(GameState, Schema)),
    Ctx = flatbuferl:new(Buffer, Schema),
    %% Access stats.hp with wildcard - P2 has no stats, should be filtered
    Result = flatbuferl_fetch:fetch(Ctx, [players, '*', stats, hp]),
    ?assertEqual([50], Result).
