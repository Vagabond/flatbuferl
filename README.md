# flatbuferl

A FlatBuffers implementation in Erlang.

No dependencies beyond kernel and stdlib. Schemas are parsed at runtime without code generation. Encoding produces iolists and decoding returns sub-binaries, both avoiding unnecessary copies.

## Usage

Schema file:
```
table Vec3 {
  x: float;
  y: float;
  z: float;
}

table Entity {
  name: string;
  pos: Vec3;
  hp: int = 100;
  items: [string];
}

root_type Entity;
```

Parsing the schema:
```erlang
{ok, Schema} = flatbuferl:parse_schema_file("entity.fbs").
%% or inline
{ok, Schema} = flatbuferl:parse_schema("table Monster { name: string; hp: int = 100; } root_type Monster;").
```

Encoding data:
```erlang
Data = #{
    name => <<"Player">>,
    pos => #{x => 1.5, y => 2.5, z => 3.5},
    hp => 200,
    items => [<<"sword">>, <<"shield">>]
},
Iodata = flatbuferl:from_map(Data, Schema),
Buffer = iolist_to_binary(Iodata).
```

Full decode with `to_map`:
```erlang
Ctx = flatbuferl:new(Buffer, Schema),
Map = flatbuferl:to_map(Ctx).
%% #{name => <<"Player">>, pos => #{x => 1.5, y => 2.5, z => 3.5}, hp => 200, items => [...]}
```

Partial decode with `get` - seeks into the buffer and only deserializes the requested path:
```erlang
Ctx = flatbuferl:new(Buffer, Schema),

%% Single field
flatbuferl:get(Ctx, [name]).              %% <<"Player">>
flatbuferl:get(Ctx, [hp]).                %% 200

%% Nested table field
flatbuferl:get(Ctx, [pos, x]).            %% 1.5
flatbuferl:get(Ctx, [pos, y]).            %% 2.5

%% Vector field (returns full list)
flatbuferl:get(Ctx, [items]).             %% [<<"sword">>, <<"shield">>]

%% Missing field returns schema default
flatbuferl:get(Ctx, [hp]).                %% 100 if hp not in buffer (schema default)

%% Check if field is present
flatbuferl:has(Ctx, [name]).              %% true
flatbuferl:has(Ctx, [pos]).               %% true or false
```

Advanced path-based queries with `fetch` - supports vector indexing, wildcards, multi-field extraction, and filtering. Use `get` for simple field access with default handling; use `fetch` when you need to index into vectors or query across collections:
```erlang
Ctx = flatbuferl:new(Buffer, Schema),

%% Vector indexing (positive and negative)
flatbuferl_fetch:fetch(Ctx, [items, 0]).        %% <<"sword">>
flatbuferl_fetch:fetch(Ctx, [items, -1]).       %% <<"shield">> (last element)
flatbuferl_fetch:fetch(Ctx, [items, 100]).      %% undefined (out of bounds)

%% Wildcards - iterate all elements (always returns a list)
flatbuferl_fetch:fetch(Ctx, [items, '*']).      %% [<<"sword">>, <<"shield">>]

%% Multi-field extraction
flatbuferl_fetch:fetch(Ctx, [pos, [x, y]]).     %% [1.5, 2.5]

%% Size pseudo-field
flatbuferl_fetch:fetch(Ctx, [items, '_size']).  %% 2
flatbuferl_fetch:fetch(Ctx, [name, '_size']).   %% 6 (string byte length)
```

For vectors of tables, wildcards and extraction combine for powerful queries:
```erlang
%% Schema: table Monster { name: string; hp: int; is_boss: bool; }
%%         table Game { monsters: [Monster]; }

%% Get all monster names
flatbuferl_fetch:fetch(Ctx, [monsters, '*', name]).
%% [<<"Goblin">>, <<"Orc">>, <<"Dragon">>]

%% Get name and hp for each monster
flatbuferl_fetch:fetch(Ctx, [monsters, '*', [name, hp]]).
%% [[<<"Goblin">>, 10], [<<"Orc">>, 30], [<<"Dragon">>, 500]]

%% Filter with guards - only bosses
flatbuferl_fetch:fetch(Ctx, [monsters, '*', [{is_boss, true}, name]]).
%% [[<<"Dragon">>]]

%% Comparison guards
flatbuferl_fetch:fetch(Ctx, [monsters, '*', [{hp, '>', 20}, name]]).
%% [[<<"Orc">>], [<<"Dragon">>]]
```

Union type handling:
```erlang
%% Schema: union Item { Weapon, Armor, Consumable }
%%         table Player { equipped: Item; inventory: [Item]; }

%% Get union type discriminator
flatbuferl_fetch:fetch(Ctx, [equipped, '_type']).     %% 'Weapon'

%% Access fields (auto-resolves union type)
flatbuferl_fetch:fetch(Ctx, [equipped, name]).        %% <<"Sword">>

%% Filter union vectors by type
flatbuferl_fetch:fetch(Ctx, [inventory, '*', [{'_type', 'Weapon'}, name, damage]]).
%% [[<<"Sword">>, 10], [<<"Axe">>, 25]]
```

See `doc/flatbuferl_fetch.html` for complete documentation on path syntax, guards, and error conditions.

Validating data before encoding:
```erlang
%% Validate a map against the schema
ok = flatbuferl:validate(Data, Schema).

%% With invalid data
{error, [{type_mismatch, hp, int, <<"not an int">>}]} =
    flatbuferl:validate(#{hp => <<"not an int">>}, Schema).

%% Strict mode - error on unknown fields
{error, [{unknown_field, bad_field}]} =
    flatbuferl:validate(#{bad_field => 1}, Schema, #{unknown_fields => error}).
```

## API

```erlang
flatbuferl:parse_schema(String | Binary) -> {ok, Schema} | {error, Reason}.
flatbuferl:parse_schema_file(Filename) -> {ok, Schema} | {error, Reason}.

flatbuferl:new(Buffer, Schema) -> Ctx.
flatbuferl:get(Ctx, Path) -> Value.           %% Path = [atom()], errors on missing
flatbuferl:get(Ctx, Path, Default) -> Value.  %% returns Default if missing
flatbuferl:has(Ctx, Path) -> boolean().
flatbuferl_fetch:fetch(Ctx, Path) -> Value.   %% advanced paths with indexing/wildcards/guards
flatbuferl:to_map(Ctx) -> Map.
flatbuferl:to_map(Ctx, Opts) -> Map.
flatbuferl:from_map(Map, Schema) -> iodata().
flatbuferl:from_map(Map, Schema, Opts) -> iodata().
flatbuferl:validate(Map, Schema) -> ok | {error, [ValidationError]}.
flatbuferl:validate(Map, Schema, Opts) -> ok | {error, [ValidationError]}.
flatbuferl:file_id(Ctx | Buffer) -> <<_:32>>.
flatbuferl:get_bytes(Ctx, Path) -> binary().
```

Decode options:

```erlang
#{deprecated => skip | allow | error}  %% skip: ignore deprecated fields (default)
                                       %% allow: parse deprecated fields
                                       %% error: fail to decode messages containing deprecated fields
```

Encode options:

```erlang
#{
    file_id => true | false | <<_:32>>,  %% true: use schema's file_identifier (default)
                                         %% false: omit file identifier
                                         %% <<_:32>>: replace with custom 4-byte identifier
    deprecated => skip | allow | error   %% skip: do not encode deprecated fields (default)
                                         %% allow: encode deprecated fields, if supplied
                                         %% error: fail to encode if deprecated fields are present
}
```

Validation options:

```erlang
#{unknown_fields => ignore | error}  %% ignore: allow fields not in schema (default)
                                     %% error: return error for unknown fields
```

## Supported Types

Scalars:
 * bool
 * byte/int8
 * ubyte/uint8
 * short/int16
 * ushort/uint16
 * int/int32
 * uint/uint32
 * long/int64
 * ulong/uint64
 * float/float32
 * double/float64

Compound:
 * string (binary)
 * vectors (list)
 * tables (map)
 * structs (map)
 * enums (atom)
 * unions

## Feature Support

Buffers are binary-compatible with flatc. Unsupported features will fail at schema parse time or be silently ignored, but supported features should produce identical wire format.

Supported:
- Tables, structs, enums, unions, vectors of unions
- Namespaces, file identifiers, include directives
- Field defaults
- Attributes: `id`, `deprecated`, `required`
- Optional scalars (`field: int = null`)
- Fixed-size arrays (`[int:4]`)
- Struct alignment (automatic, based on field sizes)
- VTable deduplication within tables
- Map/JSON validation against schema

Not supported:
- `force_align` attribute
- `rpc_service`
- Flexbuffers

## Building

```
rebar3 compile
rebar3 eunit
```

## Acknowledgements

Schema parser/lexer and test schemas adapted from [wooga/eflatbuffers](https://github.com/wooga/eflatbuffers) (MIT).

Test schema `test/schemas/union_vector.fbs` adapted from [planus](https://github.com/planus-org/planus) (MIT/Apache-2.0).
