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
{ok, Schema} = flatbuferl_schema:parse_file("entity.fbs").
%% or inline
{ok, Schema} = flatbuferl_schema:parse("table Monster { name: string; hp: int = 100; } root_type Monster;").
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

## API

```erlang
flatbuferl_schema:parse(String | Binary) -> {ok, Schema} | {error, Reason}.
flatbuferl_schema:parse_file(Filename) -> {ok, Schema} | {error, Reason}.

flatbuferl:new(Buffer, Schema) -> Ctx.
flatbuferl:get(Ctx, Path) -> Value.
flatbuferl:get(Ctx, Path, Default) -> Value.
flatbuferl:has(Ctx, Path) -> boolean().
flatbuferl:to_map(Ctx) -> Map.
flatbuferl:to_map(Ctx, Opts) -> Map.
flatbuferl:from_map(Map, Schema) -> iodata().
flatbuferl:from_map(Map, Schema, Opts) -> iodata().
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
- Namespaces, file identifiers
- Field defaults
- Attributes: `id`, `deprecated`, `required`
- Struct alignment (automatic, based on field sizes)
- VTable deduplication within tables

Not supported:
- Fixed-size arrays (`[int:4]`)
- `force_align` attribute
- `include` directives (parsed but not processed)
- `rpc_service`
- Shared strings (strings are not deduplicated across the buffer)
- Flexbuffers

## Building

```
rebar3 compile
rebar3 eunit
```

## Acknowledgements

Schema parser/lexer and test schemas adapted from [wooga/eflatbuffers](https://github.com/wooga/eflatbuffers) (MIT).

Test schema `test/schemas/union_vector.fbs` adapted from [planus](https://github.com/planus-org/planus) (MIT/Apache-2.0).
