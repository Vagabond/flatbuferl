# erl_flatbuf

A FlatBuffers implementation in Erlang, derived from [wooga/eflatbuffers](https://github.com/wooga/eflatbuffers).

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
{ok, Schema} = schema:parse_file("entity.fbs").
%% or inline
{ok, Schema} = schema:parse("table Monster { name: string; hp: int = 100; } root_type Monster;").
```

Encoding data:
```erlang
Data = #{
    name => <<"Player">>,
    pos => #{x => 1.5, y => 2.5, z => 3.5},
    hp => 200,
    items => [<<"sword">>, <<"shield">>]
},
Iodata = eflatbuffers:from_map(Data, Schema),
Buffer = iolist_to_binary(Iodata).
```

Full decode with `to_map`:
```erlang
Ctx = eflatbuffers:new(Buffer, Schema),
Map = eflatbuffers:to_map(Ctx).
%% #{name => <<"Player">>, pos => #{x => 1.5, y => 2.5, z => 3.5}, hp => 200, items => [...]}
```

Partial decode with `get` - seeks into the buffer and only deserializes the requested path:
```erlang
Ctx = eflatbuffers:new(Buffer, Schema),

%% Single field
eflatbuffers:get(Ctx, [name]).              %% <<"Player">>
eflatbuffers:get(Ctx, [hp]).                %% 200

%% Nested table field
eflatbuffers:get(Ctx, [pos, x]).            %% 1.5
eflatbuffers:get(Ctx, [pos, y]).            %% 2.5

%% Vector field (returns full list)
eflatbuffers:get(Ctx, [items]).             %% [<<"sword">>, <<"shield">>]

%% Missing field returns schema default
eflatbuffers:get(Ctx, [hp]).                %% 100 if hp not in buffer (schema default)

%% Check if field is present
eflatbuffers:has(Ctx, [name]).              %% true
eflatbuffers:has(Ctx, [pos]).               %% true or false
```

## API

```erlang
schema:parse(String | Binary) -> {ok, Schema} | {error, Reason}.
schema:parse_file(Filename) -> {ok, Schema} | {error, Reason}.

eflatbuffers:new(Buffer, Schema) -> Ctx.
eflatbuffers:get(Ctx, Path) -> Value.
eflatbuffers:get(Ctx, Path, Default) -> Value.
eflatbuffers:has(Ctx, Path) -> boolean().
eflatbuffers:to_map(Ctx) -> Map.
eflatbuffers:to_map(Ctx, Opts) -> Map.
eflatbuffers:from_map(Map, Schema) -> iodata().
eflatbuffers:from_map(Map, Schema, Opts) -> iodata().
eflatbuffers:file_id(Ctx | Buffer) -> <<_:32>>.
eflatbuffers:get_bytes(Ctx, Path) -> binary().
```

Decode options:

```erlang
#{deprecated => skip | allow | error}  %% default: skip
```

Encode options:

```erlang
#{
    file_id => true | false | <<_:32>>,  %% default: true (use schema's file_identifier)
    deprecated => skip | allow | error   %% default: skip
}
```

## Supported Types

Scalars: bool, byte/int8, ubyte/uint8, short/int16, ushort/uint16, int/int32, uint/uint32, long/int64, ulong/uint64, float/float32, double/float64.

Compound: string (binary), vectors (list), tables (map), structs (map), enums (atom), unions.

## Feature Support

Buffers are binary-compatible with flatc. Unsupported features will fail at schema parse time or be silently ignored, but supported features produce identical wire format.

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

Test schema `test/schemas/union_vector.fbs` adapted from [planus](https://github.com/planus-org/planus) (MIT/Apache-2.0).
