# FlatBuffer Scalar Alignment Bug in flatbuferl

## Summary

`flatbuferl_builder.erl` caps field alignment at 4 bytes, causing 8-byte scalar fields (`long`, `ulong`, `int64`, `uint64`, `double`, `float64`) to be placed at 4-byte-aligned positions instead of 8-byte-aligned positions. This violates the FlatBuffer specification and causes the Rust FlatBuffer verifier to reject the output.

The bug is invisible within the Erlang ecosystem (encode→decode roundtrips work fine because flatbuferl's reader tolerates misalignment) but manifests immediately when buffers cross language boundaries to Rust, C++, or any other implementation with strict verification.

## Root Cause

**File:** `src/flatbuferl_builder.erl`
**Function:** `place_fields_backward/2` (line 796)
**The bug:** Line 799

```erlang
place_fields_backward(Fields, TableSize) ->
    {Slots, _} = lists:foldl(
        fun(#field{id = Id, size = Size}, {Acc, EndPos}) ->
            Align = min(Size, 4),    %% <-- BUG: caps alignment at 4
            StartPos = EndPos - Size,
            AlignedStart = align_down(StartPos, Align),
            {Acc#{Id => AlignedStart}, AlignedStart}
        end,
        {#{}, TableSize},
        Fields
    ),
    Slots.
```

The `min(Size, 4)` expression limits alignment to 4 bytes. For 8-byte fields (Size=8), this produces `Align = 4` instead of the correct `Align = 8`. The field is then placed at a 4-byte boundary which may not be an 8-byte boundary.

### Why root tables sometimes work

The root table path (`encode_root_vtable_before`, lines 284-298) has a separate alignment adjustment:

```erlang
First8ByteOffset = find_first_8byte_field_offset(AllFields, Slots),
TablePos = case First8ByteOffset of
    none -> TablePos4;
    Offset ->
        case (TablePos4 + Offset) rem 8 of
            0 -> TablePos4;
            _ -> TablePos4 + 4
        end
end,
```

This adjusts the **table start position** to ensure the *first* 8-byte field lands on an 8-byte boundary. However, it only fixes the first 8-byte field — if there are multiple 8-byte fields and the spacing between them is not a multiple of 8 (due to intervening fields of other sizes), subsequent 8-byte fields will be misaligned.

### Why nested tables always fail

The nested table path (`encode_nested_table`, line 1647) does NOT have the `find_first_8byte_field_offset` adjustment at all. Nested tables are laid out with `place_fields_backward` only, which caps at 4-byte alignment. Any nested table containing a `long`, `ulong`, `double`, etc. will produce misaligned output.

### Why `encode_root_vtable_after` (vtable-sharing path) also fails

The vtable-sharing path (line 321) calls `place_fields_backward` for field layout and has no 8-byte alignment adjustment. Root tables that take this path will also misalign 8-byte fields.

## Reproduction

### Test case: `game_state.fbs` (existing schema)

The test file `test/complex_schemas/game_state.fbs` defines a `GameData` table with 21 reference fields followed by two `long` fields:

```flatbuffers
table GameData {
  workers:Workers;            // ref (4 bytes in table)
  trophies:int;               // 4 bytes
  academyTechnologies:[Technology];  // ref
  arsenalTechnologies:[Technology];  // ref
  ships:[Ship];               // ref
  // ... 16 more ref fields ...
  nextPirateAttack:long;      // 8 bytes -- MISALIGNED
  // ... more refs ...
  lastPvpAttackTime:long;     // 8 bytes -- MISALIGNED
  // ...
}
```

The `place_fields_backward` layout (working backward from TableSize):
1. All 21 ref fields occupy 84 bytes (21 × 4), each 4-byte aligned ✓
2. `trophies` (int) occupies 4 bytes, 4-byte aligned ✓
3. `fortressLevel` (byte) occupies 1 byte + 3 padding ✓
4. soffset occupies 4 bytes (offset 0-3 of table data)
5. Table data starts at 4 bytes into the table object

Total before first `long`: 4 (soffset) + 4 (byte+pad) + 4 (int) + 84 (refs) = 96 bytes.

If the table starts at an 8-byte-aligned position (e.g., byte 80), then `nextPirateAttack` is at absolute byte 80 + 96 = 176, which IS 8-byte aligned (176 % 8 = 0). But the backward layout places fields starting from the END of the table, and the actual position depends on the exact field ordering computed by `place_fields_backward`.

In practice, the diagnostic output from the alignment test shows:
- `nextPirateAttack` lands at absolute byte **172** (172 % 8 = 4) — MISALIGNED
- `lastPvpAttackTime` lands at absolute byte **180** (180 % 8 = 4) — MISALIGNED

### Running the test

```bash
cd flatbuferl
rebar3 eunit --module=alignment_verification_tests
```

Expected output: `game_state_nested_alignment_test` FAILS with alignment violations on `long` fields.

### Protocol schema reproduction

The `test/complex_schemas/protocol.fbs` (copied from `runner/priv/protocol.fbs`) defines the production messages. The `CommitRoundRequest` message has:

```flatbuffers
table CommitRoundRequest {
    service_id: ServiceId;       // nested table ref
    round: uint64;               // 8 bytes
    randomness: [ubyte];         // ref
    timestamp_ms: uint64;        // 8 bytes
    transactions: [Transaction]; // ref
    checkpoint: bool;            // 1 byte
    epoch_transition: EpochTransition; // ref
}
```

The Rust verifier reports: `Type u64 at position 84 is unaligned` for the `round` field.

## Impact

| Scenario | Impact |
|----------|--------|
| Erlang→Erlang roundtrip | No impact (flatbuferl reader tolerates misalignment) |
| Erlang→Rust (wasvm-rs) | **Breaks**: Rust FlatBuffer verifier rejects the buffer |
| Erlang→C++ | **Breaks**: C++ verifier rejects the buffer |
| Erlang→Java/Go/etc. | Likely breaks (most verifiers are strict) |
| Any table with `long`/`ulong`/`double`/`float64` + enough preceding fields | Affected |
| Tables with only `int`/`uint`/`string`/`bool` and nested tables | Not affected (4-byte alignment is sufficient) |

## Recommended Fix

### Minimal fix (line 799)

Change `place_fields_backward` to respect natural alignment:

```erlang
%% Before (buggy):
Align = min(Size, 4),

%% After (fixed):
Align = case Size of
    8 -> 8;   % long, ulong, double, float64
    _ -> min(Size, 4)
end,
```

This ensures 8-byte fields are placed at 8-byte boundaries within the table data area. The `align_down/2` function on line 803 already handles the actual alignment math correctly — it just receives the wrong alignment value from `min(Size, 4)`.

### Additional fix: nested table 8-byte adjustment

The `encode_nested_table` function (line 1647) should add the same `find_first_8byte_field_offset` adjustment that `encode_root_vtable_before` uses (lines 288-298). This ensures the nested table's absolute position in the buffer places the first 8-byte field correctly.

However, the `place_fields_backward` fix alone may be sufficient if the table's internal layout correctly spaces 8-byte fields at 8-byte intervals. The table start position only matters for the absolute alignment — the relative spacing between fields is what `place_fields_backward` controls.

### Comprehensive fix (recommended)

The FlatBuffer spec's reference implementation (`flatc`) uses a different strategy: it sorts fields by size (largest first) within each table, which naturally minimizes padding and ensures large fields are placed first (at the most-aligned positions). This is more efficient but would be a larger change to the builder.

A pragmatic approach:

1. **Fix `place_fields_backward`** (line 799): `Align = case Size of 8 -> 8; _ -> min(Size, 4) end`
2. **Fix `encode_nested_table`** (line 1647): Add 8-byte alignment adjustment for nested tables containing `long`/`double` fields, similar to lines 288-298
3. **Fix `encode_root_vtable_after`** (line 321): Add the same 8-byte adjustment for the vtable-sharing path
4. **Verify with the alignment test suite**: `rebar3 eunit --module=alignment_verification_tests` — all tests should pass after the fix

### What NOT to change

- The 4-binary_match tests that were skipped compare exact byte layout against `flatc`. The fix may produce different (but valid) padding than `flatc`. These tests should be updated to verify structural validity rather than byte-exact equality.
- The vtable format is correct — vtable entries are u16 and always 2-byte aligned.
- The vtable padding fix (already applied in this fork) for nested table offsets is correct and should be kept.

## Test Coverage

The test file `test/alignment_verification_tests.erl` contains 15 tests:

| Test | Schema | What it checks | Expected result |
|------|--------|---------------|-----------------|
| `game_state_nested_alignment_test` | game_state.fbs | Deeply nested tables with `long` fields | **FAILS** (exposes the bug) |
| `protocol_envelope_commit_round_test` | protocol.fbs | CommitRoundRequest with `uint64` round | Passes (happens to align) |
| `protocol_envelope_init_service_test` | protocol.fbs | InitServiceRequest with nested ServiceId | Passes (happens to align) |
| `protocol_envelope_deploy_test` | protocol.fbs | DeployRequest | Passes |
| `protocol_envelope_shutdown_test` | protocol.fbs | ShutdownRequest (no nested tables) | Passes |
| `scalar_alignment_test` | protocol.fbs | Mixed scalar types | Passes |
| `multi_u64_alignment_test` | protocol.fbs | Multiple u64 fields | Passes |
| `deep_nesting_alignment_test` | protocol.fbs | 4 levels of nesting | Passes |
| `commit_round_direct_known_positions_test` | protocol.fbs | CRR as root (not wrapped in Envelope) | Passes |
| `many_refs_then_i64_alignment_test` | protocol.fbs | 24-field table mimicking GameData | Passes |
| + 5 more synthetic tests | protocol.fbs | Various field patterns | Pass |

The protocol tests pass because the `Envelope` union wrapper adds enough padding that inner fields happen to be aligned for the specific field counts tested. The `game_state.fbs` test fails because `GameData` has 21 ref fields which shift the i64 fields to 4 mod 8.

**After fixing `place_fields_backward`**, all 15 tests should pass. The `game_state_nested_alignment_test` is the regression gate.

## Files Changed in This Fork

| File | Change | Status |
|------|--------|--------|
| `src/flatbuferl_builder.erl` | Vtable padding for nested tables (lines 1654-1660) | Applied, tested |
| `src/flatbuferl_builder.erl` line 799 | Scalar alignment fix (`min(Size, 4)` → `8` for 8-byte fields) | **NOT YET APPLIED** |
| `test/alignment_verification_tests.erl` | 15 alignment verification tests | Added |
| `test/complex_schemas/protocol.fbs` | Copy of runner's protocol schema for testing | Added |

## References

- FlatBuffer encoding spec: https://flatbuffers.dev/flatbuffers_internals.html
- Rust verifier source: `flatbuffers` crate, `Verifier::verify_buffer_alignment`
- Production failure: `mearum_runner/design/decisions/BUG-flatbuferl-alignment.md`
- Integration log: `integration_logs/flatbufferl_log.md`
