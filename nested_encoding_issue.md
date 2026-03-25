# FlatBuffer Encoding Bugs in flatbuferl

## Status: RESOLVED (2026-03-24)

## Summary

Two separate bugs were found in flatbuferl that caused cross-language FlatBuffer verification failures. Both are invisible within the Erlang ecosystem (flatbuferl's reader tolerates the issues) but manifest immediately when buffers cross language boundaries to Rust, C++, or any other implementation with strict verification.

**Bug 1 — Scalar alignment (2026-03-20, committed `e5b2518`):** `flatbuferl_builder.erl` was missing 8-byte absolute alignment adjustments for two code paths: the vtable-sharing root path (`encode_root_vtable_after`) and nested tables (via `calc_ref_align_padding`). This caused 8-byte scalar fields to land at 4-byte-aligned absolute buffer positions.

**Bug 2 — Struct array sizing (2026-03-24):** `flatbuferl_schema.erl` `primitive_type_size/1` had a catch-all clause `primitive_type_size(_) -> 4` that silently returned 4 for `{array, ElemType, Count}` tuples (fixed-size arrays in structs). This caused struct fields like `Hash256` (a struct with `bytes: [uint8:32]`) to be sized as 4 bytes instead of 32, corrupting all subsequent offsets in the table. This was the P0 blocker for N=3 consensus `DeployRequest` encoding.

## Root Cause Analysis

### How FlatBuffers alignment actually works

The FlatBuffer spec requires 8-byte scalars to be at 8-byte-aligned **absolute buffer positions** (not table-relative). The reference compiler (`flatc`) achieves this through two mechanisms:

1. **Table-relative placement uses `min(Size, 4)`** — fields within a table are placed with at most 4-byte relative alignment. This is intentional and matches flatc.

2. **Table absolute position is adjusted** — the table's start position in the buffer is shifted (by 0 or 4 bytes) to ensure the first 8-byte field lands on an 8-byte boundary. Since all 8-byte fields placed with `min(Size, 4)` have the same residue mod 8, fixing the first one fixes all of them.

### Initial misdiagnosis

The original analysis (below, kept for reference) identified `min(Size, 4)` in `place_fields_backward` as the bug. This was incorrect — `min(Size, 4)` is the correct table-relative alignment, matching `flatc`. Changing it to 8 produces correct alignment but different byte layouts from `flatc`, breaking binary-match tests and producing unnecessarily padded tables.

### Actual bugs

The root table's vtable-before path (`encode_root_vtable_before`) already had the absolute position adjustment. Two other paths were missing it:

| # | Path | Function | Issue |
|---|------|----------|-------|
| 1 | Root vtable-after (vtable sharing) | `encode_root_vtable_after` | `TablePos = HeaderSize` with no 8-byte adjustment |
| 2 | Nested tables | `calc_ref_align_padding` | Blob start padded to 4-byte only, not accounting for 8-byte fields |

## Resolution

### Fix 1: `encode_root_vtable_after` (~line 333)

Added the same `find_first_8byte_field_offset` + table position adjustment that `encode_root_vtable_before` already had. When the table has 8-byte fields, `TablePos` is shifted by +4 when needed, and padding bytes are emitted between the header and soffset.

```erlang
%% Root table starts right after header (no vtable before it)
TablePos4 = HeaderSize,

%% If table has 8-byte fields, ensure they're 8-byte aligned in the buffer
PaddedSlots = place_fields_backward(AllFields, TableSizeWithPadding),
First8ByteOffset = find_first_8byte_field_offset(AllFields, PaddedSlots),
TablePos =
    case First8ByteOffset of
        none ->
            TablePos4;
        Offset ->
            case (TablePos4 + Offset) rem 8 of
                0 -> TablePos4;
                _ -> TablePos4 + 4
            end
    end,
PreTablePad = TablePos - HeaderSize,
```

### Fix 2: `calc_ref_align_padding` (~line 1101)

For nested tables with 8-byte fields, the blob start alignment is now computed precisely so that `blob_start + PaddedVTableSize + first_8byte_field_offset` is 8-byte aligned. This uses a new helper `nested_table_first_8byte_offset/3` that calls `layout_for_value` to get the actual field layout for the specific value being encoded.

```erlang
calc_ref_align_padding(Type, Value, Pos, Defs, LayoutCache) ->
    case is_nested_table_type_cached(Type, Value, Defs, LayoutCache) of
        {true, PaddedVTableSize} ->
            %% If nested table has 8-byte fields, align blob start so that
            %% (blob_start + PaddedVTableSize + first_8byte_offset) is 8-byte aligned
            case nested_table_first_8byte_offset(Type, Value, Defs) of
                none ->
                    (4 - (Pos rem 4)) rem 4;
                First8ByteOffset ->
                    NeededMod = (8 - ((PaddedVTableSize + First8ByteOffset) rem 8)) rem 8,
                    (NeededMod - (Pos rem 8) + 8) rem 8
            end;
        false ->
            vector_8byte_align_padding(Type, Pos)
    end.
```

The key insight: `NeededMod` is the required value of `blob_start rem 8`. When `PaddedVTableSize + First8ByteOffset` is already 0 mod 8, `NeededMod` is 0 (blob needs 8-byte alignment). When it's 4 mod 8, `NeededMod` is 4 (blob needs to be at 4 mod 8). This handles all cases precisely.

### What was NOT changed (and why)

- **`min(Size, 4)` in `place_fields_backward`, `calc_all_present_layout`, and `adjust_slots_for_missing`** — This is correct. `flatc` uses the same table-relative alignment. Changing it produces valid buffers but breaks binary-match tests against `flatc` and adds unnecessary table padding.

- **`BaseTableSize` calculations** — Unchanged. The formula `4 + align_offset(RawSize, 4)` matches `flatc`.

- **`encode_nested_table` vtable padding** — Kept at 4-byte alignment. The vtable padding must agree with `is_nested_table_type_cached` (which returns the padded vtable size for uoffset calculations). Adjusting vtable padding would require a coordinated change to both functions; adjusting blob start is simpler and equally correct.

- **Vtable format, field ordering, soffset encoding** — All correct already.

### Approaches that were tried and rejected

1. **Changing `min(Size, 4)` to 8-byte table-relative alignment** — Produces valid buffers but different layouts from `flatc`. Breaks `all_scalars_binary_match` and `all_types_binary_match` tests. Also requires changing `BaseTableSize` to `align_offset(4 + RawSize, 8)` to prevent backward placement from pushing small fields into the soffset area. Too invasive for no benefit since `flatc` itself uses `min(Size, 4)`.

2. **Adding vtable padding in `encode_nested_table`** — Would require `is_nested_table_type_cached` to return the matching padded vtable size, but that function doesn't have access to the specific value's field layout. Mismatch between the two causes incorrect uoffsets.

3. **Simple 8-byte blob alignment** — Padding blob start to 8 unconditionally doesn't work when `PaddedVTableSize + First8ByteOffset` is 4 mod 8 (the blob needs to be at 4 mod 8, not 0 mod 8).

---

## Bug 2: Struct Array Sizing (2026-03-24)

### Symptom

N=3 cluster: DKG completes, HBBFT group starts, `store_component` succeeds, then `deploy` panics in wasvm-rs:

```
internal panic: range end index 3825351014 out of range for slice of length 212
```

The garbage offset `3825351014` (0xE3F70966) is a misaligned 4-byte read from the FlatBuffer being interpreted as a table offset. Only `DeployRequest` fails — simpler messages like `InitServiceRequest` and `StoreComponentRequest` work fine because they don't contain structs with fixed-size arrays.

### Root Cause

`primitive_type_size/1` in `flatbuferl_schema.erl` has a catch-all clause:

```erlang
primitive_type_size(_) -> 4.
```

When called with `{array, uint8, 32}` (the type tuple for a `[uint8:32]` struct field), it fell through to the catch-all and returned 4 instead of 32. This caused:

1. `Hash256` struct `total_size = 4` (should be 32)
2. RegistryRef table `tbl_size = 16` (should be 44 = 4+4+4+32)
3. Inline struct data overflowed the table boundary
4. Subsequent string offsets pointed into struct data → garbage offsets → panic

### Fix

One line added to `flatbuferl_schema.erl` (~line 242), before the catch-all:

```erlang
primitive_type_size({array, ElemType, Count}) -> primitive_type_size(ElemType) * Count;
```

### Verification

- flatc decodes the fixed output correctly (all fields, strings, Hash256 bytes match)
- 728 existing tests pass (0 failures)
- Binary comparison via [.claude/tmp/compare_deploy.escript](.claude/tmp/compare_deploy.escript) confirms byte-identical output to flatc for `DeployRequest`

### Status

**Committed** on `bugfix/offset` branch. Ready to merge.

### Diagnostic Artifacts (can be deleted)

These files were generated during diagnosis and are **not** tracked in git:

| File | Purpose | Track? |
|------|---------|--------|
| `deploy_request.bin` | flatc reference binary for DeployRequest (generated via `flatc -b protocol.fbs deploy.json`) | No — regenerable from schema + JSON |
| `erl_deploy_fixed.json` | JSON decode of flatbuferl's fixed output, used to verify field values match | No — diagnostic artifact |
| `.claude/tmp/compare_deploy.escript` | Escript that hex-diffs flatbuferl vs flatc output | No — in `.claude/tmp/` scratch dir |

---

## Reproduction (historical, Bug 1)

### Test case: `game_state.fbs`

The test file `test/complex_schemas/game_state.fbs` defines a `GameData` table with 21 reference fields and two `long` fields. Before the fix, the diagnostic output showed:
- `nextPirateAttack` at absolute byte **172** (172 % 8 = 4) -- MISALIGNED
- `lastPvpAttackTime` at absolute byte **180** (180 % 8 = 4) -- MISALIGNED

The root cause: `GameData` is a nested table inside `GameStateRoot`. The blob start for the nested table was 4-byte aligned but not 8-byte aligned. With PaddedVTableSize = 52 and first 8-byte field at table-relative offset 92, the absolute position was `blob_start + 52 + 92 = blob_start + 144`. Since 144 is 0 mod 8, the blob start needed to be 0 mod 8, but was only guaranteed 4 mod 8.

### Running the tests

```bash
cd flatbuferl
rebar3 eunit --module=alignment_verification_tests  # 15 alignment tests
rebar3 eunit                                         # full suite (728 tests)
```

All 728 tests pass after the fix, including all 15 alignment verification tests and all binary-match tests against `flatc`.

## Impact

| Scenario | Impact |
|----------|--------|
| Erlang-to-Erlang roundtrip | No impact (flatbuferl reader tolerates misalignment) |
| Erlang-to-Rust (wasvm-rs) | **Fixed**: Rust FlatBuffer verifier now accepts the buffer |
| Erlang-to-C++ | **Fixed**: C++ verifier now accepts the buffer |
| Erlang-to-Java/Go/etc. | **Fixed**: All compliant verifiers should accept |
| Any table with `long`/`ulong`/`double`/`float64` | Fixed for root (both paths) and nested tables |
| Tables with only `int`/`uint`/`string`/`bool` | Not affected (4-byte alignment was already sufficient) |

## Files Changed

### Bug 1 (committed `e5b2518`)

| File | Change | Status |
|------|--------|--------|
| `src/flatbuferl_builder.erl` ~line 333 | 8-byte table position adjustment in `encode_root_vtable_after` | **Committed** |
| `src/flatbuferl_builder.erl` ~line 1101 | Exact blob start alignment in `calc_ref_align_padding` | **Committed** |
| `src/flatbuferl_builder.erl` ~line 1119 | New helper `nested_table_first_8byte_offset/3` | **Committed** |
| `test/alignment_verification_tests.erl` | 15 alignment verification tests | **Committed** |
| `test/complex_schemas/protocol.fbs` | Copy of runner's protocol schema for testing | **Committed** |

### Bug 2 (committed on `bugfix/offset`)

| File | Change | Status |
|------|--------|--------|
| `src/flatbuferl_schema.erl` ~line 242 | `primitive_type_size({array, ElemType, Count})` clause | **Committed** |

Total diff across both bugs: +53 -6 lines in production code.

## FlatBuffer Spec Alignment Rules (reference)

From the FlatBuffer internals specification:

- **Scalars are aligned to their own size** in absolute buffer positions. A `uint64` at absolute buffer position P requires `P % 8 == 0`.
- **This is a spec requirement**, not just Rust verifier strictness. All compliant implementations enforce it.
- **`flatc` achieves 8-byte alignment** by using `min(Size, 4)` for table-relative field placement and adjusting the table's absolute position in the buffer. Since all 8-byte fields within a table share the same residue mod 8 (a consequence of backward placement with 4-byte alignment where 8-byte fields are placed first), fixing the table position for the first 8-byte field fixes all of them.
- **Nested tables** follow the same rules. The nested table blob's start position must be chosen so that its 8-byte fields are at 8-byte-aligned absolute positions.

## References

- FlatBuffer encoding spec: https://flatbuffers.dev/flatbuffers_internals.html
- Rust verifier source: `flatbuffers` crate, `Verifier::verify_buffer_alignment`
- Production failure: `mearum_runner/design/decisions/BUG-flatbuferl-alignment.md`
- Integration log: `integration_logs/flatbufferl_log.md`
