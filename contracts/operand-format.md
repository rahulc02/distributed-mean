# Operand format, version 1

Every input file and every partial result is an **operand**: a sum vector plus
the number of input files it covers. An input file is an operand with count 1.
Partials store **sums, never means**, so combining operands is plain addition.

All integers and floats are little-endian.

| Offset | Size      | Field   | Value                                   |
| ------ | --------- | ------- | --------------------------------------- |
| 0      | 4         | magic   | ASCII `DMOP`                            |
| 4      | 2         | version | `1` (uint16)                            |
| 6      | 2         | flags   | `0`, reserved (uint16)                  |
| 8      | 8         | count   | input files covered (uint64, > 0)       |
| 16     | 8         | c       | number of values (uint64)               |
| 24     | 8 × c     | values  | float64 sums                            |

Total size is exactly `24 + 8 × c` bytes.

The 24-byte header keeps the values 8-byte aligned, so readers can map them
without copying (`np.frombuffer(buf, dtype='<f8', offset=24)`).

## Reader obligations

A reader must reject an operand (and the task that references it is
dead-lettered) when any of these fail:

- `magic` is `DMOP` and `version` is `1`
- `c` equals the job's C
- the byte length equals `24 + 8 × c`
- `count` is greater than 0

## Writers

- TypeScript: `api/src/operand/codec.ts` (writes input files)
- Python: `py/dmean/common/operand.py` (writes partials, reads everything)

Both are checked against `fixtures/operand_v1.bin` by the contract tests.

## Final result

The final task writes `results/{job_id}.csv` instead of an operand: one line of
C comma-separated means.
