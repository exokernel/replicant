# Loro peer-id probe

Two Loro documents each insert N characters at position 0, with no sync. Then each
imports the other's update, and the probe prints the time of each import for a list
of peer-id pairs. This is the `same_region` cell of the divergence sweep, with no
Replicant code.

    cargo run --release -- 10000

On 2026-11-07, with loro 1.17.1, each import took either about 10 ms or about 4,300
ms, and the result was fixed for each pair of peer ids. See the dated observation in
`docs/divergence-sweep.md`.
