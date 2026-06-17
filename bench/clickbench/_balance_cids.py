import csv, sys

N = 12
rows = []
with open('bench/clickbench/_cid_counts.csv', newline='') as f:
    r = csv.reader(f)
    next(r)
    for cid, c in r:
        rows.append((int(cid), int(c)))

counts = [c for _, c in rows]
cids = [cid for cid, _ in rows]
total = sum(counts)
maxgrp = max(counts)
print(f"distinct CounterIDs={len(rows)} total_rows={total} ideal_share={total/N:.0f} max_single_group={maxgrp} ({100*maxgrp/total:.2f}%)")

# min-max contiguous partition into N blocks via binary search on capacity
def blocks_needed(cap):
    blocks, acc = 1, 0
    for c in counts:
        if acc + c > cap:
            blocks += 1
            acc = c
            if blocks > N:
                return blocks
        else:
            acc += c
    return blocks

lo, hi = maxgrp, total
while lo < hi:
    mid = (lo + hi) // 2
    if blocks_needed(mid) <= N:
        hi = mid
    else:
        lo = mid + 1
cap = lo

# rebuild the partition at the optimal cap, balancing block fill
blocks = []
acc = 0
start = 0
for i, c in enumerate(counts):
    if acc + c > cap and len(blocks) < N - 1:
        blocks.append((start, i))  # [start, i)
        start = i
        acc = c
    else:
        acc += c
blocks.append((start, len(counts)))

print(f"optimal min-max capacity={cap} ({100*cap/total:.2f}% of total)")
cuts = []
for bi, (s, e) in enumerate(blocks):
    bsum = sum(counts[s:e])
    lo_cid = cids[s]
    hi_cid = cids[e-1]
    print(f"  block {bi:2d}: CounterID [{lo_cid:>7} .. {hi_cid:>7}]  groups={e-s:<5} rows={bsum:>10} ({100*bsum/total:5.2f}%)")
    if bi < len(blocks) - 1:
        cuts.append(cids[e])  # first CID of next block

print("CUTS:", " ".join(str(c) for c in cuts))
