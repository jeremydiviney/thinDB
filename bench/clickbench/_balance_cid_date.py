import csv

N = 12
rows = []
with open('bench/clickbench/_cid_date.csv', newline='') as f:
    r = csv.reader(f)
    next(r)
    for cid, d, rne, b in r:
        rows.append((int(cid), d, int(rne or 0), int(b or 0)))

def partition(weight_idx, name):
    w = [row[weight_idx] for row in rows]
    total = sum(w)
    cap_lo = max(w)  # a single cell is the indivisible unit now
    # min-max contiguous partition into <=N blocks via binary search on cap.
    # When one cell exceeds total/N (the byte-whale) the cap floors at that cell
    # and fewer than N blocks result -- that is itself the finding.
    def blocks_needed(cap):
        blocks, acc = 1, 0
        for x in w:
            if acc + x > cap:
                blocks += 1; acc = x
                if blocks > N: return blocks
            else:
                acc += x
        return blocks
    lo, hi = cap_lo, total
    while lo < hi:
        mid = (lo + hi)//2
        if blocks_needed(mid) <= N: hi = mid
        else: lo = mid+1
    cap = lo
    blocks, acc, start = [], 0, 0
    for i, x in enumerate(w):
        if acc + x > cap and len(blocks) < N-1:
            blocks.append((start, i)); start = i; acc = x
        else:
            acc += x
    blocks.append((start, len(w)))
    print(f"\n=== balance by {name}: total={total} ideal={total/N:.0f} max_cell={cap_lo} ({100*cap_lo/total:.3f}% -- a single (CounterID,EventDate) cell, indivisible) cap={cap} ({100*cap/total:.2f}%) blocks={len(blocks)}")
    # boundary = first composite key of each block (b[1..N-1] are interior cuts)
    cuts = []
    for bi,(s,e) in enumerate(blocks):
        bw = sum(w[s:e])
        c0,d0 = rows[s][0], rows[s][1]
        c1,d1 = rows[e-1][0], rows[e-1][1]
        print(f"  block {bi:2d}: ({c0},{d0}) .. ({c1},{d1})  cells={e-s:<5} weight={bw:>12} ({100*bw/total:5.2f}%)")
        if bi < len(blocks)-1:
            cuts.append((rows[e][0], rows[e][1]))  # start of next block
    # emit zig arrays
    cid_arr = ", ".join(str(c) for c,_ in cuts)
    dat_arr = ", ".join(f'"{d}"' for _,d in cuts)
    print(f"  CID_CUTS [{cid_arr}]")
    print(f"  DATE_CUTS [{dat_arr}]")

partition(2, "rows (URL<>'')")
partition(3, "URL bytes")
