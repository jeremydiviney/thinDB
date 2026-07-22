"""Run the rollforward query against StarRocks (source of truth) and thinDB,
compare row count + byte-identical rows. Credentials via env SR_PWD only.

  SR_PWD='...' py testSQL/_compare_starrocks.py
"""
import os, subprocess, sys, time

MYSQL = r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
PW = os.environ.get('SR_PWD') or os.environ.get('APP_DORIS_PASSWORD')
if not PW:
    sys.exit('set SR_PWD (StarRocks password) in the environment first')

raw = open('testSQL/rollforward_1000054_flat_agg.txt', encoding='utf-8').read()
# Same SQL to both engines; only the `use <db>;` line differs by deployment,
# so drop it and select the database via the client flag instead.
query = '\n'.join(ln for ln in raw.split('\n') if not ln.strip().lower().startswith('use '))


def run(host, port, db, pw=None):
    env = dict(os.environ)
    if pw:
        env['MYSQL_PWD'] = pw
    args = [MYSQL, '-h', host, '-P', str(port), '-u', 'root', '--database=' + db, '--batch', '--raw']
    t = time.perf_counter()
    p = subprocess.run(args, input=query, capture_output=True, text=True, env=env, timeout=600)
    dt = time.perf_counter() - t
    if p.returncode != 0 or 'ERROR' in (p.stderr or ''):
        print(f'{host}:{port} FAILED:\n{(p.stderr or "").strip()[:800]}')
        sys.exit(1)
    return p.stdout, dt


def split(out):
    lines = out.rstrip('\n').split('\n')
    if lines == ['']:
        return [], []
    return lines[0], lines[1:]


print('running StarRocks...', file=sys.stderr)
sr_out, sr_t = run(os.environ['SR_HOST'], 9030, 'wayroll', PW)
print('running thinDB...', file=sys.stderr)
td_out, td_t = run('127.0.0.1', 7881, 'wayroll__public')

sr_head, sr_rows = split(sr_out)
td_head, td_rows = split(td_out)

print(f'StarRocks: {len(sr_rows)} rows in {sr_t:.2f}s')
print(f'thinDB   : {len(td_rows)} rows in {td_t:.2f}s')

cols = (sr_head or '').lower().split('\t')


def norm_cell(c):
    # StarRocks renders a month bucket as DATETIME (`2016-01-01 00:00:00`);
    # thinDB types it DATE (`2016-01-01`). Same instant — drop a midnight time
    # so the value compares equal. A non-midnight time stays and will diff.
    if c.endswith(' 00:00:00') and len(c) == 19:
        return c[:10]
    return c


def canon(rows):
    out = []
    for r in rows:
        out.append('\t'.join(norm_cell(c) for c in r.split('\t')))
    return sorted(out)


# Raw byte comparison (sorted multiset) first, then value comparison after
# normalizing the date/datetime rendering.
raw_match = sorted(sr_rows) == sorted(td_rows)
ss, ts = canon(sr_rows), canon(td_rows)

print(f'\nrow count          : {"MATCH" if len(sr_rows) == len(td_rows) else f"MISMATCH ({len(sr_rows)} vs {len(td_rows)})"}')
print(f'byte-identical (raw): {"YES" if raw_match else "NO"}')
print(f'value-identical     : {"YES" if ss == ts else "NO"}  (after date/datetime-midnight normalization)')

# Per-column mismatch tally + first real diffs (on the normalized, value level).
col_diffs = [0] * len(cols)
shown = 0
for i, (a, b) in enumerate(zip(ss, ts)):
    if a == b:
        continue
    ca, cb = a.split('\t'), b.split('\t')
    for j in range(min(len(ca), len(cb))):
        if ca[j] != cb[j]:
            col_diffs[j] += 1
    if shown < 12:
        print(f'DIFF row {i}:\n  SR: {a}\n  TD: {b}')
        shown += 1

if any(col_diffs):
    print('\nper-column value mismatches:')
    for name, n in zip(cols, col_diffs):
        if n:
            print(f'  {name}: {n}')

if len(sr_rows) != len(td_rows):
    sset, tset = set(ss), set(ts)
    print(f'\nonly in StarRocks ({sum(1 for r in ss if r not in tset)}): sample')
    for r in [r for r in ss if r not in tset][:6]:
        print('  ', r)
    print(f'only in thinDB ({sum(1 for r in ts if r not in sset)}): sample')
    for r in [r for r in ts if r not in sset][:6]:
        print('  ', r)

ok = ss == ts and len(sr_rows) == len(td_rows)
print('\n=> ' + ('VALUES IDENTICAL ✅' if ok else 'REAL DIFFERENCES FOUND ❌'))
sys.exit(0 if ok else 2)
