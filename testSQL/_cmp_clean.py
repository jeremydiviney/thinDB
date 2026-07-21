"""Clean compute-only comparison: thinDB vs StarRocks.

Network/connection overhead is cancelled by subtracting a baseline `SELECT 1`
that pays the identical connection + handshake + one round-trip in the same
multi-statement form. StarRocks query cache is disabled per session and the
query is run several times to prove it does real work each time (no result
caching -> times stay flat, they don't collapse to the baseline).

  SR_PWD='...' py testSQL/_cmp_clean.py
"""
import os, subprocess, sys, time, statistics

MYSQL = r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
PW = os.environ.get('SR_PWD') or os.environ.get('APP_DORIS_PASSWORD')
if not PW:
    sys.exit('set SR_PWD')

raw = open('testSQL/rollforward_1000054_flat_agg.txt', encoding='utf-8').read()
query = '\n'.join(ln for ln in raw.split('\n') if not ln.strip().lower().startswith('use '))

WARMUP = 2
REPS = 6


def run(host, port, db, sql, pw=None):
    env = dict(os.environ)
    if pw:
        env['MYSQL_PWD'] = pw
    args = [MYSQL, '-h', host, '-P', str(port), '-u', 'root', '--database=' + db, '--batch', '--raw']
    t = time.perf_counter()
    p = subprocess.run(args, input=sql, capture_output=True, text=True, env=env, timeout=600)
    dt = time.perf_counter() - t
    if p.returncode != 0 or 'ERROR' in (p.stderr or ''):
        sys.exit(f'{host}:{port} FAILED: {(p.stderr or "").strip()[:300]}')
    rows = max(0, len([l for l in p.stdout.splitlines() if l.strip()]) - 1)
    return dt, rows


def bench(label, host, port, db, prefix, pw=None):
    base_sql = prefix + 'SELECT 1;\n'
    q_sql = prefix + query
    for _ in range(WARMUP):
        run(host, port, db, q_sql, pw)
    base = [run(host, port, db, base_sql, pw)[0] for _ in range(REPS)]
    qs = [run(host, port, db, q_sql, pw) for _ in range(REPS)]
    qt = [d for d, _ in qs]
    rows = qs[0][1]
    bmin = min(base)
    compute = [t - bmin for t in qt]
    print(f'\n=== {label} ({host}:{port}) — rows={rows} ===')
    print('  baseline SELECT 1 (conn+handshake+1 RTT): min %.3f  med %.3f s' % (bmin, statistics.median(base)))
    print('  full query wall   :', '  '.join('%.3f' % t for t in qt))
    print('  compute (wall-base):', '  '.join('%.3f' % t for t in compute))
    print('  => compute min %.3f  med %.3f s' % (min(compute), statistics.median(compute)))
    return min(compute), statistics.median(compute)


print('RTT note: ICMP min ~103ms to StarRocks; the baseline subtraction below removes it regardless.')
sr_min, sr_med = bench('StarRocks (query cache OFF)', os.environ['SR_HOST'], 9030, 'wayroll',
                       'SET enable_query_cache=false;\n', PW)
th_min, th_med = bench('thinDB', '127.0.0.1', 7881, 'wayroll__public', '')

print('\n================ COMPUTE-ONLY (network removed) ================')
print('  StarRocks : min %.3f  med %.3f s' % (sr_min, sr_med))
print('  thinDB    : min %.3f  med %.3f s' % (th_min, th_med))
if th_med and sr_med:
    r = th_med / sr_med
    faster = 'thinDB faster' if r < 1 else 'StarRocks faster'
    print('  ratio (med): thinDB/StarRocks = %.2fx  -> %s' % (r, faster))
