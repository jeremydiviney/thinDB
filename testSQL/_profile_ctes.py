import re, subprocess, sys, time

MYSQL = r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
ARGS = ["-h", "127.0.0.1", "-P", "7881", "-u", "root", "--database=wayroll__public"]

raw = open('testSQL/rollforward_1000054_flat_agg.txt', encoding='utf-8').read()
raw = re.sub(r'--[^\n]*', '', raw)

lines = raw.split('\n')
set_lines, body_start = [], 0
for i, ln in enumerate(lines):
    s = ln.strip()
    if s.lower().startswith('set ') or s.lower().startswith('use '):
        if s.lower().startswith('set '):
            set_lines.append(s.rstrip(';') + ';')
        continue
    if s.upper().startswith('WITH'):
        body_start = i
        break
body = '\n'.join(lines[body_start:])

def split_with(s):
    s = s.strip()
    assert s[:4].upper() == 'WITH', s[:20]
    i, ctes = 4, []
    while True:
        m = re.match(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(', s[i:], re.I)
        if not m:
            raise SystemExit('expected CTE name at: ' + s[i:i+40])
        name = m.group(1); i += m.end()
        depth, start = 1, i
        while depth > 0:
            c = s[i]
            if c in "'\"":
                q = c; i += 1
                while True:
                    if s[i] == q:
                        if i + 1 < len(s) and s[i+1] == q: i += 2; continue
                        break
                    i += 1
                i += 1
            elif c == '(': depth += 1; i += 1
            elif c == ')': depth -= 1; i += 1
            else: i += 1
        ctes.append((name, s[start:i-1]))
        m2 = re.match(r'\s*,', s[i:])
        if m2: i += m2.end(); continue
        return ctes, s[i:].strip()

ctes, final_sel = split_with(body)
preamble = '\n'.join(set_lines) + '\n' if set_lines else ''

def prefix_sql(k, tail):
    pref = ctes[:k]
    return preamble + 'WITH ' + ',\n'.join(f'{n} AS (\n{b}\n)' for n, b in pref) + '\n' + tail

def run_time(sql, reps=2):
    best, out = 1e9, ''
    for _ in range(reps):
        t = time.perf_counter()
        p = subprocess.run([MYSQL] + ARGS, input=sql, capture_output=True, text=True, timeout=300)
        dt = time.perf_counter() - t
        if p.returncode != 0 or 'ERROR' in (p.stderr or ''):
            return None, (p.stderr or '').strip().split('\n')[0]
        best = min(best, dt); out = p.stdout
    return best, out

# Warm the cache: one full pass first.
print('warming...', file=sys.stderr)
run_time(prefix_sql(len(ctes), f'SELECT COUNT(*) FROM {ctes[-1][0]}'), reps=1)

print(f'{"#":>3} {"cte":42} {"rows":>9} {"cum_s":>8} {"delta_s":>8}')
prev = 0.0
for k in range(1, len(ctes) + 1):
    name = ctes[k-1][0]
    t, out = run_time(prefix_sql(k, f'SELECT COUNT(*) FROM {name}'))
    if t is None:
        print(f'{k:>3} {name:42} FAIL {out}')
        continue
    rows = ''
    for ln in (out or '').split('\n'):
        ln = ln.strip()
        if ln.isdigit(): rows = ln
    delta = t - prev
    flag = '  <<<' if delta > 0.2 else ''
    print(f'{k:>3} {name:42} {rows:>9} {t:8.3f} {delta:8.3f}{flag}')
    prev = t

# Final SELECT (whole query, full result).
tf, _ = run_time(preamble + body)
print(f'\nFULL query (final SELECT): {tf:.3f}s')
