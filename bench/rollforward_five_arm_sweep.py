"""Run the complete rollforward matrix on the private benchmark snapshot."""
import json
import os
import pathlib
import subprocess
import sys
import time

from ordinary_sql_diagnosis import start, stop, command, port_free, UNIT

CASES = ['base simple', 'base cross', 'expanded simple', 'expanded cross',
         'child simple', 'child cross', 'interval quarter', 'interval annual',
         'fx latest', 'fx average', 'noest simple', 'plans simple', 'crossplans',
         'detail simple', 'detail cross']
ARMS = ['sr', 'sql', 'sql_keyed', 'zig_unkeyed', 'zig']


def memory_receipt(folder):
    state = command(['systemctl', 'show', UNIT, '-p', 'MainPID', '-p', 'MemoryPeak', '-p', 'ControlGroup'])
    (folder / 'memory.txt').write_text(state)
    cg = dict(line.split('=', 1) for line in state.splitlines())['ControlGroup']
    events = pathlib.Path('/sys/fs/cgroup' + cg + '/memory.events')
    if events.exists():
        raw = events.read_text()
        (folder / 'memory-events.txt').write_text(raw)
        values = dict(line.split() for line in raw.splitlines())
        if any(int(values.get(k, 0)) for k in ('max', 'oom', 'oom_kill', 'oom_group_kill')):
            raise RuntimeError('Candidate reached a memory limit')


def main():
    archive, output, data = (pathlib.Path(v).resolve() for v in sys.argv[1:4])
    dop = int(sys.argv[4]) if len(sys.argv) > 4 else 12
    if dop not in (12, 16):
        raise RuntimeError('Unsupported DOP')
    password = sys.stdin.readline().rstrip('\r\n')
    if not password:
        raise RuntimeError('Missing credential')
    env = dict(os.environ, SR_PW=password, NODE_ENV='development',
               NODE_OPTIONS='--max-old-space-size=4096 --require /tmp/region-release-quiet-env.cjs')
    output.mkdir(parents=True, exist_ok=True)
    binary = (archive / 'thindb-profile').resolve()
    health = archive / 'health.py'
    if not port_free():
        raise RuntimeError('Benchmark port occupied')
    if not (output / 'health-before.json').exists():
        (output / 'health-before.json').write_text(command(['python3', str(health)]))
    harness = pathlib.Path(__file__).with_suffix('.cjs')
    completed = sum(1 for _ in output.glob('*/complete.json'))
    for project_index, project in enumerate((1000049, 1000073)):
        for case_index, label in enumerate(CASES):
            offset = (case_index + project_index) % len(ARMS)
            order = ARMS[offset:] + ARMS[:offset]
            if (case_index + project_index) % 2:
                order = list(reversed(order))
            for arm in order:
                folder = output / f'{project}-{label.replace(" ", "-")}-{arm}'
                if (folder / 'complete.json').exists():
                    continue
                folder.mkdir(exist_ok=True)
                if (folder / 'result.json').exists():
                    raise RuntimeError('Partial samples require inspection: ' + str(folder))
                if not port_free():
                    raise RuntimeError('Benchmark port occupied before block')
                began = time.time()
                try:
                    if arm != 'sr':
                        for attempt in range(60):
                            try:
                                start(archive, folder, data, dop, False)
                                break
                            except RuntimeError as error:
                                if not str(error).startswith('Less than 28 GiB'):
                                    raise
                                print(json.dumps({'waiting_for_headroom': True, 'completed': completed}), flush=True)
                                time.sleep(10)
                        else:
                            raise RuntimeError('Host headroom did not recover')
                    with (folder / 'client.log').open('w') as log:
                        result = subprocess.run(['node', str(harness), str(archive), str(folder),
                                                 str(project), arm, str(dop), label], env=env,
                                                cwd=(archive / 'app').resolve(), stdout=log,
                                                stderr=subprocess.STDOUT, timeout=900)
                    if arm != 'sr':
                        memory_receipt(folder)
                    if result.returncode:
                        raise RuntimeError((folder / 'client.log').read_text()[-1600:])
                    report = json.loads((folder / 'result.json').read_text())
                    if len(report['samples']) != 7 or sum(s['validation'] for s in report['samples']) != 1:
                        raise RuntimeError('Incomplete sample block')
                    (folder / 'complete.json').write_text(json.dumps({'order': order, 'completed_at': time.time()}))
                    completed += 1
                    progress = {'completed': completed, 'total': 150, 'project': project, 'case': label,
                                'arm': arm, 'median_ms': report['median_ms'], 'block_seconds': time.time() - began}
                    (output / 'progress.json').write_text(json.dumps(progress))
                    print(json.dumps(progress), flush=True)
                except Exception as error:
                    (folder / 'failure.json').write_text(json.dumps({'error': str(error), 'at': time.time()}))
                    raise
                finally:
                    if arm != 'sr':
                        stop(binary)
            (output / 'health-latest.json').write_text(command(['python3', str(health)]))
    (output / 'health-after.json').write_text(command(['python3', str(health)]))


if __name__ == '__main__':
    main()
