"""Run only against the existing private snapshot on the benchmark host."""
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import sys
import time

CASES = ['base simple', 'noest simple', 'plans simple', 'crossplans', 'base cross',
         'child cross', 'detail cross', 'expanded simple', 'interval quarter']
UNIT = 'thindb-ordinary-sql-diagnosis'


def command(args):
    return subprocess.check_output(args, text=True).strip()


def port_free():
    with socket.socket() as sock:
        return sock.connect_ex(('127.0.0.1', 13311)) != 0


def stop(binary):
    state = command(['systemctl', 'show', UNIT, '-p', 'MainPID', '--value'])
    pid = int(state or '0')
    if pid:
        proc = pathlib.Path('/proc', str(pid))
        args = (proc / 'cmdline').read_bytes().split(b'\0')
        if (proc / 'exe').resolve() != binary or b'13311' not in args or b'13310' in args:
            raise RuntimeError('Unexpected candidate process; refusing stop')
        subprocess.run(['sudo', '-n', 'systemctl', 'stop', UNIT], check=True)
    if not port_free():
        raise RuntimeError('Benchmark port remains occupied')


def start(archive, output, data, dop, profile, nodelay=None, trace_joins=False):
    binary = (archive / 'thindb-profile').resolve()
    metadata = json.loads((archive / 'build-metadata.json').read_text())
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    if sha != metadata['binary_sha256']:
        raise RuntimeError('Archived binary checksum mismatch')
    if not data.is_relative_to('/home/ubuntu/wayroll-bench') or 'prod' in data.name:
        raise RuntimeError('Expected private benchmark snapshot')
    for proc in pathlib.Path('/proc').glob('[0-9]*/cmdline'):
        try:
            args = proc.read_bytes().decode().split('\0')
            if '--data-dir' in args and (proc.parent / 'cwd' / args[args.index('--data-dir') + 1]).resolve() == data:
                raise RuntimeError('Snapshot already open')
        except (FileNotFoundError, PermissionError, UnicodeDecodeError):
            pass
    if not port_free():
        raise RuntimeError('Benchmark port occupied')
    memory = dict(line.split(':', 1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines())
    available = int(memory['MemAvailable'].split()[0]) * 1024
    if available < 28 * 2**30:
        raise RuntimeError('Less than 28 GiB available for unchanged 20 GiB ceiling and 8 GiB headroom')
    args = [str(binary), '--data-dir', str(data), '--bind', '127.0.0.1', '--mysql-port', '13311',
            '--pg-port', '0', '--native-port', '0', '--max-dop', str(dop), '--cache-size', '2G',
            '--memory-budget', '16G', '--query-memory-budget', '16G', '--no-compaction']
    if profile:
        args += ['--profile-ops']
    service = ['sudo', '-n', 'systemd-run', '--unit=' + UNIT, '--uid=ubuntu', '--collect',
               '--property=WorkingDirectory=' + str(output), '--property=MemoryMax=' + str(20 * 2**30),
               '--property=MemorySwapMax=0', '--property=OOMPolicy=stop', '--property=OOMScoreAdjust=800',
               '--property=StandardOutput=append:' + str(output / 'server.out'),
               '--property=StandardError=append:' + str(output / 'server.err'),
               '--setenv=THINDB_ZIG_PATH=/opt/thindb/zig/zig', '--setenv=THINDB_REGION_POOL_MB=4096']
    if profile:
        service += ['--setenv=THINDB_MYSQL_PROFILE=1']
    if trace_joins:
        service += ['--setenv=THINDB_TRACE_JOINFUSE=1']
    if nodelay is not None:
        library = pathlib.Path(__file__).with_name('ordinary_sql_nodelay.so')
        if nodelay not in ('0', '1') or not library.is_file():
            raise RuntimeError('Missing diagnostic interposer or invalid setting')
        service += ['--setenv=LD_PRELOAD=' + str(library), '--setenv=DIAG_TCP_NODELAY=' + nodelay]
    began = time.monotonic()
    subprocess.run(service + ['--'] + args, check=True)
    for _ in range(120):
        if not port_free():
            break
        time.sleep(0.5)
    else:
        raise RuntimeError('Candidate not ready')
    receipt = dict(archive_build_metadata=metadata, dop=dop, profile=profile, args=args, binary_sha256=sha,
                   available_before_bytes=available, minimum_headroom_gib=8, startup_seconds=time.monotonic() - began,
                   nodelay=nodelay, service=command(['systemctl', 'show', UNIT, '-p', 'MainPID', '-p', 'MemoryMax']))
    (output / 'service.json').write_text(json.dumps(receipt, indent=2))
    return binary


def main():
    archive, output, data = (pathlib.Path(v).resolve() for v in sys.argv[1:4])
    mode = sys.argv[4] if len(sys.argv) > 4 else 'timing'
    if mode not in ('timing', 'profile', 'transport-off', 'transport-on'):
        raise RuntimeError('Expected timing, profile, transport-off or transport-on')
    password = sys.stdin.readline().rstrip('\r\n')
    if not password:
        raise RuntimeError('Missing StarRocks credential on stdin')
    env = dict(os.environ, SR_PW=password, NODE_ENV='development',
               NODE_OPTIONS='--max-old-space-size=4096 --require /tmp/region-release-quiet-env.cjs')
    output.mkdir(parents=True, exist_ok=True)
    health_before = output / ('health-before-' + mode + '.json')
    if not health_before.exists():
        health_before.write_text(command(['python3', str(archive / 'health.py')]))
    harness = pathlib.Path(__file__).with_suffix('.cjs')
    binary = (archive / 'thindb-profile').resolve()
    if not port_free():
        raise RuntimeError('Benchmark port occupied at start')
    selected = os.environ.get('DIAG_CASES', '').split('|')
    if mode.startswith('transport-') and selected == ['']:
        selected = ['base simple', 'noest simple', 'detail cross', 'crossplans']
    dops = [int(v) for v in os.environ.get('DIAG_DOPS', '12' if mode.startswith('transport-') else '12,16').split(',')]
    for dop_index, dop in enumerate(dops):
        if dop not in (12, 16):
            raise RuntimeError('DOP must be 12 or 16')
        for index, label in enumerate(CASES):
            if selected != [''] and label not in selected:
                continue
            engines = ['sr', 'thin'] if (index + dop_index) % 2 == 0 else ['thin', 'sr']
            if mode.startswith('transport-'):
                engines = ['thin']
            for engine in engines:
                folder = output / f'{mode}-{dop}-{label.replace(" ", "-")}-{engine}'
                if (folder / 'complete.json').exists():
                    continue
                folder.mkdir(exist_ok=True)
                if (folder / 'result.json').exists():
                    raise RuntimeError('Incomplete samples exist; archive them before retrying')
                if not port_free():
                    raise RuntimeError('Benchmark port occupied before block')
                capture = None
                capture_log = None
                try:
                    if engine == 'thin':
                        nodelay = ('1' if mode == 'transport-on' else '0') if mode.startswith('transport-') else None
                        start(archive, folder, data, dop, mode == 'profile', nodelay)
                    if mode.startswith('transport-'):
                        capture_log = (folder / 'packets.txt').open('w')
                        capture = subprocess.Popen(['sudo', '-n', 'tcpdump', '-l', '-tt', '-S', '-n', '-s', '128',
                                                    '-i', 'lo', 'tcp', 'port', '13311'], stdout=capture_log, stderr=capture_log)
                        time.sleep(0.3)
                        if capture.poll() is not None:
                            raise RuntimeError('Packet capture failed')
                    with (folder / 'client.log').open('w') as log:
                        subprocess.run(['node', str(harness), str(archive), str(folder), engine, str(dop), label,
                                        'profile' if mode == 'profile' else 'timing'],
                                       env=env, cwd=(archive / 'app').resolve(), stdout=log, stderr=subprocess.STDOUT,
                                       timeout=300, check=True)
                    if engine == 'thin':
                        if mode.startswith('transport-'):
                            socket_lines = [line for line in (folder / 'server.err').read_text().splitlines() if '[diagnostic-socket]' in line]
                            if not socket_lines or any(f'requested={nodelay} after={nodelay} rc=0' not in line for line in socket_lines):
                                raise RuntimeError('TCP_NODELAY experiment did not take effect')
                        state = command(['systemctl', 'show', UNIT, '-p', 'MainPID', '-p', 'MemoryPeak', '-p', 'MemoryCurrent', '-p', 'ControlGroup'])
                        (folder / 'memory.txt').write_text(state)
                        cg = dict(line.split('=', 1) for line in state.splitlines())['ControlGroup']
                        (folder / 'memory-events.txt').write_text(pathlib.Path('/sys/fs/cgroup' + cg + '/memory.events').read_text())
                    (folder / 'complete.json').write_text(json.dumps({'order': engines, 'completed_at': time.time()}))
                    print(json.dumps({'mode': mode, 'dop': dop, 'case': label, 'engine': engine, 'complete': True}), flush=True)
                finally:
                    if capture is not None and capture.poll() is None:
                        subprocess.run(['sudo', '-n', 'kill', '-INT', str(capture.pid)], check=True)
                        capture.wait(timeout=10)
                    if capture_log is not None:
                        capture_log.close()
                    if engine == 'thin':
                        stop(binary)
    (output / ('health-after-' + mode + '.json')).write_text(command(['python3', str(archive / 'health.py')]))


if __name__ == '__main__':
    main()
