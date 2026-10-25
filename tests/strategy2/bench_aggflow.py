"""Offline RSS benchmark; ZIP/CSV stay in RAM. Run directly, outside pytest.

From the repo: python tests/strategy2/bench_aggflow.py --csv-mib 2048 --jobs 2
The direct invocation requires PYTHONPATH=.
"""

import argparse
import hashlib
from io import BytesIO
import random
from pathlib import Path
from uuid import uuid4
import resource
import sys
import time
from zipfile import ZipFile, ZIP_DEFLATED

import httpx

from strategy2.download import Download, run_downloads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv-mib', type=int, default=512)
    parser.add_argument('--jobs', type=int, default=2)
    args = parser.parse_args()
    rng = random.Random(42)
    # Vary decimals/IDs enough to exercise realistic decompression and parsing.
    block = ''.join(
        f'{i},{rng.randrange(1000000, 9000000) / 100:.2f},'
        f'{rng.randrange(1, 1000000) / 1000000:.6f},{i * 2},{i * 2 + 1},'
        f'{1672531200000 + (i % 44640) * 60000 + i % 60000},'
        f'{str(bool(i % 2)).lower()}\n'
        for i in range(131072)).encode()
    target = args.csv_mib * 1024 * 1024
    started = time.monotonic()
    with BytesIO() as source:
        written = 0
        with ZipFile(source, 'w', ZIP_DEFLATED, compresslevel=1) as archive:
            with archive.open('synthetic.csv', 'w', force_zip64=True) as member:
                while written < target:
                    member.write(block)
                    written += len(block)
        size = source.tell()
        build_seconds = time.monotonic() - started
        view = source.getbuffer()
        digest = hashlib.sha256()
        for offset in range(0, size, 1024 * 1024):
            digest.update(view[offset:offset + 1024 * 1024])
        expected = digest.hexdigest()

        class Chunks(httpx.SyncByteStream):
            def __iter__(self):
                for offset in range(0, size, 1024 * 1024):
                    yield bytes(view[offset:offset + 1024 * 1024])

        def handler(request):
            if request.url.path.endswith('.CHECKSUM'):
                return httpx.Response(200, content=expected.encode())
            return httpx.Response(200, stream=Chunks())

        root = Path(__file__).parent / '.tmp' / f'bench-{uuid4().hex}'
        tasks = [Download('aggflow', f'TEST{i}', '2023-01',
                          f'https://fixture.invalid/TEST{i}.zip',
                          f'binance/aggflow/TEST{i}/2023-01.parquet') for i in range(args.jobs)]
        started = time.monotonic()
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            rows = run_downloads(tasks, root, args.jobs, client, aggflow_jobs=args.jobs)
        assert all(row['status'] == 'downloaded' and row['bytes'] == size for row in rows), rows
        del view
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_bytes = rss if sys.platform == 'darwin' else rss * 1024
    print(f'csv_bytes_per_job={written} zip_bytes_per_job={size} jobs={args.jobs} '
          f'peak_rss_mib={rss_bytes / 1024**2:.1f} '
          f'build_seconds={build_seconds:.2f} download_aggregate_seconds={time.monotonic() - started:.2f} '
          f'sha256={digest.hexdigest()}', flush=True)


if __name__ == '__main__':
    main()
