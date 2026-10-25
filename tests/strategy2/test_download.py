"""Offline planning, integrity, resume and paged DVOL tests."""

import csv
from datetime import date
import hashlib
from io import StringIO
from pathlib import Path

import httpx
import pytest

from strategy2 import download as module
from strategy2.download import (BINANCE, DATASETS, HOUR_MS, USER_AGENT, download,
                                main, plan_downloads, run_downloads)


def plan(start="2026-09-28", end="2026-10-02", datasets=DATASETS, coins=("BTC",)):
    return plan_downloads(date.fromisoformat(start), date.fromisoformat(end), coins, datasets)


def checksum(content):
    return f"{hashlib.sha256(content).hexdigest()}  archive.zip\n".encode()


def client_for(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def archive():
    return plan("2026-09-01", "2026-09-30", ("perp_klines",))[0]


def test_monthly_daily_split_and_exact_urls():
    items = plan()
    assert len(items) == 23
    expected = {
        "perp_klines": "futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09.zip",
        "spot_klines": "spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09.zip",
        "premium_index": "futures/um/monthly/premiumIndexKlines/BTCUSDT/1m/BTCUSDT-1m-2026-09.zip",
        "funding_rate": "futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-09.zip",
        "metrics": "futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-09-28.zip",
        "book_depth": "futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-09-28.zip",
    }
    for dataset, suffix in expected.items():
        item = next(item for item in items if item.dataset == dataset)
        assert item.url == BINANCE + suffix
        assert item.path == "binance/" + suffix
    for dataset in ("perp_klines", "spot_klines", "premium_index"):
        subset = [item for item in items if item.dataset == dataset]
        assert [item.period for item in subset] == ["2026-09", "2026-10-01", "2026-10-02"]
        assert all("/daily/" in item.url for item in subset[1:])
    assert [item.period for item in items if item.dataset == "funding_rate"] == ["2026-09", "2026-10"]


@pytest.mark.parametrize("start,end,periods", [
    ("2024-02-01", "2024-02-29", ["2024-02"]),
    ("2024-02-28", "2024-02-29", ["2024-02"]),
    ("2024-02-28", "2024-03-01", ["2024-02", "2024-03-01"]),
    ("2024-02-28", "2024-02-28", ["2024-02-28"]),
    ("2025-12-31", "2026-01-01", ["2025-12", "2026-01-01"]),
    ("2026-01-01", "2026-01-31", ["2026-01"]),
])
def test_range_edges_including_leap_year_and_partial_first_month(start, end, periods):
    assert [item.period for item in plan(start, end, ("perp_klines",))] == periods


def test_daily_ranges_are_inclusive_and_dvol_only_supports_btc_eth():
    items = plan("2024-02-28", "2024-03-01", ("metrics", "dvol"), ("BTC", "ETH", "SOL", "XRP", "DOGE"))
    assert [item.period for item in items if item.dataset == "metrics" and item.coin == "BTC"] == [
        "2024-02-28", "2024-02-29", "2024-03-01"]
    dvol = [item for item in items if item.dataset == "dvol"]
    assert len(dvol) == 4 and {item.coin for item in dvol} == {"BTC", "ETH"}
    assert dvol[0].path == "deribit/dvol/BTC/1h/2024-02.csv"
    assert dvol[0].end_ms - dvol[0].start_ms == 48 * HOUR_MS
    assert dvol[1].end_ms - dvol[1].start_ms == 24 * HOUR_MS
    assert int(httpx.URL(dvol[0].url).params["end_timestamp"]) == dvol[0].end_ms - 1
    assert httpx.URL(dvol[0].url).params["resolution"] == "3600"


def test_duplicate_selections_do_not_schedule_duplicate_files():
    items = plan(datasets=("metrics", "metrics"), coins=("BTC", "BTC"))
    assert len(items) == 5


@pytest.mark.parametrize("kwargs", [
    {"start": "2026-10-02", "end": "2026-10-01"},
    {"coins": ("BNB",)}, {"datasets": ("bad",)}, {"coins": ()}, {"datasets": ()},
])
def test_invalid_plan_rejected(kwargs):
    with pytest.raises(ValueError):
        plan(**kwargs)


def test_checksum_pass_resume_and_coverage(tmp_path):
    item = archive()
    content = b"public archive bytes"
    requests = []

    def handler(request):
        assert request.headers["user-agent"] == USER_AGENT
        requests.append(str(request.url))
        return httpx.Response(200, content=checksum(content) if str(request.url).endswith(".CHECKSUM") else content)

    with client_for(handler) as client:
        first = run_downloads([item], tmp_path, client=client)
        assert first[0]["status"] == "downloaded"
        path = tmp_path / item.path
        before = path.stat().st_mtime_ns
        assert path.read_bytes() == content
        assert Path(str(path) + ".CHECKSUM").read_bytes() == checksum(content)
        second = run_downloads([item], tmp_path, client=client)
    assert second[0]["status"] == "present"
    assert second[0]["bytes"] == len(content)
    assert path.stat().st_mtime_ns == before
    assert requests == [item.url + ".CHECKSUM", item.url, item.url + ".CHECKSUM"]
    rows = list(csv.DictReader((tmp_path / "coverage.csv").open()))
    assert rows == [{**second[0], "bytes": str(len(content))}]
    assert not list(tmp_path.rglob("*.part"))


@pytest.mark.parametrize("checksum_body", [checksum(b"different bytes"), b"not a checksum", b"\xff", b""])
def test_bad_checksum_never_publishes_archive(tmp_path, checksum_body):
    item = archive()
    with client_for(lambda request: httpx.Response(200, content=(
            checksum_body if str(request.url).endswith(".CHECKSUM") else b"downloaded bytes"))) as client:
        row = download(item, tmp_path, client)
    assert row["status"] == "checksum_failed"
    assert not (tmp_path / item.path).exists()


@pytest.mark.parametrize("missing_checksum", [True, False])
def test_404_is_missing_without_retries(tmp_path, missing_checksum):
    item = archive()
    requests = []

    def handler(request):
        requests.append(str(request.url))
        if str(request.url).endswith(".CHECKSUM") and not missing_checksum:
            return httpx.Response(200, content=checksum(b"archive"))
        return httpx.Response(404)

    with client_for(handler) as client:
        row = download(item, tmp_path, client)
    assert row["status"] == "missing"
    assert len(requests) == (1 if missing_checksum else 2)
    assert not (tmp_path / item.path).exists()


def test_existing_conflict_is_never_overwritten_or_redownloaded(tmp_path):
    item = archive()
    path = tmp_path / item.path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"keep original")
    before = path.stat().st_mtime_ns

    def handler(request):
        assert str(request.url) == item.url + ".CHECKSUM"
        return httpx.Response(200, content=checksum(b"remote revision"))

    with client_for(handler) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert path.read_bytes() == b"keep original"
    assert path.stat().st_mtime_ns == before


def test_existing_checksum_conflict_is_preserved(tmp_path):
    item = archive()
    path = tmp_path / item.path
    path.parent.mkdir(parents=True)
    sidecar = Path(str(path) + ".CHECKSUM")
    sidecar.write_bytes(checksum(b"old bytes"))
    with client_for(lambda request: httpx.Response(200, content=checksum(b"new bytes"))) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert sidecar.read_bytes() == checksum(b"old bytes")
    assert not path.exists()


def test_concurrent_publication_cannot_overwrite_a_conflict(tmp_path, monkeypatch):
    item = archive()
    original_link = module.os.link

    def racing_link(source, destination):
        if str(destination).endswith(".zip"):
            destination.write_bytes(b"another process won")
        original_link(source, destination)

    monkeypatch.setattr(module.os, "link", racing_link)
    with client_for(lambda request: httpx.Response(200, content=(
            checksum(b"remote") if str(request.url).endswith(".CHECKSUM") else b"remote"))) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert (tmp_path / item.path).read_bytes() == b"another process won"


def test_transient_http_and_transport_errors_retry_with_backoff(tmp_path, monkeypatch):
    pauses = []
    monkeypatch.setattr(module.time, "sleep", pauses.append)
    item = archive()
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("temporary", request=request)
        if calls == 2:
            return httpx.Response(429)
        if calls == 3:
            return httpx.Response(503)
        return httpx.Response(200, content=checksum(b"ok") if str(request.url).endswith(".CHECKSUM") else b"ok")

    with client_for(handler) as client:
        assert download(item, tmp_path, client)["status"] == "downloaded"
    assert pauses == [.5, 1., 2.]
    assert calls == 5


def test_permanent_http_errors_are_not_retried(tmp_path):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(403)

    with client_for(handler) as client, pytest.raises(httpx.HTTPStatusError):
        download(archive(), tmp_path, client)
    assert len(requests) == 1


def dvol_points(item):
    return [[stamp, 40., 42., 39., 41.] for stamp in range(item.start_ms, item.end_ms, HOUR_MS)]


def test_dvol_paging_sorted_csv_and_verified_resume(tmp_path):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    points = dvol_points(item)
    ends = []

    def handler(request):
        assert request.url.params["currency"] == "BTC"
        assert request.url.params["resolution"] == "3600"
        ends.append(int(request.url.params["end_timestamp"]))
        # Latest page first, with a duplicate point on the second page.
        if len(ends) == 1:
            result = {"data": points[12:][::-1], "continuation": points[12][0]}
        else:
            result = {"data": points[:13][::-1], "continuation": None}
        return httpx.Response(200, json={"result": result})

    with client_for(handler) as client:
        row = download(item, tmp_path, client)
        assert row["status"] == "downloaded"
        path = tmp_path / item.path
        before = path.stat().st_mtime_ns
        assert download(item, tmp_path, client)["status"] == "present"
    assert ends == [item.end_ms - 1, points[12][0]]
    csv_rows = list(csv.DictReader(StringIO(path.read_text())))
    assert list(csv_rows[0]) == ["ts_ms", "open", "high", "low", "close"]
    assert [int(row["ts_ms"]) for row in csv_rows] == [point[0] for point in points]
    assert all(float(row["close"]) == 41 for row in csv_rows)
    assert path.stat().st_mtime_ns == before
    assert Path(str(path) + ".CHECKSUM").read_text().startswith(hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.mark.parametrize("kind", ["empty", "gap", "404"])
def test_dvol_incomplete_history_is_missing(tmp_path, kind):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    points = [] if kind == "empty" else dvol_points(item)[1:]
    with client_for(lambda request: httpx.Response(404) if kind == "404" else
                    httpx.Response(200, json={"result": {"data": points, "continuation": None}})) as client:
        assert download(item, tmp_path, client)["status"] == "missing"
    assert not (tmp_path / item.path).exists()


@pytest.mark.parametrize("kind", ["changed", "expanded", "no_checksum"])
def test_dvol_existing_conflict_is_preserved(tmp_path, kind):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    path = tmp_path / item.path
    if kind == "no_checksum":
        path.parent.mkdir(parents=True)
        path.write_bytes(b"unverified CSV")
    else:
        with client_for(lambda request: httpx.Response(200, json={
                "result": {"data": dvol_points(item), "continuation": None}})) as client:
            assert download(item, tmp_path, client)["status"] == "downloaded"
        if kind == "changed":
            path.write_bytes(b"changed CSV")
        else:
            item = plan("2026-09-29", "2026-09-30", ("dvol",))[0]
    before = path.read_bytes()
    with client_for(lambda request: pytest.fail("verified resume must not fetch DVOL")) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert path.read_bytes() == before


def test_dvol_nonadvancing_continuation_is_rejected(tmp_path):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    with client_for(lambda request: httpx.Response(200, json={"result": {
            "data": [], "continuation": item.end_ms - 1}})) as client:
        with pytest.raises(ValueError, match="continuation"):
            download(item, tmp_path, client)


def test_dry_run_has_no_network_or_filesystem_writes(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(module, "run_downloads", lambda *a, **kw: pytest.fail("dry run downloaded"))
    root = tmp_path / "not-created"
    assert main(["--root", str(root), "--from", "2026-09-28", "--to", "2026-10-02",
                 "--coins", "BTC", "--dry-run"]) == 0
    output = capsys.readouterr().out
    assert "23 files planned" in output
    assert archive().url in output
    assert not root.exists()


def test_cli_summarizes_coverage_and_fails_on_integrity_errors(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(module, "run_downloads", lambda *a, **kw: [
        {"dataset": "perp_klines", "status": "downloaded", "bytes": 50},
        {"dataset": "perp_klines", "status": "missing", "bytes": 0},
        {"dataset": "metrics", "status": "checksum_failed", "bytes": 10}])
    assert main(["--root", str(tmp_path), "--from", "2026-09-28", "--to", "2026-10-02"]) == 1
    output = capsys.readouterr().out
    assert "perp_klines: bytes=50, downloaded=1, missing=1" in output
    assert "metrics: bytes=10, checksum_failed=1" in output


@pytest.mark.parametrize("jobs", [0, -1])
def test_invalid_job_count_rejected_before_work(tmp_path, jobs):
    with pytest.raises(ValueError, match="positive"):
        run_downloads([], tmp_path, jobs=jobs)
