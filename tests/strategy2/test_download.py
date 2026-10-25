"""Offline planning, integrity, resume and paged DVOL tests."""

import csv
from datetime import date
import hashlib
from io import StringIO
from pathlib import Path
import subprocess
import sys

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
    assert len(items) == 28
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
    assert [item.period for item in items if item.dataset == "funding_rate"] == ["2026-09", "2026-10-01_2026-10-02"]


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
    assert rows == [{**second[0], "bytes": str(len(content)), "missing_hours": "0"}]
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


@pytest.mark.parametrize("kind", ["empty", "404"])
def test_dvol_absent_history_is_missing(tmp_path, kind):
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
    assert "28 files planned" in output
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


def test_errors_are_isolated_coverage_is_written_and_rerun_retries(tmp_path, monkeypatch):
    items = plan("2026-09-28", "2026-09-28", ("perp_klines", "metrics", "dvol"))
    monkeypatch.setattr(module.time, "sleep", lambda delay: None)
    calls = []

    def failing(request):
        calls.append(str(request.url))
        if request.url.host == "www.deribit.com":
            raise httpx.ReadTimeout("slow server\nplease retry", request=request)
        if "/metrics/" in str(request.url):
            return httpx.Response(404)
        return httpx.Response(200, content=checksum(b"ok") if str(request.url).endswith(".CHECKSUM") else b"ok")

    with client_for(failing) as client:
        rows = run_downloads(items, tmp_path, client=client)
    assert [row["status"] for row in rows] == ["downloaded", "missing", "error"]
    assert rows[0]["error"] == rows[1]["error"] == ""
    assert rows[2]["error"] == "ReadTimeout: slow server please retry"
    assert calls.count(items[2].url) == 4
    with (tmp_path / "coverage.csv").open() as source:
        coverage = list(csv.DictReader(source))
    assert len(coverage) == 3
    assert coverage[2]["status"] == "error" and coverage[2]["error"] == rows[2]["error"]
    retry_calls = []

    def recovered(request):
        retry_calls.append(str(request.url))
        if request.url.host == "www.deribit.com":
            return httpx.Response(200, json={"result": {"data": dvol_points(items[2]), "continuation": None}})
        return httpx.Response(200, content=checksum(b"ok") if str(request.url).endswith(".CHECKSUM") else b"ok")

    with client_for(recovered) as client:
        rows = run_downloads(items, tmp_path, client=client)
    assert [row["status"] for row in rows] == ["present", "downloaded", "downloaded"]
    assert items[0].url not in retry_calls
    assert items[1].url in retry_calls and items[2].url in retry_calls
    assert all(row["error"] == "" for row in rows)


def test_unexpected_item_errors_are_short_and_do_not_stop_other_items(tmp_path, monkeypatch):
    items = plan("2026-09-28", "2026-09-29", ("metrics",))

    def handler(item, root, client):
        if item == items[0]:
            raise RuntimeError("x" * 1000)
        return module._row(item, "missing")

    monkeypatch.setattr(module, "download", handler)
    with client_for(lambda request: pytest.fail("unexpected HTTP")) as client:
        rows = run_downloads(items, tmp_path, client=client)
    assert [row["status"] for row in rows] == ["error", "missing"]
    assert len(rows[0]["error"]) == 240
    assert (tmp_path / "coverage.csv").exists()


def test_dvol_gap_is_published_with_missing_hour_count(tmp_path):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    points = dvol_points(item)[1:]
    with client_for(lambda request: httpx.Response(200, json={
            "result": {"data": points, "continuation": None}})) as client:
        row = run_downloads([item], tmp_path, client=client)[0]
    assert row["status"] == "partial" and row["missing_hours"] == 1
    path = tmp_path / item.path
    assert len(list(csv.DictReader(StringIO(path.read_text())))) == 23
    sidecar = Path(str(path) + ".CHECKSUM").read_text().splitlines()
    assert sidecar[0].split()[0] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert sidecar[1] == f"{item.start_ms} {item.end_ms}"
    with (tmp_path / "coverage.csv").open() as source:
        coverage = next(csv.DictReader(source))
    assert coverage["missing_hours"] == "1" and coverage["error"] == ""


@pytest.mark.parametrize("kind", ["complete", "partial", "changed", "subset", "same"])
def test_dvol_upgrade_requires_strict_superset_and_identical_overlap(tmp_path, kind):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    points = dvol_points(item)
    old = points[2:]
    new = points if kind != "partial" else points[1:]
    if kind == "changed":
        new = [point.copy() for point in new]
        new[2][4] += 1
    elif kind == "subset":
        new = points[3:]
    elif kind == "same":
        new = old
    path = tmp_path / item.path

    def fetch(values):
        with client_for(lambda request: httpx.Response(200, json={
                "result": {"data": values, "continuation": None}})) as client:
            return download(item, tmp_path, client)

    assert fetch(old)["status"] == "partial"
    before = path.read_bytes()
    checksum_path = Path(str(path) + ".CHECKSUM")
    before_checksum = checksum_path.read_bytes()
    result = fetch(new)
    if kind in ("changed", "subset", "same"):
        assert result["status"] == "conflict"
        assert path.read_bytes() == before and checksum_path.read_bytes() == before_checksum
    else:
        assert result["status"] == ("downloaded" if kind == "complete" else "partial")
        assert result["missing_hours"] == (0 if kind == "complete" else 1)
        rows = list(csv.DictReader(StringIO(path.read_text())))
        assert len(rows) == len(new)
        assert checksum_path.read_text().split()[0] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_funding_partial_final_month_uses_rest_and_full_months_use_archives():
    items = plan(datasets=("funding_rate",))
    assert len(items) == 2
    assert "/monthly/fundingRate/" in items[0].url
    item = items[1]
    assert item.path == ("binance/futures/um/rest/fundingRate/BTCUSDT/"
                         "BTCUSDT-fundingRate-2026-10-01_2026-10-02.csv")
    params = httpx.URL(item.url).params
    assert httpx.URL(item.url).host == "fapi.binance.com"
    assert dict(params) == {"symbol": "BTCUSDT", "startTime": str(item.start_ms),
                            "endTime": str(item.end_ms - 1), "limit": "1000"}
    assert item.end_ms - item.start_ms == 48 * HOUR_MS
    assert plan("2026-09-28", "2026-09-30", ("funding_rate",))[0].path.endswith("2026-09.zip")
    assert plan("2026-09-28", "2026-09-29", ("funding_rate",))[0].period == "2026-09-28_2026-09-29"


def funding_points(item, count=2):
    return [{"symbol": "BTCUSDT", "fundingTime": item.start_ms + i,
             "fundingRate": "-0.00001000", "markPrice": "00065000.12000000"}
            for i in range(count)]


def test_funding_paging_decimal_strings_checksum_and_present(tmp_path):
    item = plan("2026-10-01", "2026-10-02", ("funding_rate",))[0]
    points = funding_points(item, 1001)
    starts = []

    def handler(request):
        assert request.url.params["limit"] == "1000"
        assert int(request.url.params["endTime"]) == item.end_ms - 1
        start = int(request.url.params["startTime"])
        starts.append(start)
        page = points[:1000][::-1] if start == item.start_ms else points[1000:]
        return httpx.Response(200, json=page)

    with client_for(handler) as client:
        assert download(item, tmp_path, client)["status"] == "downloaded"
        path = tmp_path / item.path
        content = path.read_bytes()
        before = path.stat().st_mtime_ns
        assert download(item, tmp_path, client)["status"] == "present"
    assert starts == [item.start_ms, item.start_ms + 1000] * 2
    rows = list(csv.DictReader(StringIO(content.decode())))
    assert list(rows[0]) == ["funding_time_ms", "funding_rate", "mark_price"]
    assert len(rows) == 1001
    assert [int(row["funding_time_ms"]) for row in rows] == [point["fundingTime"] for point in points]
    assert all(row["funding_rate"] == "-0.00001000" and row["mark_price"] == "00065000.12000000" for row in rows)
    assert path.stat().st_mtime_ns == before
    sidecar = Path(str(path) + ".CHECKSUM").read_text().splitlines()
    assert sidecar[0].split()[0] == hashlib.sha256(content).hexdigest()
    assert sidecar[1] == f"{item.start_ms} {item.end_ms}"


@pytest.mark.parametrize("kind", ["rate", "price", "removed", "added", "empty", "corrupt"])
def test_changed_funding_snapshot_is_conflict_and_preserved(tmp_path, kind):
    item = plan("2026-10-01", "2026-10-02", ("funding_rate",))[0]
    points = funding_points(item)
    with client_for(lambda request: httpx.Response(200, json=points)) as client:
        assert download(item, tmp_path, client)["status"] == "downloaded"
    path = tmp_path / item.path
    if kind == "corrupt":
        path.write_bytes(b"corrupt file")
    before = path.read_bytes()
    before_checksum = Path(str(path) + ".CHECKSUM").read_bytes()
    if kind == "rate":
        points[0]["fundingRate"] = "0.00002000"
    elif kind == "price":
        points[0]["markPrice"] = "65000.13"
    elif kind == "removed":
        points = points[1:]
    elif kind == "added":
        points = funding_points(item, 3)
    elif kind == "empty":
        points = []
    with client_for(lambda request: httpx.Response(200, json=points)) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert path.read_bytes() == before
    assert Path(str(path) + ".CHECKSUM").read_bytes() == before_checksum


@pytest.mark.parametrize("status", ["error", "conflict", "checksum_failed", "partial", "missing"])
def test_cli_exit_status_for_each_coverage_outcome(tmp_path, monkeypatch, capsys, status):
    item = archive()
    monkeypatch.setattr(module, "run_downloads", lambda *args, **kwargs: [module._row(item, status)])
    result = main(["--root", str(tmp_path), "--from", "2026-09-28", "--to", "2026-10-02"])
    assert result == int(status in ("error", "conflict", "checksum_failed"))
    assert status in capsys.readouterr().out


def test_dry_run_broken_pipe_exits_cleanly(tmp_path):
    process = subprocess.Popen([sys.executable, "-m", "strategy2.download", "--root", str(tmp_path),
                                "--from", "2023-01-01", "--to", "2026-10-20", "--dry-run"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert b"files planned" in process.stdout.readline()
        process.stdout.close()
        assert process.wait(timeout=15) == 0
        assert process.stderr.read() == b""
    finally:
        process.stderr.close()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("start,end,hours", [
    ("2023-01-01", "2023-01-31", 744),
    ("2023-06-01", "2023-06-30", 720),
])
def test_complete_dvol_month_hour_counts(tmp_path, start, end, hours):
    item = plan(start, end, ("dvol",))[0]
    points = dvol_points(item)
    assert len(points) == hours
    with client_for(lambda request: httpx.Response(200, json={
            "result": {"data": points, "continuation": None}})) as client:
        row = download(item, tmp_path, client)
    assert row["status"] == "downloaded" and row["missing_hours"] == 0
    with (tmp_path / item.path).open() as source:
        assert len(list(csv.DictReader(source))) == hours


def test_dvol_does_not_replace_unverified_file_created_during_fetch(tmp_path):
    item = plan("2026-09-30", "2026-09-30", ("dvol",))[0]
    path = tmp_path / item.path

    def handler(request):
        path.write_bytes(b"another writer's snapshot")
        return httpx.Response(200, json={"result": {"data": dvol_points(item), "continuation": None}})

    with client_for(handler) as client:
        assert download(item, tmp_path, client)["status"] == "conflict"
    assert path.read_bytes() == b"another writer's snapshot"



def coinbase_day(coin="BTC", day="2026-01-01"):
    return plan(day, day, ("coinbase_klines",), (coin,))[0]


@pytest.fixture
def no_coinbase_wait(monkeypatch):
    monkeypatch.setattr(module, "_coinbase_throttle", lambda: None)


def test_coinbase_plan_past_utc_days_only():
    from datetime import datetime, timedelta, timezone
    today = datetime.now(timezone.utc).date()
    items = plan_downloads(today - timedelta(days=1), today + timedelta(days=2),
                           ("XRP",), ("coinbase_klines",))
    assert len(items) == 1
    assert items[0].end_ms == module._ms(today)
    assert items[0].path == f"coinbase/candles_1m/XRP-USD/{items[0].period}.csv"
    assert not plan_downloads(today, today, ("BTC",), ("coinbase_klines",))


def test_coinbase_five_pages_sorted_decimal_lexemes_sidecar_and_resume(tmp_path, no_coinbase_wait):
    from datetime import datetime
    item = coinbase_day()
    windows = []
    def handler(request):
        assert request.url.path == '/products/BTC-USD/candles'
        assert request.url.params['granularity'] == '60'
        lo = int(datetime.fromisoformat(request.url.params['start']).timestamp() * 1000)
        hi = int(datetime.fromisoformat(request.url.params['end']).timestamp() * 1000) + 1
        windows.append((lo, hi))
        # Numeric JSON values, not strings: retain trailing zeros and exponent.
        rows = [f'[{stamp//1000},90.000,110.0000,100.0100,101.02000,1.2300e-3]'
                for stamp in range(lo, hi, 60_000)][::-1]
        return httpx.Response(200, content=('[' + ','.join(rows) + ']').encode())
    with client_for(handler) as client:
        assert download(item, tmp_path, client)['status'] == 'downloaded'
        path = tmp_path / item.path
        before = path.stat().st_mtime_ns
        assert download(item, tmp_path, client)['status'] == 'present'
    expected = [(lo, min(lo + 300 * 60_000, item.end_ms))
                for lo in range(item.start_ms, item.end_ms, 300 * 60_000)]
    assert len(expected) == 5 and windows == expected * 2
    rows = list(csv.DictReader(StringIO(path.read_text())))
    assert len(rows) == 1440
    assert [int(row['ts_ms']) for row in rows] == list(range(item.start_ms, item.end_ms, 60_000))
    assert list(rows[0]) == ['ts_ms', 'open', 'high', 'low', 'close', 'volume']
    assert rows[0] == dict(ts_ms=str(item.start_ms), open='100.0100', high='110.0000',
                           low='90.000', close='101.02000', volume='1.2300e-3')
    assert path.stat().st_mtime_ns == before
    lines = Path(str(path) + '.CHECKSUM').read_text().splitlines()
    assert lines[0].split()[0] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert lines[1] == f'{item.start_ms} {item.end_ms}'


@pytest.mark.parametrize('coin,day', [('BTC', '2026-01-01'), ('XRP', '2023-07-12')])
def test_coinbase_empty_days_are_present_files_and_coverage(tmp_path, no_coinbase_wait, coin, day):
    item = coinbase_day(coin, day)
    calls = []
    def handler(request):
        calls.append(request.url)
        return httpx.Response(200, json=[])
    with client_for(handler) as client:
        first = run_downloads([item], tmp_path, client=client)[0]
        second = run_downloads([item], tmp_path, client=client)[0]
    assert first['status'] == 'downloaded' and second['status'] == 'present'
    assert first['error'] == second['error'] == 'empty'
    assert len(calls) == 10
    assert (tmp_path / item.path).read_bytes() == b'ts_ms,open,high,low,close,volume\r\n'
    assert next(csv.DictReader((tmp_path / 'coverage.csv').open()))['error'] == 'empty'


@pytest.mark.parametrize('kind', ['changed', 'removed', 'added', 'corrupt', 'range'])
def test_coinbase_conflict_preserves_snapshot(tmp_path, no_coinbase_wait, kind):
    from datetime import datetime
    item = coinbase_day()
    points = [[item.start_ms // 1000, '90.00', '110.00', '100.00', '101.00', '2.0']]
    def handler(request):
        lo = int(datetime.fromisoformat(request.url.params['start']).timestamp())
        hi = int(datetime.fromisoformat(request.url.params['end']).timestamp()) + 1
        return httpx.Response(200, json=[p for p in points if lo <= p[0] < hi])
    with client_for(handler) as client:
        assert download(item, tmp_path, client)['status'] == 'downloaded'
        path = tmp_path / item.path
        sidecar = Path(str(path) + '.CHECKSUM')
        if kind == 'corrupt':
            path.write_bytes(b'bad')
        elif kind == 'range':
            sidecar.write_text(sidecar.read_text().replace(str(item.end_ms), str(item.end_ms + 60_000)))
        elif kind == 'changed':
            points[0][4] = '102.00'
        elif kind == 'removed':
            points.clear()
        elif kind == 'added':
            points.append([item.start_ms // 1000 + 60, '90', '110', '100', '101', '2'])
        before, check = path.read_bytes(), sidecar.read_bytes()
        assert download(item, tmp_path, client)['status'] == 'conflict'
    assert path.read_bytes() == before and sidecar.read_bytes() == check


def test_coinbase_429_backoff_then_empty_page_does_not_stop_paging(tmp_path, no_coinbase_wait, monkeypatch):
    calls, pauses = [], []
    monkeypatch.setattr(module.time, 'sleep', pauses.append)
    def handler(request):
        calls.append(request.url)
        return httpx.Response(429 if len(calls) == 1 else 200, json=[])
    with client_for(handler) as client:
        assert download(coinbase_day(), tmp_path, client)['status'] == 'downloaded'
    assert len(calls) == 6 and calls[0] == calls[1]
    assert pauses == [.5]


def test_coinbase_rate_limiter_shared_across_requests_and_retries(monkeypatch):
    now, starts = [0.], []
    monkeypatch.setattr(module, '_coinbase_last_request', float('-inf'))
    monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(module.time, 'sleep', lambda delay: now.__setitem__(0, now[0] + delay))
    def handler(request):
        starts.append(now[0])
        return httpx.Response(200, json=[])
    with client_for(handler) as client:
        for _ in range(12):
            module._get(client, coinbase_day().url)
    assert starts == [i * .125 for i in range(12)]
