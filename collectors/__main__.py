"""Run one exchange's collectors until SIGINT/SIGTERM.

    uv run python -m collectors binance|okx|kraken|coinbase|bybit|deribit

SIGHUP replaces every connection make-before-break (same procedure as the midnight rotation).
"""

import argparse
import asyncio
import logging
import signal
import time

from collectors import binance, bybit, coinbase, deribit, kraken, okx
from collectors.common.config import data_dir, kafka_bootstrap, load_config
from collectors.common.runner import run_exchange
from collectors.common.session import Session
from collectors.common.sinks import KafkaSink

HANDLERS = {"binance": binance.handler_for, "okx": okx.handler_for, "kraken": kraken.handler_for,
            "coinbase": coinbase.handler_for, "bybit": bybit.handler_for}


def setup_logging() -> None:
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def _run(exchange: str) -> int:
    cfg = load_config()
    sink = KafkaSink(kafka_bootstrap(), f"collector-{exchange}", cfg["kafka"]["producer_buffer_kbytes"])
    session = Session(exchange, data_dir() / "state")
    stop, rotate = asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    loop.add_signal_handler(signal.SIGHUP, rotate.set)
    if exchange == "deribit":
        clean = await deribit.run_poller(sink, cfg, session, stop, rotate)
        return 0 if clean else 1
    stats = await run_exchange(exchange, HANDLERS[exchange], sink, cfg, session, stop, rotate)
    return 0 if stats.clean_shutdown else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("exchange", choices=sorted([*HANDLERS, "deribit"]))
    args = parser.parse_args()
    setup_logging()
    raise SystemExit(asyncio.run(_run(args.exchange)))


if __name__ == "__main__":
    main()
