"""Load config/markets.yaml and .env, and expand them into streams and connections."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "markets.yaml"

load_dotenv(ROOT / ".env")

BOOK_FEEDS = {"l2", "l3"}
TRADE_FEEDS = {"trades", "trades_agg"}


@dataclass(frozen=True)
class Stream:
    """One (exchange, market, feed, symbol) stream = one Kafka (topic, partition) = one daily file."""

    exchange: str
    market: str
    feed: str
    asset: str
    symbol: str
    channel: str
    topic: str
    partition: int

    @property
    def key(self) -> str:
        return f"{self.topic}/{self.partition}"

    @property
    def is_book(self) -> bool:
        return self.feed in BOOK_FEEDS

    @property
    def file_symbol(self) -> str:
        return self.symbol.replace("/", "-")


@dataclass
class ConnSpec:
    """One WebSocket connection and the streams it carries."""

    exchange: str
    market: str
    name: str
    url: str
    streams: list[Stream]
    options: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        return f"{self.exchange}.{self.market}.{self.name}"


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def data_dir() -> Path:
    path = Path(os.environ.get("DATA_DIR", "data"))
    return path if path.is_absolute() else ROOT / path


def topic_name(cfg: dict, exchange: str, market: str, feed: str) -> str:
    return f"raw.{exchange}.{market}.{feed}.{cfg['topic_version']}"


def connections(cfg: dict, exchange: str) -> list[ConnSpec]:
    specs = []
    for market, mcfg in cfg["exchanges"][exchange].items():
        options = {k: v for k, v in mcfg.items() if k not in ("symbols", "connections")}
        for name, ccfg in mcfg["connections"].items():
            streams = [
                Stream(
                    exchange=exchange,
                    market=market,
                    feed=feed,
                    asset=asset,
                    symbol=symbol,
                    channel=channel,
                    topic=topic_name(cfg, exchange, market, feed),
                    partition=cfg["partitions"][asset],
                )
                for feed, channel in ccfg["feeds"].items()
                for asset, symbol in mcfg["symbols"].items()
            ]
            conn_options = dict(options, **{k: v for k, v in ccfg.items() if k not in ("url", "feeds")})
            specs.append(ConnSpec(exchange, market, name, ccfg["url"], streams, conn_options))
    return specs


def all_streams(cfg: dict) -> list[Stream]:
    return [s for exchange in cfg["exchanges"] for spec in connections(cfg, exchange) for s in spec.streams]


def stream_index(cfg: dict) -> dict[tuple[str, int], Stream]:
    """(topic, partition) -> Stream, used by the recorder and data checker."""
    return {(s.topic, s.partition): s for s in all_streams(cfg)}
