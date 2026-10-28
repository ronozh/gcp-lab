"""FeedConfig: everything the common ingest code needs to know about one feed.

One feed = one folder: bronze/<source>/<feed>/config.py (CONFIG) + schema.py (SCHEMA).
Every option has a sensible default, so a feed config only states what differs.
New options must have defaults too, so existing configs keep working.
"""
import importlib
from dataclasses import dataclass, fields
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class FeedConfig:
    source: str                                 # = the folder bronze/<source>/; landing prefix <source>/
    name: str                                   # feed = the folder bronze/<source>/<feed>/ = bronze table name
    schema: StructType                          # from schema.py next to config.py
    file_glob: str = "*/*.csv"                  # under <source>/<name>/
    business_date_regex: str = r"/(\d{4}-\d{2}-\d{2})/"   # where the business date sits in the path

    # Spark CSV reader options
    format: str = "csv"
    header: bool = True
    delimiter: str = ","
    encoding: str = "UTF-8"
    quote: str = '"'
    escape: str = "\\"
    null_value: str = ""
    date_format: str = "yyyy-MM-dd"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    parse_mode: str = "FAILFAST"                # a malformed row fails the job (no silent nulls)

    @classmethod
    def from_dict(cls, values: dict) -> "FeedConfig":
        """Keys override defaults. Unknown keys raise, so a typo can't silently fall back to a default."""
        unknown = set(values) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown FeedConfig keys: {sorted(unknown)}")
        return cls(**values)

    @staticmethod
    def load(source: str, feed: str) -> "FeedConfig":
        """CONFIG from bronze/<source>/<feed>/config.py. No registry: a new folder is a new feed."""
        try:
            cfg = importlib.import_module(f"bronze.{source}.{feed}.config").CONFIG
        except ModuleNotFoundError as e:
            raise ValueError(f"no feed config bronze/{source}/{feed}/config.py") from e
        if (cfg.source, cfg.name) != (source, feed):   # catches a copy-pasted config
            raise ValueError(f"bronze/{source}/{feed}/config.py says source={cfg.source} name={cfg.name}")
        return cfg

    def reader_options(self) -> dict:
        return {
            "header": self.header, "sep": self.delimiter, "encoding": self.encoding,
            "quote": self.quote, "escape": self.escape, "nullValue": self.null_value,
            "dateFormat": self.date_format, "timestampFormat": self.timestamp_format,
            "timestampNTZFormat": self.timestamp_format, "mode": self.parse_mode,
        }
