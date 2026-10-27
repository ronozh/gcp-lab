"""FeedConfig: everything the common ingest code needs to know about one feed.

Every option has a sensible default, so a feed config only states what differs.
New options must have defaults too, so existing configs keep working.
"""
from dataclasses import dataclass, fields
from typing import Optional

from pyspark.sql.types import StructType


@dataclass(frozen=True)
class FeedConfig:
    name: str                                   # feed = bronze table name
    schema: StructType                          # from bronze/schema/<feed>.py
    domain: str = "ecommerce"                   # landing prefix: <domain>/<name>/<date>/<file>
    file_glob: str = "*/*.csv"                  # under <domain>/<name>/
    business_date_regex: str = r"/(\d{4}-\d{2}-\d{2})/"   # where the business date sits in the path
    ctrl_suffix: Optional[str] = ".ctrl"        # control file (record_count); None = no validation

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

    def reader_options(self) -> dict:
        return {
            "header": self.header, "sep": self.delimiter, "encoding": self.encoding,
            "quote": self.quote, "escape": self.escape, "nullValue": self.null_value,
            "dateFormat": self.date_format, "timestampFormat": self.timestamp_format,
            "timestampNTZFormat": self.timestamp_format, "mode": self.parse_mode,
        }
