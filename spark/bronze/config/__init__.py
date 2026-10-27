"""Feed registry. To add a feed: config/<feed>.py + schema/<feed>.py, then list it here."""
from bronze.config import campaign, customer, product, transaction
from bronze.config.feed import FeedConfig

FEEDS = {c.CONFIG.name: c.CONFIG for c in (campaign, customer, product, transaction)}


def load(name: str) -> FeedConfig:
    if name not in FEEDS:
        raise ValueError(f"unknown feed {name!r}; known: {sorted(FEEDS)}")
    return FEEDS[name]
