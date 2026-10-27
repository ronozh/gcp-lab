from bronze.config.feed import FeedConfig
from bronze.schema import transaction

CONFIG = FeedConfig.from_dict({
    "name": "transaction",
    "schema": transaction.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
