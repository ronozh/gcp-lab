from bronze.config.feed import FeedConfig
from bronze.schema import customer

CONFIG = FeedConfig.from_dict({
    "name": "customer",
    "schema": customer.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
