from bronze.base import FeedConfig
from bronze.ecommerce.transaction import schema

CONFIG = FeedConfig.from_dict({
    "source": "ecommerce",
    "name": "transaction",
    "schema": schema.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
