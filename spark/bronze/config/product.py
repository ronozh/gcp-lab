from bronze.config.feed import FeedConfig
from bronze.schema import product

CONFIG = FeedConfig.from_dict({
    "name": "product",
    "schema": product.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
