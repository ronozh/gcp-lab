from bronze.base import FeedConfig
from bronze.ecommerce.product import schema

CONFIG = FeedConfig.from_dict({
    "source": "ecommerce",
    "name": "product",
    "schema": schema.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
