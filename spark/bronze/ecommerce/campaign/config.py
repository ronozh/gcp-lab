from bronze.base import FeedConfig
from bronze.ecommerce.campaign import schema

CONFIG = FeedConfig.from_dict({
    "source": "ecommerce",
    "name": "campaign",
    "schema": schema.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
