from bronze.base import FeedConfig
from bronze.ecommerce.customer import schema

CONFIG = FeedConfig.from_dict({
    "source": "ecommerce",
    "name": "customer",
    "schema": schema.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
