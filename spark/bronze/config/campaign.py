from bronze.config.feed import FeedConfig
from bronze.schema import campaign

CONFIG = FeedConfig.from_dict({
    "name": "campaign",
    "schema": campaign.SCHEMA,
    # Any other FeedConfig field overrides its default, e.g. "delimiter": "|", "header": False.
})
