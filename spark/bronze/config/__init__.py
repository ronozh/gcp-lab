"""Feed registry. To add a feed: config/<feed>.py + schema/<feed>.py, then list it here."""
from bronze.config import campaign, customer, product, transaction

FEEDS = {c.CONFIG.name: c.CONFIG for c in (campaign, customer, product, transaction)}
