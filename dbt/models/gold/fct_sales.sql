-- One row per transaction, enriched with current customer, product and campaign details.
select
    t.transaction_id,
    t.event_ts,
    date(t.event_ts)        as order_date,
    t.status,
    t.channel,
    t.payment_method,
    t.currency,
    t.quantity,
    t.unit_price,
    t.discount_pct,
    t.gross_amount,
    t.net_amount,
    t.margin_amount,
    t.customer_id,
    c.first_name            as customer_first_name,
    c.last_name             as customer_last_name,
    c.email                 as customer_email,
    c.tier                  as customer_tier,
    c.state                 as customer_state,
    c.city                  as customer_city,
    t.product_id,
    p.sku,
    p.brand,
    p.category,
    p.subcategory,
    p.product_name,
    t.campaign_id,
    k.campaign_name,
    k.channel               as campaign_channel,
    t._meta_business_date,
    t._meta_file_name
from {{ ref('transaction') }} t
left join {{ ref('customer') }} c on c.customer_id = t.customer_id
left join {{ ref('product') }}  p on p.product_id  = t.product_id
left join {{ ref('campaign') }} k on k.campaign_id = t.campaign_id
