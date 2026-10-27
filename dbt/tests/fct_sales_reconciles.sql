-- Gold must keep every silver transaction and its amount (fails if any row is returned).
select *
from (
    select count(*) as n, round(sum(net_amount), 2) as net from {{ ref('transaction') }}
) s
cross join (
    select count(*) as n, round(sum(net_amount), 2) as net from {{ ref('fct_sales') }}
) g
where s.n != g.n or s.net != g.net
