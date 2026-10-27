-- Daily event feed: all days; per business date, rows of the latest file (a resent day replaces the old one).
select * except (_meta_record_number, _meta_file_size, _meta_file_modified_ts)
from {{ source('bronze', 'transaction') }}
qualify dense_rank() over (partition by _meta_business_date order by _meta_file_name desc) = 1
