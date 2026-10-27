-- Daily full snapshot feed: current state = rows of the latest file.
select * except (_meta_record_number, _meta_file_size, _meta_file_modified_ts)
from {{ source('bronze', 'product') }}
qualify dense_rank() over (order by _meta_business_date desc, _meta_file_name desc) = 1
