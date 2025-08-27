{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with flattened_outputs as (
select
tx.hash_key,
tx.block_number,
tx.block_timestamp,
tx.is_coinbase,
f.value:address::STRING as output_address,
f.value:value::FLOAT as output_value

from {{ ref('stg_btc')}} tx,

LATERAL FLATTEN(input => outputs) f

WHERE f.value:address is not null

{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= (
  SELECT max(BLOCK_TIMESTAMP)
  FROM {{ this }}
)
{% endif %}
)

select *
from flattened_outputs