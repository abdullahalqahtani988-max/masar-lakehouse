with source as (

    select * from {{ source('x_bronze', 'gps_pings') }}

)

select
    cast(trip_id as string)      as trip_id,
    cast(driver_id as string)    as driver_id,
    initcap(trim(city))          as city,
    to_timestamp(event_ts)       as pinged_at,
    cast(lat as double)          as latitude,
    cast(lon as double)          as longitude,
    _ingested_at

from source

where trip_id is not null
  and driver_id is not null