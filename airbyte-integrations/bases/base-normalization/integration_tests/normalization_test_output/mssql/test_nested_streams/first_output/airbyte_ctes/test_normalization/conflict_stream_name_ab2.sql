USE [test_normalization];
    execute('create view _airbyte_test_normalization."conflict_stream_name_ab2__dbt_tmp" as
    
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast(id as 
    VARCHAR(max)) as id,
    cast(conflict_stream_name as VARCHAR(max)) as conflict_stream_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    SYSDATETIME() as _airbyte_normalized_at
from "test_normalization"._airbyte_test_normalization."conflict_stream_name_ab1"
-- conflict_stream_name
where 1 = 1
    ');

