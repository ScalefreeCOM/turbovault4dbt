{{ config(materialized='view') }}

{%- set yaml_metadata -%}
control_snap_v0: 'control_snap_v0'
log_logic: 
    daily:
        duration: 3
        unit: 'MONTH'
    weekly:
        duration: 1
        unit: 'YEAR'
    monthly:
        duration: 5
        unit: 'YEAR'
    yearly:
        forever: true
{%- endset -%}    

{{ datavault4dbt.control_snap_v1(yaml_metadata=yaml_metadata) }}