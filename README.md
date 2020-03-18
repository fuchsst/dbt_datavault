### Data Vault Materialisation

_Ensure that none of the attribute fields are named like one of the standard fields (e.g. `id`, `load_ts`)._

#### Hubs
The materialisation for a hub takes the following config

| setting                       | required | example                                | default        | description | 
|-------------------------------|:--------:|----------------------------------------|----------------|-------------|
| surrogate_key_type            | Yes      | 'hash'                                 | sequence       | `sequence` (auto-increment number) or `hash` (sha-1 hash of the business key)
| business_key                  | Yes      | 'src_field_name1'                      |                | the name of the source field that contains the business key
| load_field_name               | No       | 'loaded_at'                            | current_date() | name of the field that contains the load timestamp, if not provided the current time will be used 
| source_system_field_name      | No       | 'of_a_down'                            | 'UNKNOWN'      | name of the field that contains the name of the source system, if not supplied the target field will be supplied with 'UNKNOWN'

Example:
```
{{
    config(
      schema = 'core',
      materialized = 'dv_hub',
      surrogate_key_type = 'sequence',
      business_key = 'src_field_name1',
      load_field_name = 'loaded_at',
      source_system_field_name = 'of_a_down'
    )
}}

  select ...
```

Every hub materialisation will have the same format:

| field name        | field type | description       |
|-------------------|------------|-------------------|
| id                | number     | the surrogate key
| key               | string     | the business key
| load_ts           | timestamp  | timestamp when the record was loaded
| source_system     | string     | name of the system the record was initially loaded from
| last_seen_ts      | timestamp  | the record was last seen in the source model
| last_seen_system  | string     | name of the system the record was last seen (mind that an alias can be configured and multiple models might append to the same hub!)


#### Links
The materialisation for a link takes the following config

| setting                       | required | example                                | default        | description | 
|-------------------------------|:--------:|----------------------------------------|----------------|-------------|
| surrogate_key_type            | Yes      | 'hash'                                 | sequence       | same as for hub (`sequence` or `hash`)    
| link_fields                   | Yes      |                                        |                | list of fields that link target field name to the source field with the business key and the corresponding hub 
| link_fields.source_field_name | Yes      | 'src_field_name1'                      |                | name of the business key field in the source 
| link_fields.hub               | Yes      | ref('hub_name1')                       |                | reference to the hub table for that business key
| link_fields.name              | Yes      | 'dest_field_name'                      |                | name of the field in the result model
| load_field_name               | No       | 'loaded_at'                            | current_date() | name of the field that contains the load timestamp, if not provided the current time will be used 
| source_system_field_name      | No       | 'src_system'                           | 'UNKNOWN'      | name of the field that contains the name of the source system, if not supplied the target field will be supplied with 'UNKNOWN'

```
{{
    config(
      materialized = 'dv_link',
      surrogate_key_type = 'sequence',
      link_fields = [
                        { 
                          'source_field_name' : 'src_field_name1',
                          'hub' : ref('hub_name1'),
                          'name' : 'dest_field_name' 
                        }, 
                        {
                          'source_field_name' : 'src_field_name2' 
                          'hub' : ref('hub_name2'),
                          'name' : 'dest_field_name2' 
                        }
                      ],
      load_field_name = 'loaded_at',
      source_system_field_name = 'src_system'
    )
}}

  select ...
```

Every link materialisation will have the following three standard fields and one field for each hub in the link:

| field name        | field type | description       |
|-------------------|------------|-------------------|
| id                | number     | the surrogate key
| load_ts           | timestamp  | timestamp when the record was loaded
| source_system     | string     | name of the system the record was initially loaded from
| <field1..n>       | number     | the surrogate keys of the hubs for each linked business entity

A good practice is to name them in the form <hub_name>_id.


#### Satellites
The materialisation for a satellite takes the following config

| setting                | required | example                                | default     | description | 
|------------------------|:--------:|----------------------------------------|-------------|-------------|
| parent_table           | Yes      | ref('hub_table1')                      |             | reference to the hub or link table this satellite belongs to
| business_key           | No       | 'src_field_name1'                      |             | field name of the business key in the source model (for link tables, use a list of field names that are in the same order as the `hub_tables` list)
| link_fields            | No       |                                        |             | list of fields that link target field name to the source field with the business key and the corresponding hub 
| secondary_keys          | No       | ['src_field5']                        |             | an optional field that will become part of the unique key of the satellite (besides the parent table id)
| load_field_name        | No       | 'valid_from'                           | 'load_ts'   | name of the load timestamp field in the satellite
| is_historical          | No       | false                                  | true        | if set to false, the satellite will not be historized (no unload timestamp field and existing records will be updated instead of unload timestamp set + new record)
| tombstone_field_name   | No       | 'is_deleted'                           |             | can be any type of field (char or date). If it is not null, the existing records of this business key in the satellites will be physically deleted (opposed to adding a new historical row) and one row added with all fields null and a `load_ts` of `min(load_ts)` from the records that are deleted and a `unload_ts` with the provided `load_ts` (or current_date() if missing). This might be required when the user requests to delete all his data (as allowed by GDPR)  

_`business_key` ( + `parent_table`) and `link_fields` are mutually exclusive, depending on the type (hub or link) of the referenced table_

Example:

For a hub satellite
```
{{
    config(
      materialized = 'dv_satellite',
      parent_table = 'hub_table1',
      business_key = 'src_field_name1',
      secondary_keys = ['src_field5'],
      load_field_name = 'valid_from',
      tombstone_field_name = 'tombstone_flag'
    )
}}

  select ...
```

For a link satellite
```
{{
    config(
      materialized = 'dv_satellite',
      parent_table = 'link_table1',
      link_fields = [
                      { 
                        'source_field_name' : 'src_field_name1',
                        'hub' : 'hub_name1',
                        'name' : 'dest_field_name' 
                      }, 
                      {
                        'source_field_name' : 'src_field_name2' 
                        'hub' : 'hub_name2',
                        'name' : 'dest_field_name2' 
                      }
                    ],
      secondary_keys = ['src_field5'],
      load_field_name = 'valid_from',
      tombstone_field_name = 'tombstone_flag',
      is_historical = false
    )
}}

  select ...
```

Every satellite materialisation will have the following fields:

| field name             | field type | description       |
|------------------------|------------|-------------------|
| id                     | number     | the surrogate key
| md5_hash               | string     | hex representation of the md5-hash of the attribute fields 
| load_ts                | timestamp  | timestamp when the record was loaded - taken from `load_field_name` or `CURRENT_DATE()` if not set
| unload_ts              | timestamp  | timestamp when a newer record was loaded (will be equal to the load_ts of the new record), null if this is the latest record - not created if `historical` is false
| <parent_table_name>_id | number     | ID of the parent (hub or link) table 
| <field1..n>            | *          | the attribute fields (all other fields that are not explicitly specified in `business_key`/`link_fields`, `secondary_pk_field`, `load_field_name` or `tombstone_field_name`

