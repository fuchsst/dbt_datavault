{% macro get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

{#
    Checks if the relation contains all the required fields with the right type for a satellite table
#}
{% macro is_valid_satellite_relation(target_relation, 
                                     parent_table, 
                                     business_key, 
                                     link_fields, 
                                     secondary_pk_field, 
                                     load_field_name, 
                                     is_historical, 
                                     tombstone_field_name) %}
    {# TODO: add proper validation #}
    {% do return(True) %}
{% endmacro %}

{% macro create_md5_expression(field_names, prefix) %}
    {% set prefixed_field_names = [] %}
    {% if prefix is none %}
        {% set prefix_with_default = '' %}
    {% else %}
        {% set prefix_with_default = prefix %}
    {% endif %}
    
    {% for field_name in field_names %}
        {% do prefixed_field_names.append("NVL(TO_CHAR(" ~ prefix_with_default ~ field_name ~ "), 'n/a')") %}
    {% endfor %}
    MD5_HEX(CONCAT({{prefixed_field_names|join(",'|', ")}}))
{% endmacro %}

{#
    Create a temp table
#}
{% macro build_satellite_temp_table(tmp_relation, 
                                    parent_table, 
                                    business_key, 
                                    link_fields, 
                                    secondary_pk_field, 
                                    load_field_name, 
                                    is_historical, 
                                    tombstone_field_name, 
                                    sql) %}
    {% do log("Build satellite temp table '" ~ tmp_relation ~ "'") %}

    {% set (stage_exists, stage_relation) = get_or_create_relation(tmp_relation.database, tmp_relation.schema, tmp_relation.identifier ~ '_v', 'view') %}
    {% call statement('create_stage_view') %}
        CREATE OR REPLACE VIEW {{ stage_relation }} AS 
        {{ sql }}
    {% endcall %}
    
    {% set columns = adapter.get_columns_in_relation(stage_relation) %}

    {% call statement('drop_stage_view') %}
        DROP VIEW {{ stage_relation }};
    {% endcall %}

    {%- set link_field_names = [] -%}
    {%- if link_fields is not none -%}
        {% for link_field in link_fields -%}
            {%- do link_field_names.append(link_field.source_field_name.lower()) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if business_key is not none -%}{%- do link_field_names.append(business_key.lower()) -%}{%- endif -%}
    {%- if secondary_pk_field is not none -%}{%- do link_field_names.append(secondary_pk_field.lower()) -%}{%- endif -%}
    {%- if load_field_name is not none -%}{%- do link_field_names.append(load_field_name.lower()) -%}{%- endif -%}
    {%- if tombstone_field_name is not none -%}{%- do link_field_names.append(tombstone_field_name.lower()) -%}{%- endif -%}

    {%- set col_names = [] -%}
    {%- for column in columns -%}
        {%- if not column.name.lower() in link_field_names -%}
            {%- do col_names.append(column.name.lower()) -%}
        {%- endif -%}
    {%- endfor -%}

    CREATE OR REPLACE TEMP TABLE {{ tmp_relation }} AS
    SELECT  parent_table.id AS {{parent_table.identifier}}_id,
            {{create_md5_expression(col_names, 'src.')}} AS md5_hash,
            {% if secondary_pk_field is not none -%}
                src.{{secondary_pk_field}},
            {%- endif -%}
            {% for column_name in col_names %}src.{{column_name}},{% endfor %}
            {%- if tombstone_field_name is not none %}
                NVL2(src.{{tombstone_field_name}}, 'Y', 'N') AS is_tombstone,
            {%- else -%}
                'N' AS is_tombstone,
            {% endif -%}
            {%- if load_field_name is not none %}
            src.{{load_field_name}} 
            {%- else -%}
            CURRENT_TIMESTAMP
            {%- endif %} AS load_ts,
            NULL::TIMESTAMP_NTZ AS unload_ts
    FROM (
        {{sql}}
        ) AS src
    {% if business_key is not none -%} {# satellite for a hub #}
        JOIN {{parent_table}} AS parent_table ON parent_table.key = src.{{business_key}}
    {% else %}  {# satellite for a link #}
        {% set join_expressions = [] %}
        {% for link_field in link_fields -%}
            {% set hub_schema = link_field.get(link_field.hub_schema, tmp_relation.schema) %}
            {% do join_expressions.append(link_field.hub ~ '_' ~ loop.index ~ '.id=parent_table.' ~ link_field.hub ~ '_id') %}
            JOIN {{tmp_relation.database}}.{{hub_schema}}.{{link_field.hub}} AS {{link_field.hub}}_{{loop.index}} ON src.{{link_field.source_field_name}}={{link_field.hub}}_{{loop.index}}.key
        {%- endfor %}
        JOIN {{parent_table}} AS parent_table ON {{ join_expressions|join(" AND ") }}
    {%- endif %}
    ;
{% endmacro %}

{# 
   Creates (or replaces it if exists) the target relation
   TODO: check if the Snowflake specific SQL can be replaced by dbt macro 
#}
{% macro create_dv_satellite(relation, 
                             tmp_relation, 
                             secondary_pk_field, 
                             load_field_name, 
                             is_historical, 
                             tombstone_field_name) %}
    {%- do log("Build data vault satellite table '" ~ relation ~ "'") -%}
    {%- set (stage_exists, stage_relation) = get_or_create_relation(relation.database, relation.schema, relation.identifier ~ '_tmp_v', 'view') -%}
    {%- set tmp_rel_columns = adapter.get_columns_in_relation(tmp_relation) -%}
    CREATE TABLE {{relation}} (
        id NUMBER AUTOINCREMENT,
        {%- for column in tmp_rel_columns|list -%}
            {%- if (column.name.lower() != 'is_tombstone' and column.name.lower() != 'unload_ts')
                or (is_historical and column.name.lower() == 'unload_ts')  -%}
                {{column.name}} {{column.data_type}},
            {%- endif -%}
        {%- endfor %}
        CONSTRAINT {{relation.identifier}}_pk PRIMARY KEY (id)
    )
{% endmacro %}

{# 
   Merge the temporary data into the target satellite table
#}
{% macro satellite_merge_sql(temp_relation, 
                             target_relation, 
                             parent_table, 
                             secondary_pk_field, 
                             load_field_name, 
                             is_historical, 
                             tombstone_field_name) %}
    {%- do log("Merge data vault satellite temp table '" ~ temp_relation ~ "' into '" ~ target_relation ~ "'") -%}
    {%- set temp_rel_columns = adapter.get_columns_in_relation(temp_relation) -%}

    {%- if is_historical -%}
        {# set unload_ts when a newer changed record arrived #}
        UPDATE {{target_relation}} dst
        SET unload_ts = src.load_ts
        FROM (SELECT dst.id, MIN(src.load_ts) AS load_ts
                FROM {{target_relation}} dst
                JOIN {{temp_relation}} src ON dst.{{parent_table.identifier}}_id=src.{{parent_table.identifier}}_id
                             {% if secondary_pk_field is not none -%}
                             AND dst.{{secondary_pk_field}}=src.{{secondary_pk_field}} 
                             {%- endif %}
                WHERE dst.md5_hash != src.md5_hash AND dst.load_ts < src.load_ts AND dst.unload_ts IS NULL
                GROUP BY dst.id
            ) src
        WHERE src.id=dst.id;

        {# generate field list #}
        {% set src_field_list = [] %}
        {% set src_field_list_wo_ts = [] %} {# without load_ts & unload_ts fields #}
        {%- for column in temp_rel_columns -%}
            {%- if column.name.lower() != "is_tombstone" -%}
                {%- do src_field_list.append(column.name) -%}
                {%- if column.name.lower() != "load_ts"
                    and column.name.lower() != "unload_ts" -%}
                    {%- do src_field_list_wo_ts.append(column.name) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor %}

        {# add new records #}
        INSERT INTO {{target_relation}} ({{ src_field_list_wo_ts|join(", ") }}, load_ts, unload_ts)
        SELECT src.{{ src_field_list_wo_ts|join(", src.") }}, src.load_ts, MIN(src_next.load_ts) AS unload_ts
        FROM {{temp_relation}} src
        LEFT JOIN {{target_relation}} dst ON dst.{{parent_table.identifier}}_id=src.{{parent_table.identifier}}_id
                                            {% if secondary_pk_field is not none -%}
                                            AND dst.{{secondary_pk_field}}=src.{{secondary_pk_field}} 
                                            {%- endif %}
        LEFT JOIN {{temp_relation}} src_next ON src_next.{{parent_table.identifier}}_id=src.{{parent_table.identifier}}_id
                                            {% if secondary_pk_field is not none -%}
                                            AND src_next.{{secondary_pk_field}}=src.{{secondary_pk_field}} 
                                            {%- endif %}
                                            AND src_next.load_ts > src.load_ts
        WHERE (dst.id IS NULL OR (dst.md5_hash != src.md5_hash AND dst.load_ts < src.load_ts)) AND src.is_tombstone='N'
        GROUP BY src.{{ src_field_list_wo_ts|join(", src.") }}, src.load_ts;

        {# Logic to remove historical records when a tombstone arrives #}
        {% set temp_tombstone_relation = api.Relation.create(database=target_relation.database,
                                               schema=target_relation.schema,
                                               identifier=target_relation.identifier ~ "__dbt_tmp_tombstone",
                                               type=target_relation.type
                                              ) %}  
        CREATE OR REPLACE TEMP TABLE {{temp_tombstone_relation}} AS
        SELECT src.{{src_field_list_wo_ts|join(", src.")}}, 
                MIN(dst.load_ts) OVER (PARTITION BY dst.{{parent_table.identifier}}_id
                                            {% if secondary_pk_field is not none -%}
                                            , dst.{{secondary_pk_field}}
                                            {%- endif %}) AS load_ts,
                MAX(src.load_ts) OVER (PARTITION BY dst.{{parent_table.identifier}}_id
                                            {% if secondary_pk_field is not none -%}
                                            , dst.{{secondary_pk_field}}
                                            {%- endif %}) AS unload_ts 
        FROM {{temp_relation}} AS src
        LEFT JOIN {{target_relation}} AS dst ON dst.{{parent_table.identifier}}_id=src.{{parent_table.identifier}}_id
                                                {% if secondary_pk_field is not none -%}
                                                AND dst.{{secondary_pk_field}}=src.{{secondary_pk_field}} 
                                                {%- endif %}
        WHERE src.is_tombstone='Y';

        DELETE FROM {{target_relation}} AS dst
        WHERE EXISTS (SELECT 1 FROM {{temp_tombstone_relation}} AS src 
                      WHERE dst.{{parent_table.identifier}}_id=src.{{parent_table.identifier}}_id
                        {% if secondary_pk_field is not none -%}
                        AND dst.{{secondary_pk_field}}=src.{{secondary_pk_field}} 
                        {%- endif %});

        INSERT INTO {{target_relation}} ({{ src_field_list|join(", ") }})
        SELECT src.{{ src_field_list|join(", src.") }}
        FROM {{temp_tombstone_relation}} AS src;

    {%- else -%}
        {%- set update_expressions = [] -%}
        {%- set src_field_list = [] -%}
        {%- for column in temp_rel_columns -%}
            {%- if column.name.lower() != "is_tombstone"
                and column.name.lower() != "unload_ts" -%}
                {%- if column.name.lower() != parent_table.identifier.lower() ~ "_id" 
                   and column.name.lower() != "secondary_pk_field" -%}
                    {%- do update_expressions.append("dst." ~ column.name ~ "=src." ~ column.name) -%}
                {%- endif -%}
                {%- do src_field_list.append(column.name) -%}
            {%- endif -%}
        {%- endfor -%}

        MERGE INTO {{ target_relation }} dst
        USING (SELECT * FROM {{ temp_relation }}) src
        ON src.{{parent_table.identifier}}_id=dst.{{parent_table.identifier}}_id
           {%- if secondary_pk_field is not none %}
            AND src.{{secondary_pk_field}}=dst.{{secondary_pk_field}}
           {% endif %}
        WHEN MATCHED THEN UPDATE SET 
            {{ update_expressions|join(", ") }}
        WHEN NOT MATCHED THEN INSERT ({{ src_field_list|join(", ") }})
                              VALUES (src.{{ src_field_list|join(", src.") }})
    {%- endif %}
{% endmacro %}


{% materialization dv_satellite, default %}
    {% set config = model.get('config') %}
    {% set sql = model.get('injected_sql') %}
    
    {% set target_database = model.get('database') %}
    {% set target_schema = model.get('schema') %}
    {% set target_table = model.get('alias', model.get('name')) %}
    {% set temp_table = target_table ~ "__dbt_tmp" %}

    {% set surrogate_key_type = config.get('surrogate_key_type', 'sequence') %}
    
    {% set full_refresh_mode = (flags.FULL_REFRESH == True) %}

    {% set old_relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=target_table) %}
    {% set exists_as_table = (old_relation is not none and old_relation.is_table) %}
    {% set exists_not_as_table = (old_relation is not none and not old_relation.is_table) %}
    {% set should_drop = (full_refresh_mode or exists_not_as_table) %}

    {% set parent_table = ref(config.get('parent_table')) %}
    {% set business_key = config.get('business_key') %}
    {% set link_fields = config.get('link_fields') %}
    {% set secondary_pk_field = config.get('secondary_pk_field') %}
    {% set load_field_name = config.get('load_field_name', 'load_ts') %}
    {% set is_historical = config.get('is_historical', 'true').lower() == 'true' %}
    {% set tombstone_field_name = config.get('tombstone_field_name') %}

    {%- do log("is_historical '" ~ is_historical ~ "'") -%}

    {% if old_relation is not none and should_drop -%}
        {{ adapter.drop_relation(old_relation) }}
        {% set old_relation = none %}
    {%- endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if not adapter.check_schema_exists(target_database, target_schema) %}
        {% do create_schema(target_database, target_schema) %}
    {% endif %}

    {% set target_relation_exists, target_relation = get_or_create_relation(
          database=target_database,
          schema=target_schema,
          identifier=target_table,
          type='table') %}
    
    {% if target_relation_exists and not is_valid_satellite_relation(target_relation, surrogate_key_type, business_key, link_fields) %}
        {% do exceptions.relation_wrong_type(target_relation, 'satellite') %}
    {% endif %}

    {% set temp_relation = api.Relation.create(database=target_relation.database,
                                               schema=target_relation.schema,
                                               identifier=target_relation.identifier ~ "__dbt_tmp",
                                               type=target_relation.type
                                              ) %}
    {% call statement() %}
        {{ build_satellite_temp_table(
                                    temp_relation, 
                                    parent_table, 
                                    business_key, 
                                    link_fields, 
                                    secondary_pk_field, 
                                    load_field_name, 
                                    is_historical, 
                                    tombstone_field_name, 
                                    sql) }}
    {% endcall %}

    {% if full_refresh_mode or old_relation is none -%}
        {% call statement() %}
            {{ create_dv_satellite(target_relation, 
                                   temp_relation, 
                                   secondary_pk_field, 
                                   load_field_name, 
                                   is_historical, 
                                   tombstone_field_name) }}
        {% endcall %}
    {% else %}
        {% do adapter.expand_target_column_types(temp_relation, target_relation) %}
        {% set missing_columns = adapter.get_missing_columns(temp_relation, target_relation)
                                   |rejectattr("name", "equalto", "is_tombstone") 
                                   |rejectattr("name","equalto", "IS_TOMBSTONE") 
                                   |list %}
        {% do create_columns(target_relation, missing_columns) %}
    {%- endif %}

    {% call statement('main') %}
        {{ satellite_merge_sql(temp_relation, 
                                target_relation, 
                                parent_table, 
                                secondary_pk_field, 
                                load_field_name, 
                                is_historical, 
                                tombstone_field_name) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    
    {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}