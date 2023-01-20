import codecs
from datetime import datetime
import os
import templates as tmp

def gen_hashed_columns(cursor,source):
  command = ""

  source_name, source_object = source.split("_")

  query = f"""
              SELECT h.Target_Primary_Key_Physical_Name,LISTAGG(h.Source_Column_Physical_Name,',') WITHIN GROUP (order by h.Target_Column_Sort_Order),FALSE as IS_SATELLITE
              FROM core.hub_entities h
              inner join core.source_data src on h.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              group by h.Target_Primary_Key_Physical_Name
              UNION ALL
              SELECT CONCAT('hk_',l.Target_link_table_physical_name),LISTAGG(l.Source_Column_Physical_Name,',') WITHIN GROUP (order by l.Target_Column_Sort_Order),FALSE as IS_SATELLITE
              FROM core.link_entities l
              inner join core.source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              group by CONCAT('hk_',l.Target_link_table_physical_name)
              UNION ALL
              SELECT CONCAT('hd_',s.Target_Satellite_Table_Physical_Name),LISTAGG(s.Source_Column_Physical_Name,',') WITHIN GROUP (order by s.Target_Column_Sort_Order),TRUE as IS_SATELLITE
              FROM core.hub_satellites s
              inner join core.source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              group by CONCAT('hd_',s.Target_Satellite_Table_Physical_Name)
              UNION ALL
              SELECT CONCAT('hd_',s.Target_Satellite_Table_Physical_Name),LISTAGG(s.Source_Column_Physical_Name, ',') WITHIN GROUP (order by s.Target_Column_Sort_Order),TRUE as IS_SATELLITE
              FROM core.link_satellites s
              inner join core.source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              group by CONCAT('hd_',s.Target_Satellite_Table_Physical_Name)
              """
  cursor.execute(query)
  results = cursor.fetchall()

  for hashkey in results:
  
    hashkey_name = hashkey[0]
    bk_list = hashkey[1].split(",")

    command = command + f"\t{hashkey_name}:\n"

    if hashkey[2]: 
      command = command + "\t\tis_hashdiff: true\n\t\tcolumns:\n"

      for bk in bk_list:
        command = command + f"\t\t\t- {bk}\n"
    
    else:
      for bk in bk_list:
        command = command + f"\t\t- {bk}\n"

  return command


def gen_prejoin_columns(cursor, source):
  
  command = ""  

  source_name, source_object = source.split("_")
  
  query = f"""SELECT 
              COALESCE(l.Prejoin_Target_Column_Alias,l.Prejoin_Extraction_Column_Name) as Prejoin_Target_Column_Name,
              pj_src.Source_Schema_Physical_Name, 
              pj_src.Source_Table_Physical_Name,
              l.Prejoin_Extraction_Column_Name, 
              l.Source_column_physical_name,
              l.Prejoin_Table_Column_Name
              FROM link_entities l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              inner join source_data pj_src on l.Prejoin_Table_Identifier = pj_src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and l.Prejoin_Table_Identifier is not NULL"""
  
  
  cursor.execute(query)
  prejoined_column_rows = cursor.fetchall()
  for prejoined_column in prejoined_column_rows:

    if command == "":
      command = "prejoined_columns:\n"

    schema = prejoined_column[1]
    table = prejoined_column[2]
    alias = prejoined_column[0]
    bk_column = prejoined_column[3]
    this_column_name = prejoined_column[4]
    ref_column_name = prejoined_column[5]

    command = command + f"""\t{alias}:\n\t\tsrc_schema:"{schema}"\n\t\tsrc_table:"{table}"\n\t\tbk:"{bk_column}"\n\t\tthis_column_name:"{this_column_name}"\n\t\tref_column_name:"{ref_column_name}"\n"""

  return command
  

def main(cursor, source,generated_timestamp,stage_default_schema,model_path):

  hashed_columns = gen_hashed_columns(cursor, source)
  prejoins = gen_prejoin_columns(cursor, source)

  source_name, source_object = source.split("_")

  model_path = model_path.replace("@@entitytype", "Stage").replace("@@SourceSystem", source_name)


  query = f"""SELECT Source_Schema_Physical_Name,Source_Table_Physical_Name, Record_Source_Column, Load_Date_Column  FROM source_data src
                WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
  
  cursor.execute(query)
  sources = cursor.fetchall()
  for row in sources: #sources usually only has one row
    source_schema_name = row[0]
    source_table_name = row[1]  
    rs = row[2]
    ldts = row[3]
  timestamp = generated_timestamp
  with open(os.path.join(".","templates","stage.txt"),"r") as f:
    command = f.read()
  f.close()
  command2 = command.replace("@@HashedColumns",hashed_columns).replace("@@RecordSource",rs).replace("@@LoadDate",ldts).replace("@@PrejoinedColumns",prejoins).replace('@@SourceName',source_schema_name).replace('@@SourceTable',source_table_name).replace('@@SCHEMA',stage_default_schema)
  
#   command = """{{ config(materialized='view') }}
# {{ config(schema='@@SCHEMA') }}

# {%- set yaml_metadata -%}
# source_model: 
#   '@@SourceName': '@@SourceTable'
# hashed_columns:
# @@HashedColumns
# rsrc: '@@RecordSource' 
# ldts: '@@LoadDate'
# include_source_columns: true
# @@PrejoinedColumns

# {%- endset -%}

# {% set metadata_dict = fromyaml(yaml_metadata) %}

# {{ stage(include_source_columns=metadata_dict['include_source_columns'],
#                   source_table=metadata_dict['source_table'],
#                   source_schema=metadata_dict['source_schema'],
#                   hashed_columns=metadata_dict['hashed_columns'],
#                   ranked_columns=none,
#                   rsrc=metadata_dict['rsrc'],
#                   ldts=metadata_dict['ldts'],
#                   prejoined_columns=metadata_dict['prejoined_columns']) }}
# """.replace("@@HashedColumns",hashed_columns).replace("@@RecordSource",rs).replace("@@LoadDate",ldts).replace("@@PrejoinedColumns",prejoins).replace('@@SourceName',source_schema_name).replace('@@SourceTable',source_table_name).replace('@@SCHEMA',stage_default_schema)
  
  filename = os.path.join(model_path, timestamp , f"{source_table_name.lower()}.sql")
          
  path = os.path.join(model_path, timestamp)


  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command2.expandtabs(2))

  print(f"Created model \'{source_table_name.lower()}.sql\'")