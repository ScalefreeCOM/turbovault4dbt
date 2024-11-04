import codecs
from datetime import datetime
import os

def get_groupname(cursor,source_name,source_object):
    query = f"""SELECT DISTINCT GROUP_NAME from source_data 
    where Source_System = '{source_name}' and Source_Object = '{source_object}'
    LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_hashed_columns(cursor,source, hashdiff_naming, source_name,source_object):
  
  command = ""
  #print(source)

  query = f"""
              SELECT Target_Primary_Key_Physical_Name, GROUP_CONCAT(Source_Column_Physical_Name), FALSE FROM 
              (SELECT COALESCE(h.Target_Role_Primary_Key_Physical_Name,h.Target_Primary_Key_Physical_Name) as Target_Primary_Key_Physical_Name, h.Source_Column_Physical_Name
              FROM standard_hub h
              inner join source_data src on h.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              ORDER BY h.Target_Column_Sort_Order) 
              GROUP BY Target_Primary_Key_Physical_Name

              UNION ALL

              SELECT Target_Primary_Key_Physical_Name, GROUP_CONCAT(Source_Column_Physical_Name), FALSE FROM
              (SELECT l.Target_Primary_Key_Physical_Name, l.Source_Column_Physical_Name
              FROM standard_link l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              AND l.Target_Primary_Key_Physical_Name IS NOT NULL
              ORDER BY l.Target_Column_Sort_Order)
              group by Target_Primary_Key_Physical_Name
              
              UNION ALL
              
              SELECT Target_Primary_Key_Physical_Name, GROUP_CONCAT(Source_Column_Physical_Name), IS_SATELLITE FROM
              (SELECT 
              l.Hub_primary_key_physical_name as Target_Primary_Key_Physical_Name,             
              COALESCE(l.Prejoin_Target_Column_Alias, l.Prejoin_Extraction_Column_Name, l.Source_Column_Physical_Name) as Source_Column_Physical_Name,
              FALSE as IS_SATELLITE
              FROM standard_link l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              AND l.Target_Primary_Key_Physical_Name IS NOT NULL
              ORDER BY l.Target_Column_Sort_Order)
              group by Target_Primary_Key_Physical_Name
              
              UNION ALL
              SELECT Target_Satellite_Table_Physical_Name,GROUP_CONCAT(Source_Column_Physical_Name),TRUE FROM 
              (SELECT '{hashdiff_naming.replace("@@SatName", "")}' || s.Target_Satellite_Table_Physical_Name as Target_Satellite_Table_Physical_Name,s.Source_Column_Physical_Name
              FROM standard_satellite s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              order by s.Target_Column_Sort_Order)
              group by Target_Satellite_Table_Physical_Name
              
              UNION ALL
              SELECT Target_Satellite_Table_Physical_Name,GROUP_CONCAT(Source_Column_Physical_Name),TRUE FROM 
              (SELECT '{hashdiff_naming.replace("@@SatName", "")}' || s.Target_Satellite_Table_Physical_Name as Target_Satellite_Table_Physical_Name,s.Source_Column_Physical_Name
              FROM multiactive_satellite s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              order by s.Target_Column_Sort_Order)
              group by Target_Satellite_Table_Physical_Name

              UNION ALL
              SELECT Target_Satellite_Table_Physical_Name,GROUP_CONCAT(Source_Column_Physical_Name),TRUE FROM 
              (SELECT '{hashdiff_naming.replace("@@SatName", "")}' || s.Target_Satellite_Table_Physical_Name as Target_Satellite_Table_Physical_Name,s.Source_Column_Physical_Name
              FROM non_historized_satellite s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              )
              group by Target_Satellite_Table_Physical_Name

              UNION ALL

              SELECT Target_Primary_Key_Physical_Name, GROUP_CONCAT(Source_Column_Physical_Name), FALSE FROM
              (SELECT l.Target_Primary_Key_Physical_Name, l.Source_Column_Physical_Name
              FROM non_historized_link l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              AND l.Target_Primary_Key_Physical_Name IS NOT NULL
              ORDER BY l.Target_Column_Sort_Order)
              group by Target_Primary_Key_Physical_Name

              UNION ALL

              SELECT Target_Primary_Key_Physical_Name, GROUP_CONCAT(Source_Column_Physical_Name), IS_SATELLITE FROM
              (SELECT 
              l.Hub_primary_key_physical_name as Target_Primary_Key_Physical_Name,             
              COALESCE(l.Prejoin_Target_Column_Alias, l.Prejoin_Extraction_Column_Name, l.Source_Column_Physical_Name) as Source_Column_Physical_Name,
              FALSE as IS_SATELLITE
              FROM non_historized_link l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              AND l.Target_Primary_Key_Physical_Name IS NOT NULL
              and l.Hub_Identifier is not NULL
              ORDER BY l.Target_Column_Sort_Order)
              group by Target_Primary_Key_Physical_Name
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


def gen_prejoin_columns(cursor, source, source_name, source_object):
  
  command = ""  
  
  query = f"""SELECT DISTINCT
              COALESCE(l.Prejoin_Target_Column_Alias,l.Prejoin_Extraction_Column_Name) as Prejoin_Target_Column_Name,
              pj_src.Source_Schema_Physical_Name, 
              pj_src.Source_Table_Physical_Name,
              l.Prejoin_Extraction_Column_Name, 
              l.Source_column_physical_name,
              l.Prejoin_Table_Column_Name
              FROM standard_link l
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

    command = command + f"""\t{alias}:\n\t\tsrc_name: '{schema}'\n\t\tsrc_table: '{table}'\n\t\tbk: '{bk_column}'\n\t\tthis_column_name: '{this_column_name}'\n\t\tref_column_name: '{ref_column_name}'\n"""

  return command


def gen_multiactive_columns(cursor,source, source_name, source_object):
  command = ""
  query = f"""SELECT DISTINCT Multi_Active_Attributes,Parent_primary_key_physical_name 
                from multiactive_satellite mas
                inner join source_data src on mas.Source_Table_Identifier = src.Source_table_identifier
                WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
  cursor.execute(query)
  multiactive_column = cursor.fetchall()
  for row in multiactive_column:
    if command == "":
      command = "multiactive_config:\n\tmulti_active_key:\n"
    
    ma_key = row[0]
    parent_hk = row[1]

    for key in ma_key.split(';'):
      command = command + f"\t- {key}\n"
    command = command + f"\tmain_hashkey_column:\n\t- {parent_hk}"
  return command

def generate_stage(data_structure):
  cursor = data_structure['cursor']
  source = data_structure['source']
  source_name = data_structure['source_name']
  source_object = data_structure['source_object']
  generated_timestamp = data_structure['generated_timestamp']
  stage_default_schema = data_structure['stage_default_schema']
  model_path = data_structure['model_path']
  hashdiff_naming = data_structure['hashdiff_naming']
  try:
    flowBiConfigs = data_structure['flowBiConfigs']
  except:
    pass
  
  hashed_columns = gen_hashed_columns(cursor, source, hashdiff_naming, source_name, source_object)
  prejoins = gen_prejoin_columns(cursor, source, source_name, source_object)
  try:
    multiactive = gen_multiactive_columns(cursor,source, source_name, source_object) ## TODO: here the code fails and generates None
  except:
    multiactive = ""
  group_name = get_groupname(cursor,source_name,source_object)
  model_path = model_path.replace("@@GroupName", 'Stage').replace("@@SourceSystem", source_name).replace('@@timestamp',generated_timestamp)

  query = f"""SELECT Source_Schema_Physical_Name,Source_Table_Physical_Name, Record_Source_Column, Load_Date_Column, Source_System  FROM source_data src
                WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
                """

  cursor.execute(query)
  sources = cursor.fetchall()

  for row in sources: #sources usually only has one row
    source_schema_name = row[0]
    source_table_name = row[1]  
    rs = row[2]
    ldts = row[3]
    source_system_name = row[4]
  timestamp = generated_timestamp

  root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
  with open(os.path.join(root,"templates","stage.txt"),"r") as f:
      command_tmp = f.read()
  f.close()
  command = command_tmp.replace("@@RecordSource",rs).replace("@@LoadDate",ldts).replace("@@HashedColumns", hashed_columns).replace("@@PrejoinedColumns",prejoins).replace('@@SourceName',source_system_name).replace('@@SourceTable',source_table_name).replace('@@SCHEMA',stage_default_schema).replace('@@MultiActive',multiactive)

  filename = os.path.join(model_path , f"stg_{source_table_name.lower()}.sql")
          
  #path = os.path.join(model_path)

  # Check whether the specified path exists or not
  isExist = os.path.exists(model_path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(model_path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))
  if data_structure['console_outputs']:
    print(f"Created stage model \'stg_{source_table_name.lower()}.sql\'")