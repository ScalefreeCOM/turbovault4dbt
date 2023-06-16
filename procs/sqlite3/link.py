import os

from procs.sqlite3.hub import generate_source_models

def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from standard_link where Link_Identifier = '{object_id}' ORDER BY Target_Column_Sort_Order LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def generate_link_list(cursor, source):

    source_name, source_object = source.split("__")

    query = f"""SELECT Link_Identifier,Target_link_table_physical_name,GROUP_CONCAT(COALESCE(Hub_primary_key_physical_name,Source_column_physical_name)) FROM
                (SELECT l.Link_Identifier,Target_link_table_physical_name,Hub_primary_key_physical_name,Source_column_physical_name
                from standard_link l
                inner join source_data src on src.Source_table_identifier = l.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                order by l.Target_Column_Sort_Order)
                group by Link_Identifier,Target_link_table_physical_name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_source_models(cursor, link_id):

    command = ""

    query = f"""SELECT Source_Table_Physical_Name,GROUP_CONCAT(COALESCE(Hub_primary_key_physical_name,Source_column_physical_name)),Static_Part_of_Record_Source_Column FROM
                (SELECT src.Source_Table_Physical_Name,l.Hub_primary_key_physical_name,Source_column_physical_name,src.Static_Part_of_Record_Source_Column 
                FROM standard_link l
                inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and Link_Identifier = '{link_id}'
                ORDER BY Target_Column_Sort_Order)
                group by Source_Table_Physical_Name,Static_Part_of_Record_Source_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()

    for source_table_row in results:
        source_table_name = 'stg_' + source_table_row[0].lower()
        fk_columns = source_table_row[1].split(',')

        if len(fk_columns) > 1: 
            fk_col_output = ""
            for fk in fk_columns: 
                fk_col_output += f"\n\t\t\t- '{fk}'"
        else:
            fk_col_output = "'" + fk_columns[0] + "'"
        
        command += f"\n\t{source_table_name}:\n\t\tfk_columns: {fk_col_output}"
        rsrc_static = source_table_row[2]

        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"

    return command


def generate_link_hashkey(cursor, link_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM standard_link
                WHERE link_identifier = '{link_id}'"""

    cursor.execute(query)
    results = cursor.fetchall()

    for link_hashkey_row in results: #Usually a link only has one hashkey column, so results should only return one row
        link_hashkey_name = link_hashkey_row[0] 

    return link_hashkey_name
            

def generate_link(cursor, source, generated_timestamp, rdv_default_schema, model_path):

  link_list = generate_link_list(cursor=cursor, source=source)

  for link in link_list:
    
    link_name = link[1]
    link_id = link[0]
    fk_list = link[2].split(',')
    group_name = get_groupname(cursor,link_id)
    fk_string = ""
    for fk in fk_list:
      fk_string += f"\n\t- '{fk}'"

    source_models = generate_source_models(cursor, link_id)
    link_hashkey = generate_link_hashkey(cursor, link_id)

    source_name, source_object = source.split("__")
    model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)




    with open(os.path.join(".","templates","link.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@LinkHashkey', link_hashkey).replace('@@ForeignHashkeys', fk_string)
    

    filename = os.path.join(model_path , f"{link_name}.sql")
            
    path = os.path.join(model_path)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:   
    # Create a new directory because it does not exist 
      os.makedirs(path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))
      print(f"Created Link Model {link_name}")
