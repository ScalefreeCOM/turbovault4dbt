import os

from procs.sqlite3.hub import generate_source_models

def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from non_historized_link where NH_Link_Identifier = '{object_id}' ORDER BY Target_Column_Sort_Order LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def generate_link_list(cursor, source, source_name, source_object):

    query = f"""SELECT NH_Link_Identifier,Target_link_table_physical_name,GROUP_CONCAT(COALESCE(Hub_primary_key_physical_name,Source_column_physical_name))
                FROM
                (SELECT DISTINCT l.NH_Link_Identifier,Target_link_table_physical_name,Hub_primary_key_physical_name,Source_column_physical_name,Hub_primary_key_physical_name
                from non_historized_link l
                inner join source_data src on src.Source_table_identifier = l.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                and l.Target_Primary_Key_Physical_Name <> ''
                order by l.Target_Column_Sort_Order)
                group by NH_Link_Identifier,Target_link_table_physical_name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def gen_payload(cursor,source_table_identifier):
    src_payload= ""
    target_payload=""
    query = f"""SELECT Source_table_identifier,GROUP_CONCAT(Source_column_physical_name),GROUP_CONCAT(Target_column_physical_name)
    FROM non_historized_link l 
    WHERE 1=1
    AND l.Source_Table_Identifier = '{source_table_identifier}'
    AND l.Target_Primary_Key_Physical_Name IS NULL
    GROUP BY Source_table_identifier"""

    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
       for column in row[1].split(','):
        if src_payload == "":
            src_payload = "\n\t\tpayload:\n"
        src_payload = src_payload + f'\t\t\t- {column}\n'

    for row in results:
       for column in row[2].split(','):
            if target_payload == "":
                target_payload = "\npayload:\n"
            target_payload = target_payload + f'\t\t- {column}\n'
    

    return src_payload,target_payload

def generate_source_models(cursor, link_id):

    command = ""

    query = f"""SELECT Source_Table_Physical_Name, Source_table_identifier,
                GROUP_CONCAT(Target_column_physical_name)
                ,Static_Part_of_Record_Source_Column FROM
                (SELECT DISTINCT src.Source_Table_Physical_Name,src.Source_table_identifier,l.Target_column_physical_name,src.Static_Part_of_Record_Source_Column 
                FROM non_historized_link l
                inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and NH_Link_Identifier = '{link_id}'
                and Target_Primary_Key_Physical_Name <> ""
                ORDER BY Target_Column_Sort_Order)
                group by Source_Table_Physical_Name,Static_Part_of_Record_Source_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()


    for source_table_row in results:
        source_table_identifier = source_table_row[1]
        source_table_name = source_table_row[0].lower()
        fk_columns = source_table_row[2].split(',')
        src_payload,target_payload = gen_payload(cursor,source_table_identifier)

        if len(fk_columns) > 1: 
            fk_col_output = ""
            for fk in fk_columns: 
                fk_col_output += f"\n\t\t\t- '{fk}'"
        else:
            fk_col_output = "'" + fk_columns[0] + "'"
        
        command += f"\n\tstg_{source_table_name}:\n\t\tfk_columns: {fk_col_output}"
        rsrc_static = source_table_row[3]
        if src_payload != "":
           command = command + src_payload


        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"


    return command,target_payload


def generate_link_hashkey(cursor, link_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM non_historized_link
                WHERE NH_link_identifier = '{link_id}'
                AND Target_Primary_Key_Physical_Name <> ''"""

    cursor.execute(query)
    results = cursor.fetchall()
    link_hashkey_name=""
    for link_hashkey_row in results: #Usually a link only has one hashkey column, so results should only return one row
        link_hashkey_name = link_hashkey_row[0] 

    return link_hashkey_name
            

def generate_nh_link(data_structure):
  cursor = data_structure['cursor']
  source = data_structure['source']
  generated_timestamp = data_structure['generated_timestamp']
  rdv_default_schema = data_structure['rdv_default_schema']
  model_path = data_structure['model_path']  
  source_name = data_structure['source_name'] 
  source_object = data_structure['source_object']       
  link_list = generate_link_list(cursor=cursor, source=source, source_name= source_name, source_object= source_object)

  for link in link_list:
    
    link_name = link[1]
    link_id = link[0]
    fk_list = link[2].split(',')

    fk_string = ""
    for fk in fk_list:
      fk_string += f"\n\t- '{fk}'"

    source_models,target_payload = generate_source_models(cursor, link_id)
    link_hashkey = generate_link_hashkey(cursor, link_id)
    group_name = 'RDV/' + get_groupname(cursor,link_id)
    model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)



    root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
    with open(os.path.join(root,"templates","nh_link.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@LinkHashkey', link_hashkey).replace('@@ForeignHashkeys', fk_string).replace('@@Payload',target_payload)


    filename = os.path.join(model_path , f"{link_name}.sql")
            
    # Check whether the specified path exists or not
    isExist = os.path.exists(model_path)

    if not isExist:   
    # Create a new directory because it does not exist 
      os.makedirs(model_path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))
      if data_structure['console_outputs']:
        print(f"Created Link Model {link_name}")
