from numpy import object_
import os

def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from non_historized_satellite where NH_Satellite_Identifier = '{object_id}' ORDER BY Target_Column_Physical_Name LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_payload(payload_list):
    payload_string = ''
    
    for column in payload_list:
        payload_string = payload_string + f'\t- {column.lower()}\n'
    
    return payload_string

def generate_nh_satellite_list(cursor, source, source_name, source_object):

    query = f"""SELECT DISTINCT NH_Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,GROUP_CONCAT(Target_Column_Physical_Name),
                Source_Table_Physical_Name,Load_Date_Column
                from 
                (SELECT DISTINCT hs.NH_Satellite_Identifier,hs.Target_Satellite_Table_Physical_Name,hs.Parent_Primary_Key_Physical_Name,hs.Target_Column_Physical_Name,
                src.Source_Table_Physical_Name,src.Load_Date_Column FROM non_historized_satellite hs
                inner join source_data src on src.Source_table_identifier = hs.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}')
                group by NH_Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column"""

    cursor.execute(query)
    results = cursor.fetchall()

    return results
        

def generate_nh_satellite(data_structure):
    cursor = data_structure['cursor']
    source = data_structure['source']
    generated_timestamp = data_structure['generated_timestamp']
    rdv_default_schema = data_structure['rdv_default_schema']
    model_path = data_structure['model_path']       
    source_name = data_structure['source_name'] 
    source_object = data_structure['source_object']   
    nh_satellite_list = generate_nh_satellite_list(cursor=cursor, source=source, source_name= source_name, source_object= source_object)


    for nh_satellite in nh_satellite_list:
        nh_satellite_name = nh_satellite[1]
        hashkey_column = nh_satellite[2]
        payload_list = nh_satellite[3].split(',')
        source_model = nh_satellite[4].lower()
        loaddate = nh_satellite[5]

        payload = gen_payload(payload_list)
        group_name = 'RDV/' + get_groupname(cursor,nh_satellite[0])
        
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","nh_sat.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)

        filename = os.path.join(model_path , f"{nh_satellite_name}.sql")
                
        # Check whether the specified path exists or not
        isExist = os.path.exists(model_path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(model_path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            if data_structure['console_outputs']:
                print(f"Created Satellite Model {nh_satellite_name}")