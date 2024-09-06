from numpy import object_
import os
def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from multiactive_satellite where MA_Satellite_Identifier = '{object_id}' ORDER BY Target_Column_Sort_Order LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_payload(payload_list):
    payload_string = ''
    
    for column in payload_list:
        payload_string = payload_string + f'\t- {column.lower()}\n'
    
    return payload_string

def generate_ma_satellite_list(cursor, source, source_name, source_object):

    query = f"""SELECT DISTINCT MA_Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,GROUP_CONCAT(Target_Column_Physical_Name),
                Source_Table_Physical_Name,Load_Date_Column,Multi_Active_Attributes
                from 
                (SELECT DISTINCT hs.MA_Satellite_Identifier,hs.Target_Satellite_Table_Physical_Name,hs.Parent_Primary_Key_Physical_Name,hs.Target_Column_Physical_Name,
                src.Source_Table_Physical_Name,src.Load_Date_Column,hs.Multi_Active_Attributes FROM multiactive_satellite hs
                inner join source_data src on src.Source_table_identifier = hs.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                order by Target_Column_Sort_Order asc)
                group by MA_Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column"""

    cursor.execute(query)
    results = cursor.fetchall()

    return results
        

def generate_ma_satellite(data_structure):
    cursor = data_structure['cursor']
    source = data_structure['source']
    generated_timestamp = data_structure['generated_timestamp']
    rdv_default_schema = data_structure['rdv_default_schema']
    model_path = data_structure['model_path']  
    hashdiff_naming = data_structure['hashdiff_naming']        
    source_name = data_structure['source_name'] 
    source_object = data_structure['source_object'] 
    satellite_list = generate_ma_satellite_list(cursor=cursor, source=source, source_name= source_name, source_object= source_object)

    for satellite in satellite_list:
        satellite_name = satellite[1]
        hashkey_column = satellite[2]
        hashdiff_column = hashdiff_naming.replace('@@SatName',satellite_name)
        payload_list = satellite[3].split(',')
        source_model = satellite[4].lower()
        loaddate = satellite[5]
        ma_attribute_list = satellite[6].split(';')
        group_name = get_groupname(cursor,satellite[0])
        model_path_v0 = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        model_path_v1 = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)

        payload = gen_payload(payload_list)
        ma_attribute = gen_payload(ma_attribute_list)
        
        
        #Satellite_v0
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","ma_sat_v0.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@MaAttribute', ma_attribute).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            
  
        satellite_model_name_splitted_list = satellite_name.split('_')
        satellite_model_name_splitted_list[-2] += '0'
        satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

        filename = os.path.join(model_path_v0 , f"{satellite_model_name_v0}.sql")
                
        path = os.path.join(model_path_v0)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command_v0.expandtabs(2))
            if data_structure['console_outputs']:
                print(f"Created Multi Active Satellite Model {satellite_model_name_v0}")

        #Satellite_v1
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","ma_sat_v1.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@MaAttribute', ma_attribute).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            
  

        filename_v1 = os.path.join(model_path_v1 , f"{satellite_name}.sql")
                
        path_v1 = os.path.join(model_path_v1)

        # Check whether the specified path exists or not
        isExist_v1 = os.path.exists(path_v1)

        if not isExist_v1:   
        # Create a new directory because it does not exist 
            os.makedirs(path_v1)

        with open(filename_v1, 'w') as f:
            f.write(command_v1.expandtabs(2))
            if data_structure['console_outputs']:
                print(f"Created Multi Active Satellite Model {satellite_name}")