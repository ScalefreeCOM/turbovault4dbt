import os

def generate_ref_sat(cursor,source_name, source_object):
        query = f"""
        SELECT DISTINCT 'stg_' || LOWER(src.Source_Object),rh.Source_Column_Physical_Name,rs.Target_Reference_table_physical_name,GROUP_CONCAT(rs.Source_Column_Physical_Name)
        FROM ref_sat rs
        inner join ref_hub rh on rs.Parent_Table_Identifier = rh.Reference_Hub_Identifier and rs.Source_Table_Identifier = rh.Source_Table_Identifier
        inner join source_data src on rs.Source_Table_Identifier = src.Source_table_identifier
        WHERE 1=1
        AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
        GROUP BY src.Source_Object,rs.Target_Reference_table_physical_name
        ORDER BY rs.Target_Column_Sort_Order asc
"""

        cursor.execute(query)
        return cursor.fetchall()
def generate_source_model(cursor,source_name,source_object,ref_hub_id):
        query = f""" SELECT DISTINCT ('stg_' || src.Source_Object ), src.Static_Part_of_Record_Source_Column
        from ref_hub rh
        inner join source_data src on rh.Source_Table_Identifier = src.Source_table_identifier
        WHERE 1=1
        AND rh.Reference_Hub_Identifier= '{ref_hub_id}'
        AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
        cursor.execute(query)
        results = cursor.fetchall()
        return results

def get_hub_source(cursor,source_name,source_object):
        query = f"""SELECT DISTINCT rh.Reference_Hub_Identifier,rh.Target_Reference_table_physical_name,rh.Source_Column_Physical_Name
    from ref_hub rh
    inner join source_data src on rh.Source_Table_Identifier = src.Source_Table_Identifier
    WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
    ORDER BY  rh.Target_Column_Sort_Order asc"""
        cursor.execute(query)
        return cursor.fetchall()

def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT IFNULL(GROUP_NAME,'') from ref_table where Reference_Table_Identifier = '{object_id}' LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_ref_hub(cursor,ref_id):
    query = f"""
    SELECT DISTINCT rh.Target_Reference_table_physical_name
    from ref_table rt
    inner join ref_hub rh on rt.Referenced_Hub = rh.Reference_Hub_Identifier
    where Reference_Table_Identifier = '{ref_id}'"""
    cursor.execute(query)
    return cursor.fetchall()

def get_ref_sat(cursor,ref_id):
    query = f"""
    SELECT DISTINCT (rs.Target_Reference_table_physical_name||'|'||IFNULL(Included_Columns,'')||'|'||IFNULL(Excluded_Columns,'')) 
    from ref_table rt
    inner join ref_sat rs on rt.Referenced_Satellite = rs.Reference_Satellite_Identifier
    where Reference_Table_Identifier = '{ref_id}'"""
    cursor.execute(query)
    return cursor.fetchall()

def generate_ref_list(cursor, source_name, source_object):
    query = f"""SELECT  rt.Reference_Table_Identifier,rt.Target_Reference_table_physical_name,rt.Historized
                from ref_table rt
                inner join ref_hub rh on rt.Referenced_Hub = rh.Reference_Hub_Identifier
                inner join source_data src on rh.Source_Table_Identifier = src.Source_table_identifier

                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                GROUP BY rt.Reference_Table_Identifier,rt.Target_Reference_table_physical_name,rt.Historized
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_ref_table(cursor,source, generated_timestamp,rdv_default_schema,model_path,ref_list, hashdiff_naming, source_name, source_object, console_outputs):
    for ref in ref_list:

        ref_id = ref[0]
        ref_name = ref[1]
        historized = ref[2]

        group_name = 'RDV/' + get_groupname(cursor,ref_id)
        
        ref_hub_list = get_ref_hub(cursor,ref_id)
        ref_hubs = []
        for ref_hub in ref_hub_list:
            ref_hubs.append(ref_hub)
        ##@@RefHub
        ref_hub_string_list = list(dict.fromkeys(ref_hubs))
        if(len(ref_hub_string_list) > 1):
            raise Exception('Reference Table has more than one referenced Hub')
        ref_hub_string = ""
        for elem in ref_hub_string_list:
            ref_hub_string += elem[0]

        ##@@RefSat
        
        ref_sat_list = get_ref_sat(cursor,ref_id)
        ref_sats = []
        for ref_sat in ref_sat_list:
            ref_sats.append(ref_sat)

        ref_sat_string_list = []

        for sat in ref_sats:
            sat_name = sat[0].split('|')[0]
            include = sat[0].split('|')[1]
            exclude = sat[0].split('|')[2]
            if(include != '' and exclude == ''):
                include_columns = include.split(';')
                sat_name = f'\n\t- {sat_name}:\n\t\t\tinclude:'
                for column in include_columns:
                    sat_name += f'\n\t\t\t\t- {column}'
                ref_sat_string_list.append(sat_name)
            elif(include == '' and exclude != ''):
                exclude_columns = exclude.split(';')
                sat_name = f'\n\t- {sat_name}:\n\t\t\texclude:'
                for column in exclude_columns:
                    sat_name += f'\n\t\t\t\t- {column}'
                ref_sat_string_list.append(sat_name)
            else:
                sat_name = f'\n\t- {sat_name}: \n'
                ref_sat_string_list.append(sat_name)
        ref_sat_string_list = list(dict.fromkeys(ref_sat_string_list))
        ref_sat_string = ""
        for elem in ref_sat_string_list:
            ref_sat_string += elem

        ##@@Historized
        if(historized != 'full' and historized != 'latest'):
            historized = f"snapshot'\nsnapshot_relation:'{historized}"
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","ref_table.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@Schema',rdv_default_schema).replace('@@RefHub', ref_hub_string).replace('@@RefSat',ref_sat_string).replace('@@Historized',historized)
           
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        filename = os.path.join(model_path,  f"{ref_name}.sql")
                

        # Check whether the specified path exists or not
        isExist = os.path.exists(model_path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(model_path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            if console_outputs:
                print(f"Created Reference Table Model {ref_name}")  

        #Reference Hub
        relevant_hubs = get_hub_source(cursor,source_name,source_object)
        bk = []
        for key in relevant_hubs:
            bk.append(key[2])
        bk_str = ','.join(bk)
        source_models = ''


        for hub in relevant_hubs:
            ref_hub_id = hub[0]
            ref_hub_name = hub[1]
            ref_keys = hub[2]


            source_model_list = generate_source_model(cursor,source_name,source_object,ref_hub_id)
            for src in source_model_list:
                Source_name = src[0]
                rsrc_static = src[1]
                source_models += f"\n\t\t- name: {Source_name.lower()}\n\t\t\tref_keys: '{ref_keys}'\n\t\t\trsrc_static: '{rsrc_static}'"
                
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","ref_hub.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@Schema',rdv_default_schema).replace('@@SourceModel', source_models).replace('@@RefKeys',bk_str)
           
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',Source_name).replace('@@timestamp',generated_timestamp)
        filename = os.path.join(model_path,  f"{ref_hub_name}.sql")
                
        # Check whether the specified path exists or not
        isExist = os.path.exists(model_path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(model_path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            if console_outputs:
                print(f"Created Reference Hub Model {ref_hub_name}")  

        #Ref Satellites

        ref_sat_list = generate_ref_sat(cursor,source_name, source_object)
        command_tmp = ''
        #Satellite v0
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","ref_sat_v0.txt") ,"r") as f:
            command_tmp = f.read()
        f.close()

        for sat in ref_sat_list:
            sat_source = sat[0]
            sat_key = sat[1]
            sat_name = sat[2]
            hashdiff_column = hashdiff_naming.replace('@@SatName',sat_name)
            sat_payload = sat[3]
            payload = ''
            payload_column_list = sat_payload.split(',')
            for col in payload_column_list:
                payload += f'\n\t\t- {col}'
            satellite_model_name_splitted_list = sat_name.split('_')
            satellite_model_name_splitted_list[-2] += '_0'
            satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

            command = command_tmp.replace('@@Schema',rdv_default_schema).replace('@@SourceModel', sat_source).replace('@@RefKeys',sat_key).replace('@@HashDiff',hashdiff_column).replace('@@Payload',payload)

            model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',Source_name).replace('@@timestamp',generated_timestamp)
            filename = os.path.join(model_path,  f"{satellite_model_name_v0}.sql")
                    
            # Check whether the specified path exists or not
            isExist = os.path.exists(model_path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(model_path)

            with open(filename, 'w') as f:
                f.write(command.expandtabs(2))
                if console_outputs:
                    print(f"Created Reference Sat Model {sat_name}")  

            #Satellite_v1
            root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
            with open(os.path.join(root,"templates","ref_sat_v1.txt"),"r") as f:
                command_tmp = f.read()
            f.close()
            command_v1 = command_tmp.replace('@@Schema',rdv_default_schema).replace('@@RefSat', satellite_model_name_v0).replace('@@RefKeys', sat_key).replace('@@HashDiff', hashdiff_column).replace('@@Schema', rdv_default_schema)
                
    

            filename_v1 = os.path.join(model_path , f"{sat_name}.sql")
                    
            path_v1 = os.path.join(model_path)

            # Check whether the specified path exists or not
            isExist_v1 = os.path.exists(path_v1)

            if not isExist_v1:   
            # Create a new directory because it does not exist 
                os.makedirs(path_v1)

            with open(filename_v1, 'w') as f:
                f.write(command_v1.expandtabs(2))
                if console_outputs:
                    print(f"Created Ref Sat Model {sat_name}")


def generate_ref(data_structure):
    cursor = data_structure['cursor']
    source = data_structure['source']
    source_name = data_structure['source_name']   
    source_object = data_structure['source_object'] 
    generated_timestamp = data_structure['generated_timestamp']
    rdv_default_schema = data_structure['rdv_default_schema']
    model_path = data_structure['model_path']  
    hashdiff_naming = data_structure['hashdiff_naming']      
    console_outputs=  data_structure['console_outputs']
    ref_list = generate_ref_list(cursor,source_name, source_object)
    generate_ref_table(cursor,source, generated_timestamp,rdv_default_schema,model_path,ref_list, hashdiff_naming, source_name, source_object, console_outputs)




