import os

def generate_ref_table(cursor,source, generated_timestamp,rdv_default_schema,model_path,ref_list):
    source_name, source_object = source.split("_")
    for ref in ref_list:

        ref_id = ref[0]
        ref_name = ref[1]
        historized = ref[2]

        group_name = get_groupname(cursor,ref_id)
        
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
                sat_name = f'\n\t- {sat_name}: {{}}\n'
                ref_sat_string_list.append(sat_name)
        ref_sat_string_list = list(dict.fromkeys(ref_sat_string_list))
        ref_sat_string = ""
        for elem in ref_sat_string_list:
            ref_sat_string += elem

        ##@@Historized
        if(historized != 'full' and historized != 'latest'):
            historized = f"'snapshot'\nsnapshot_relation:{historized}"

        with open(os.path.join(".","templates","ref_table.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@RefHub', ref_hub_string).replace('@@RefSat',ref_sat_string).replace('@@Historized',historized)
           
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        filename = os.path.join(model_path,  f"{ref_name}.sql")
                
        path = os.path.join(model_path)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Reference Table Model {ref_name}")  


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

def generate_ref_list(cursor, source):

    source_name, source_object = source.split("_")

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


##RefTable

def generate_ref(cursor,source, generated_timestamp,rdv_default_schema,model_path):
    
    ref_list = generate_ref_list(cursor=cursor,source=source)


    generate_ref_table(cursor,source, generated_timestamp,rdv_default_schema,model_path,ref_list)

##RefHubs



##RefSats


