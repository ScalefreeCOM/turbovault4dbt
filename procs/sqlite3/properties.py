import os
def get_groupname(cursor,source_name,source_object):
    query = f"""SELECT DISTINCT GROUP_NAME from source_data 
    where Source_System = '{source_name}' and Source_Object = '{source_object}'
    LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_properties(data_structure):
    cursor= data_structure['cursor']
    generated_timestamp= data_structure['generated_timestamp']
    model_path= data_structure['model_path']
    source_name= data_structure['source_name']
    source_object= data_structure['source_object']
    command = "version: 2\nmodels:"
    group_name = get_groupname(cursor,source_name,source_object)

    #Generating Hub Tests
    hub_query = f"""SELECT DISTINCT Target_Hub_table_physical_name,Target_Primary_Key_Physical_Name 
    from standard_hub h
    INNER JOIN source_data src on src.Source_table_identifier = h.Source_Table_Identifier
    WHERE 1=1
    AND h.Is_Primary_Source = '1'
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
    cursor.execute(hub_query)
    results = cursor.fetchall()

    for hub in results:
        hub_name = hub[0]
        hub_hk = hub[1]
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","hub_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace("@@HubName",hub_name).replace("@@HubHK",hub_hk)
        command = command + '\n'+command_tmp

    #Generating Link Tests
    link_query = f"""SELECT Target_link_table_physical_name,Target_Primary_Key_Physical_Name, GROUP_CONCAT(RefHub) FROM(
    SELECT DISTINCT l.Target_link_table_physical_name,l.Target_Primary_Key_Physical_Name,(h.Target_Hub_table_physical_name || ';' || l.Hub_primary_key_physical_name) as RefHub
    FROM standard_link l
    INNER JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
    INNER JOIN source_data src on l.Source_Table_Identifier = src.Source_table_identifier
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 
    UNION ALL
    SELECT DISTINCT l.Target_link_table_physical_name,l.Target_Primary_Key_Physical_Name,(h.Target_Hub_table_physical_name || ';' || l.Hub_primary_key_physical_name) as RefHub
    FROM non_historized_link l
    INNER JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
    INNER JOIN source_data src on l.Source_Table_Identifier = src.Source_table_identifier
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 
    )
    GROUP BY Target_link_table_physical_name,Target_Primary_Key_Physical_Name"""

    cursor.execute(link_query)
    results = cursor.fetchall()

    for link in results:
        link_name = link[0]
        link_hk = link[1]
        ref_hub = link[2].split(',')
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","link_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        ref_hub_tmp = ""
        for hub in ref_hub:
            hub_name,hub_hk = hub.split(';')
            root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
            with open(os.path.join(root,"templates","link_hub_test.txt"),"r") as f:
                hub_tmp = f.read()
            f.close()
            ref_hub_tmp = ref_hub_tmp + '\n' + hub_tmp.replace('@@HubName',hub_name).replace("@@HubHK",hub_hk)
        command_tmp = command_tmp.replace("@@LinkName",link_name).replace("@@LinkHK",link_hk).replace("@@HubRef",ref_hub_tmp)
        command = command + '\n' + command_tmp


    #Generating Satellite Tests
    sat_query = f"""SELECT DISTINCT Target_Satellite_Table_Physical_Name
    ,COALESCE(sh.Target_Hub_table_physical_name,sl.Target_link_table_physical_name,nhl.Target_link_table_physical_name) as Parent_Table_Name
    ,Parent_Primary_Key_Physical_Name
    FROM 
    (
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_Identifier,Parent_Primary_Key_Physical_Name FROM standard_satellite
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_primary_key_physical_name FROM multiactive_satellite
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_Primary_Key_Physical_Name FROM non_historized_satellite
    ) s
    INNER JOIN source_data src on src.Source_Table_Identifier = s.Source_Table_Identifier
    LEFT JOIN standard_link sl on sl.Link_Identifier = s.Parent_Identifier
    LEFT JOIN standard_hub sh on sh.Hub_Identifier = s.Parent_Identifier
    LEFT JOIN non_historized_link nhl on nhl.NH_Link_Identifier = s.Parent_Identifier

    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 

    """
    cursor.execute(sat_query)
    results = cursor.fetchall()

    for sat in results:
        sat_name = sat[0]
        parent_name = sat[1]
        parent_hk = sat[2]
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","sat_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@SatName',sat_name).replace('@@ParentHK',parent_hk).replace('@@ParentTable',parent_name)
        command = command + '\n' + command_tmp

    #Generating Pit Tests
    pit_query = f"""SELECT DISTINCT
    p.Pit_Physical_Table_Name
    ,h.Target_Hub_table_physical_name
    ,h.Target_Primary_Key_Physical_Name
    FROM pit p
    inner join standard_hub h on p.Tracked_Entity = h.Hub_Identifier
    inner join source_data src on h.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    
    UNION ALL

    SELECT DISTINCT
    p.Pit_Physical_Table_Name
    ,l.Target_Link_table_physical_name
    ,l.Target_Primary_Key_Physical_Name
    FROM pit p
    inner join standard_link l on p.Tracked_Entity = l.Link_Identifier
    inner join source_data src on l.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    """
    cursor.execute(pit_query)
    results = cursor.fetchall()

    for pit in results:
        pit_name = pit[0]
        entity_name = pit[1]
        entity_hk = pit[2]
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
        with open(os.path.join(root,"templates","pit_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@PitName',pit_name).replace('@@HK',entity_hk).replace('@@Entity',entity_name)
        command = command + '\n' + command_tmp

    model_path = model_path.replace("@@SourceSystem","").replace("@@GroupName",group_name).replace('@@timestamp',generated_timestamp)
    filename = os.path.join(model_path , f"{source_object.lower()}.yml")
          

    # Check whether the specified path exists or not
    isExist = os.path.exists(model_path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(model_path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))
    if data_structure['console_outputs']:
        print(f"Created {source_object.lower()}.yml")