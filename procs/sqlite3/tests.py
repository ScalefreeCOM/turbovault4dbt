import os


def gen_tests(cursor,source,generated_timestamp,model_path):
    command = "models:"
    source_name, source_object = source.split("_")
    hub_query = f"""SELECT DISTINCT Target_Hub_table_physical_name,Target_Primary_Key_Physical_Name 
    from standard_hub h
    INNER JOIN source_data src on src.Source_table_identifier = h.Source_Table_Identifier
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
    cursor.execute(hub_query)
    results = cursor.fetchall()

    for hub in results:
        hub_name = hub[0]
        hub_hk = hub[1]
        with open(os.path.join(".","templates","hub_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace("@@HubName",hub_name).replace("@@HubHK",hub_hk)
        command = command + '\n'+command_tmp


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
        with open(os.path.join(".","templates","link_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        ref_hub_tmp = ""
        for hub in ref_hub:
            hub_name,hub_hk = hub.split(';')
            with open(os.path.join(".","templates","link_hub_test.txt"),"r") as f:
                hub_tmp = f.read()
            f.close()
            ref_hub_tmp = ref_hub_tmp + '\n' + hub_tmp.replace('@@HubName',hub_name).replace("@@HubHK",hub_hk)
        command_tmp = command_tmp.replace("@@LinkName",link_name).replace("@@LinkHK",link_hk).replace("@@HubRef",ref_hub_tmp)
        command = command + '\n' + command_tmp








    model_path = model_path.replace("@@SourceSystem","dbt_project")
    filename = os.path.join(model_path, generated_timestamp , f"{source_object.lower()}.yml")
          
    path = os.path.join(model_path, generated_timestamp)


    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))

    print(f"Created {source_object.lower()}.yml")