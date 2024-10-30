import os
def get_groupname(cursor,source_name,source_object):
    query = f"""SELECT DISTINCT GROUP_NAME from source_data 
    where Source_System = '{source_name}' and Source_Object = '{source_object}'
    LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_properties(cursor,source,generated_timestamp,model_path):
    command = "version: 2\nmodels:"
    source_name, source_object = source.split("_.._")
    group_name = get_groupname(cursor,source_name,source_object)



    #Generating Hub Tests
    hub_query = f"""SELECT DISTINCT Target_Hub_table_physical_name,Target_Primary_Key_Physical_Name, Target_Table_Comment 
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

        table_comment = hub[2]
        if table_comment == None:
            table_comment = ""
        else: table_comment = "description: "+hub[2]

        with open(os.path.join(".","templates","hub_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace("@@HubName",hub_name+"_VI").replace("@@HubHK",hub_hk).replace("@@Target_Table_Comment",table_comment)
        command = command + '\n'+command_tmp

        bk_col_query = f"""
        SELECT DISTINCT Business_Key_Physical_Name, Target_Business_Key_Comment
        FROM standard_hub
        WHERE Target_Hub_table_physical_name = '{hub_name}'
        AND Is_Primary_Source = '1'
        """
        cursor.execute(bk_col_query)
        results = cursor.fetchall()

        for bk_column in results:
            column_name =  bk_column[0]
            column_comment = bk_column[1]
            command = command + '\n' + "      - name: "+column_name

            if column_comment != None:
                command = command + '\n' + "        description: " + column_comment

    #Generating Link Tests
    link_query = f"""SELECT Target_link_table_physical_name,Target_Primary_Key_Physical_Name, GROUP_CONCAT(RefHub), Target_Table_Comment FROM(
    SELECT DISTINCT l.Target_link_table_physical_name,l.Target_Primary_Key_Physical_Name,(h.Target_Hub_table_physical_name || ';' || l.Target_column_physical_name) as RefHub, l.Target_Table_Comment
    FROM standard_link l
    INNER JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
    INNER JOIN source_data src on l.Source_Table_Identifier = src.Source_table_identifier
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
    AND  (l.Hub_identifier IS NOT NULL OR l.Target_Primary_Key_Physical_Name IS NOT NULL)
    UNION
    SELECT DISTINCT l.Target_link_table_physical_name,l.Target_Primary_Key_Physical_Name,(h.Target_Hub_table_physical_name || ';' || l.Target_column_physical_name) as RefHub, l.Target_Table_Comment
    FROM non_historized_link l
    INNER JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
    INNER JOIN source_data src on l.Source_Table_Identifier = src.Source_table_identifier
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
    AND  (l.Hub_identifier IS NOT NULL OR l.Target_Primary_Key_Physical_Name IS NOT NULL)
    )
    GROUP BY Target_link_table_physical_name,Target_Primary_Key_Physical_Name, Target_Table_Comment
"""

    cursor.execute(link_query)
    results = cursor.fetchall()

    for link in results:
        link_name = link[0]
        link_hk = link[1]
        ref_hub = link[2].split(',')

        table_comment = link[3]
        if table_comment == None:
            table_comment = ""
        else:
            table_comment = "description: " + link[3]

        with open(os.path.join(".","templates","link_test.txt"),"r") as f:
            command_tmp = f.read()
            command_tmp = command_tmp.replace("@@Target_Table_Comment",table_comment)
        f.close()
        ref_hub_tmp = ""
        for hub in ref_hub:
            hub_name,hub_hk = hub.split(';')
            with open(os.path.join(".","templates","link_hub_test.txt"),"r") as f:
                hub_tmp = f.read()
            f.close()
            ref_hub_tmp = ref_hub_tmp + '\n' + hub_tmp.replace('@@HubName',hub_name).replace("@@HubHK",hub_hk)
        command_tmp = command_tmp.replace("@@LinkName",link_name+"_VI").replace("@@LinkHK",link_hk).replace("@@HubRef",ref_hub_tmp)
        command = command + '\n' + command_tmp

        link_col_query = f"""
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM standard_link l
        WHERE Target_link_table_physical_name= '{link_name}'
        UNION
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM non_historized_link n
        WHERE Target_link_table_physical_name= '{link_name}'
        """
        cursor.execute(link_col_query)
        results = cursor.fetchall()

        for link_column in results:
            column_name =  link_column[0]
            column_comment = link_column[1]
            command = command + '\n' + "      - name: "+column_name

            if column_comment != None:
                command = command + '\n' + "        description: " + column_comment

    #Generating Satellite Tests
    sat_query = f"""SELECT DISTINCT Target_Satellite_Table_Physical_Name
    ,COALESCE(sh.Target_Hub_table_physical_name,sl.Target_link_table_physical_name,nhl.Target_link_table_physical_name) as Parent_Table_Name
    ,Parent_Primary_Key_Physical_Name
    ,s.Target_Table_Comment
    FROM 
    (
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_Identifier,Parent_Primary_Key_Physical_Name, Target_Table_Comment FROM standard_satellite WHERE Target_Column_Sort_Order = 1
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_primary_key_physical_name, Target_Table_Comment FROM multiactive_satellite WHERE Target_Column_Sort_Order = 1
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_Primary_Key_Physical_Name, null FROM non_historized_satellite
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

        table_comment = sat[3]
        if table_comment == None:
            table_comment = ""
        else: table_comment = "description: "+sat[3]


        with open(os.path.join(".","templates","sat_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@SatName',sat_name+"_VI").replace('@@ParentHK',parent_hk).replace('@@ParentTable',parent_name).replace('@@Target_Table_Comment',table_comment)
        command = command + '\n' + command_tmp

        sat_col_query = f"""
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM standard_satellite
        WHERE Target_Satellite_Table_Physical_Name= '{sat_name}'
        UNION
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM multiactive_satellite
        WHERE Target_Satellite_Table_Physical_Name= '{sat_name}'
        """
        cursor.execute(sat_col_query)
        results = cursor.fetchall()

        for sat_column in results:
            column_name =  sat_column[0]
            column_comment = sat_column[1]
            command = command + '\n' + "      - name: "+column_name

            if column_comment != None:
                command = command + '\n' + "        description: " + column_comment



    #Generating Pit Tests
    pit_query = f"""SELECT DISTINCT
    p.Pit_Physical_Table_Name
    ,h.Target_Hub_table_physical_name
    ,h.Target_Primary_Key_Physical_Name
    FROM pit p
    inner join standard_hub h on p.Tracked_Entity = h.Hub_Identifier
    inner join source_data src on h.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    AND h.Is_Primary_Source = '1'
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
        with open(os.path.join(".","templates","pit_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@PitName',pit_name+"_VI").replace('@@HK',entity_hk).replace('@@Entity',entity_name)
        command = command + '\n' + command_tmp

    #Generating Satellite Version:0 Tests
    sat_query = f"""SELECT DISTINCT Target_Satellite_Table_Physical_Name
    ,COALESCE(sh.Target_Hub_table_physical_name,sl.Target_link_table_physical_name,nhl.Target_link_table_physical_name) as Parent_Table_Name
    ,Parent_Primary_Key_Physical_Name
    ,s.Target_Table_Comment
    FROM 
    (
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_Identifier,Parent_Primary_Key_Physical_Name, Target_Table_Comment FROM standard_satellite WHERE Target_Column_Sort_Order = 1
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_primary_key_physical_name, Target_Table_Comment FROM multiactive_satellite WHERE Target_Column_Sort_Order = 1
    UNION ALL
    SELECT DISTINCT Target_Satellite_Table_Physical_Name,Source_Table_Identifier,Parent_identifier,Parent_Primary_Key_Physical_Name, null FROM non_historized_satellite
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

        satellite_model_name_splitted_list = sat_name.split('_')
        satellite_model_name_splitted_list[-2] += '0'
        satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

        parent_name = sat[1]
        parent_hk = sat[2]

        table_comment = sat[3]
        if table_comment == None:
            table_comment = ""
        else: table_comment = "description: "+sat[3]


        with open(os.path.join(".","templates","sat_v0_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@SatName',satellite_model_name_v0+"_VI").replace('@@ParentHK',parent_hk).replace('@@ParentTable',parent_name).replace('@@Target_Table_Comment',table_comment)
        command = command + '\n' + command_tmp

        sat_col_query = f"""
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM standard_satellite
        WHERE Target_Satellite_Table_Physical_Name= '{sat_name}'
        UNION
        SELECT DISTINCT Target_Column_Physical_Name, Target_Column_Comment 
        FROM multiactive_satellite
        WHERE Target_Satellite_Table_Physical_Name= '{sat_name}'
        """
        cursor.execute(sat_col_query)
        results = cursor.fetchall()

        for sat_column in results:
            column_name =  sat_column[0]
            column_comment = sat_column[1]
            command = command + '\n' + "      - name: "+column_name

            if column_comment != None:
                command = command + '\n' + "        description: " + column_comment

    # Generating Link Effectivity Satellite Version:0 Tests
    sat_query = f"""
    SELECT DISTINCT f.Link_Effectivity_Satellite, sl.Target_link_table_physical_name, sl.Target_Primary_Key_Physical_Name  
    FROM link_eff_satellite f
    INNER JOIN standard_link sl on sl.Link_Identifier = f.Link_Identifier
    INNER JOIN source_data src on src.Source_Table_Identifier = sl.Source_Table_Identifier
    WHERE 1=1
    AND sl.Driving_Key = 1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 

    """
    cursor.execute(sat_query)
    results = cursor.fetchall()

    for sat in results:

        sat_name = sat[0]

        satellite_model_name_splitted_list = sat_name.split('_')
        satellite_model_name_splitted_list[-2] += '0'
        satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

        parent_name = sat[1]
        parent_hk = sat[2]

        with open(os.path.join(".", "templates", "eff_sat_v0_test.txt"), "r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@SatName', satellite_model_name_v0+"_VI").replace('@@ParentHK',
                                                                                        parent_hk).replace(
            '@@ParentTable', parent_name)
        command = command + '\n' + command_tmp

        sat_col_query = f"""
        SELECT DISTINCT sl.Hub_primary_key_physical_name  
        FROM link_eff_satellite f
        INNER JOIN standard_link sl on sl.Link_Identifier = f.Link_Identifier
        INNER JOIN source_data src on src.Source_Table_Identifier = sl.Source_Table_Identifier
        WHERE 1=1
        AND sl.Driving_Key = 1
        AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 
        """
        cursor.execute(sat_col_query)
        results = cursor.fetchall()

        for driving_key_column in results:
            column_name = driving_key_column[0]
            if column_name == "":
                command = command + '\n' + "      - name: " + column_name
                command = command + '\n' + "        description: \"Driving Key Column\""


    # Generating Link Effectivity Satellite Version:1 Tests
    sat_query = f"""
    SELECT DISTINCT f.Link_Effectivity_Satellite, sl.Target_Primary_Key_Physical_Name    
    FROM link_eff_satellite f
    INNER JOIN standard_link sl on sl.Link_Identifier = f.Link_Identifier
    INNER JOIN source_data src on src.Source_Table_Identifier = sl.Source_Table_Identifier
    WHERE 1=1
    AND sl.Driving_Key = 1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 

    """
    cursor.execute(sat_query)
    results = cursor.fetchall()

    for sat in results:

        sat_name = sat[0]
        parent_hk = sat[1]

        with open(os.path.join(".", "templates", "eff_sat_v1_test.txt"), "r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@SatName', sat_name+"_VI").replace('@@ParentHK',parent_hk)
        command = command + '\n' + command_tmp

    #Generating Stage Tests
    stage_query = f"""
    SELECT DISTINCT src.Source_Table_Physical_Name, 'pp_'||src.Source_Table_Physical_Name
    FROM source_data src 
    WHERE 1=1
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}' 
    """
    cursor.execute(stage_query)
    results = cursor.fetchall()

    for stage in results:

        src_table_name = stage[0]
        stage_name = stage[1]



        with open(os.path.join(".","templates","stage_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace('@@StageTableName',stage_name+"_vi").replace('@@SrcTable',src_table_name)
        command = command + '\n' + command_tmp

        stag_col_query = f"""SELECT s.Source_Column_Physical_Name, MIN(s.Target_Column_Comment)
        FROM 
        (
        SELECT DISTINCT Source_Table_Identifier, Source_Column_Physical_Name, Target_Column_Comment FROM standard_satellite WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_Column_Physical_Name, Target_Column_Comment FROM standard_satellite WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Source_Column_Physical_Name, Target_Column_Comment FROM multiactive_satellite WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_Column_Physical_Name, Target_Column_Comment FROM multiactive_satellite WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Source_Column_Physical_Name, Target_Column_Comment FROM standard_link WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Source_Column_Physical_Name, Target_Column_Comment FROM non_historized_link WHERE Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_column_physical_name, '"Hash Key. '||REPLACE(Target_Column_Comment,'"','')||'"' FROM standard_link WHERE Hub_identifier IS NOT NULL AND Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_column_physical_name, '"Hash Key. '||REPLACE(Target_Column_Comment,'"','')||'"' FROM non_historized_link WHERE Hub_identifier IS NOT NULL AND Target_Column_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Source_Column_Physical_Name, Target_Business_Key_Comment FROM standard_hub WHERE Target_Business_Key_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Business_Key_Physical_Name, '"Business Key. '||REPLACE(Target_Business_Key_Comment,'"','')||'"' FROM standard_hub WHERE Target_Business_Key_Comment IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_Primary_Key_Physical_Name, '"Link Hash Key"' FROM standard_link WHERE Target_Primary_Key_Physical_Name IS NOT NULL
        UNION
        SELECT DISTINCT Source_Table_Identifier, Target_Primary_Key_Physical_Name, '"Link Hash Key"' FROM non_historized_link WHERE Target_Primary_Key_Physical_Name IS NOT NULL
        ) s
        INNER JOIN source_data src on src.Source_Table_Identifier = s.Source_Table_Identifier
    
        WHERE 1=1

        AND src.Source_Table_Physical_Name = '{src_table_name}'
        GROUP BY s.Source_Column_Physical_Name 
    
        """
        cursor.execute(stag_col_query)
        results = cursor.fetchall()

        for stage_column in results:
            column_name =  stage_column[0]
            column_comment = stage_column[1]
            command = command + '\n' + "      - name: "+column_name

            if column_comment != None:
                command = command + '\n' + "        description: " + column_comment






    #Generating Record Tracking Satellites Tests
    rts_query = f"""SELECT DISTINCT Record_Tracking_Satellite, Target_Primary_Key_Physical_Name 
    from standard_hub h
    INNER JOIN source_data src on src.Source_table_identifier = h.Source_Table_Identifier
    WHERE 1=1
    AND h.Is_Primary_Source = '1'
    AND h.Record_Tracking_Satellite!='0'
    AND h.Record_Tracking_Satellite IS NOT NULL
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
    UNION
    SELECT DISTINCT Record_Tracking_Satellite, Target_Primary_Key_Physical_Name 
    from standard_link l
    INNER JOIN source_data src on src.Source_table_identifier = l.Source_Table_Identifier
    WHERE 1=1
    AND l.Record_Tracking_Satellite!='0'
    AND l.Record_Tracking_Satellite IS NOT NULL
    AND src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
    cursor.execute(rts_query)
    results = cursor.fetchall()

    for rts in results:
        rts_name = rts[0]
        rts_hk = rts[1]


        with open(os.path.join(".","templates","rts_test.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_tmp = command_tmp.replace("@@RTSName",rts_name+"_VI").replace("@@HashKey",rts_hk)
        command = command + '\n'+command_tmp



    model_path = model_path.replace("@@SourceSystem","").replace("@@GroupName",group_name).replace('@@timestamp',generated_timestamp)
    filename = os.path.join(model_path , f"{source_object.lower()}_view.yml")

    path = os.path.join(model_path)


    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:
    # Create a new directory because it does not exist
        os.makedirs(path)

    with open(filename, 'w',encoding='utf8') as f:
        f.write(command.expandtabs(2))

    print(f"Created {source_object.lower()}_view.yml")