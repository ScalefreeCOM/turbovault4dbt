import os
def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME,Is_Primary_Source
    from standard_hub 
    where Hub_Identifier = '{object_id}' 
    UNION ALL
    SELECT DISTINCT GROUP_NAME, Target_Column_Sort_Order
    from standard_link 
    where Link_Identifier = '{object_id}' 
    ORDER BY Is_Primary_Source LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_object_list(cursor,source, source_name, source_object):
    
    query = f"""SELECT DISTINCT h.Hub_Identifier 
                from standard_hub h 
                inner join source_data src on src.Source_Table_Identifier = h.Source_Table_Identifier
                WHERE 1=1
                AND src.Source_System = '{source_name}'
                AND src.Source_Object = '{source_object}'
                UNION ALL
                SELECT DISTINCT l.Link_Identifier
                from standard_link l
                inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
                WHERE 1=1
                AND src.Source_System = '{source_name}'
                AND src.Source_Object = '{source_object}'
                """
    
    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_rt_satellite(data_structure):
    cursor = data_structure['cursor']
    source = data_structure['source']
    source_name = data_structure['source_name']
    source_object = data_structure['source_object']
    generated_timestamp = data_structure['generated_timestamp']
    rdv_default_schema = data_structure['rdv_default_schema']
    model_path = data_structure['model_path']         

    object_list = get_object_list(cursor,source, source_name, source_object)

    for object in object_list:
        query = f"""SELECT DISTINCT h.Target_Primary_Key_Physical_Name, h.Target_Hub_table_physical_name, GROUP_CONCAT(src.Source_Table_Identifier)
                    from standard_hub h
                    inner join source_data src on h.Source_Table_Identifier = src.Source_Table_Identifier
                    WHERE 1=1
                    AND h.Record_Tracking_Satellite = '1'
                    AND h.Hub_Identifier = '{object[0]}'
                    GROUP BY h.Target_Primary_Key_Physical_Name,h.Target_Hub_table_physical_name
                    UNION ALL
                    SELECT DISTINCT l.Target_Primary_Key_Physical_Name, l.Target_Link_table_physical_name, GROUP_CONCAT(src.Source_Table_Identifier)
                    from standard_link l
                    inner join source_data src on l.Source_Table_Identifier = src.Source_Table_Identifier
                    WHERE 1=1
                    AND l.Record_Tracking_Satellite = '1'
                    AND l.Link_Identifier = '{object[0]}'
                    GROUP BY l.Target_Primary_Key_Physical_Name,l.Target_Link_table_physical_name
                """
        
        cursor.execute(query)
        results = cursor.fetchall()


        group_name = 'RDV/' + get_groupname(cursor,object[0])
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)

        for rt_sat in results:
            tracked_hk = rt_sat[0]
            tracked_entity = rt_sat[1]
            sources = ""

            for source in list(dict.fromkeys(rt_sat[2].split(','))):
                query2 = f"""SELECT Source_Table_Physical_Name,Static_Part_of_Record_Source_Column 
                            from source_data
                            WHERE 1=1
                            AND Source_table_identifier = '{source}'
                            """
                cursor.execute(query2)
                result = cursor.fetchone()
                sources = sources + f"\n\tstg_{result[0].lower()}:\n\t\trsrc_static: '{result[1]}'"
            root = os.path.join(os.path.dirname(os.path.abspath(__file__)).split('\\procs\\sqlite3')[0])
            with open(os.path.join(root,"templates","record_tracking_sat.txt"),"r") as f:
            #with open(os.path.join(".","templates","record_tracking_sat.txt"),"r") as f:
                command_tmp = f.read()
            f.close()
            command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@Tracked_HK', tracked_hk).replace('@@Source_Models', sources)

            filename = os.path.join(model_path , f"{tracked_entity}_rts.sql")
                    
            # Check whether the specified path exists or not
            isExist = os.path.exists(model_path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(model_path)

            with open(filename, 'w') as f:
                f.write(command.expandtabs(2))
                if data_structure['console_outputs']:
                    print(f"Created Record Tracking Satellite {tracked_entity}_rts")  
           