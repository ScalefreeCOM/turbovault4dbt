import os

def get_object_list(cursor,source):
    
    source_name, source_object = source.split("_")
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

def generate_rt_satellite(cursor,source, generated_timestamp,rdv_default_schema,model_path):

    source_name, source_object = source.split("_")
    model_path = model_path.replace('@@entitytype','RTS').replace('@@SourceSystem',source_name)

    object_list = get_object_list(cursor,source)

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

        for rt_sat in results:
            tracked_hk = rt_sat[0]
            tracked_entity = rt_sat[1]
            sources = ""
            print(list(set(rt_sat[2].split(','))))
            for source in list(set(rt_sat[2].split(','))):
                query2 = f"""SELECT Source_Table_Physical_Name,Static_Part_of_Record_Source_Column 
                            from source_data
                            WHERE 1=1
                            AND Source_table_identifier = '{source}'
                            """
                cursor.execute(query2)
                result = cursor.fetchone()
                sources = sources + f"\n\t{result[0].lower()}:\n\t\trsrc_static: '{result[1]}'"
            with open(os.path.join(".","templates","record_tracking_sat.txt"),"r") as f:
                command_tmp = f.read()
            f.close()
            command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@Tracked_HK', tracked_hk).replace('@@Source_Models', sources)

            filename = os.path.join(model_path, generated_timestamp , f"{tracked_entity}_rts.sql")
                    
            path = os.path.join(model_path, generated_timestamp)

            # Check whether the specified path exists or not
            isExist = os.path.exists(path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(path)

            with open(filename, 'w') as f:
                f.write(command.expandtabs(2))
                print(f"Created Record Tracking Satellite {tracked_entity}_rts")  
           