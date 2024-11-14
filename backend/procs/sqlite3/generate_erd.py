import os
import subprocess



def get_source_group(cursor,source_system,source_object):
    query = f"""SELECT DISTINCT src.Group_Name 
    from source_data src
    WHERE 1=1 
    AND src.Source_System = '{source_system}'
    AND src.Source_Object = '{source_object}'"""
    cursor.execute(query)
    results = cursor.fetchone()
    return results[0]

def generate_pit_list(cursor,source_name_list,source_object_list):
    query = f"""SELECT DISTINCT l.Pit_Identifier 
    FROM standard_link l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})

    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def generate_link_list(cursor,source_name_list,source_object_list):
    query = f"""SELECT DISTINCT l.Link_Identifier 
    FROM standard_link l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})

    UNION ALL

    SELECT DISTINCT l.NH_Link_Identifier 
    FROM non_historized_link l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})
    
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def generate_sat_list(cursor,source_name_list,source_object_list):
    query = f"""SELECT DISTINCT l.Satellite_Identifier 
    FROM standard_satellite l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})

    UNION ALL

    SELECT DISTINCT l.MA_Satellite_Identifier 
    FROM multiactive_satellite l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})  

    UNION ALL

    SELECT DISTINCT l.NH_Satellite_Identifier 
    FROM non_historized_satellite l 
    inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
    WHERE 1=1
    and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})    

    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def generate_parents(cursor,sat_id):
    query = f"""SELECT DISTINCT h.Target_Hub_table_physical_name, h.Target_Primary_Key_Physical_Name
             from standard_satellite s
             inner join standard_hub h on s.Parent_Identifier = h.Hub_Identifier
             WHERE 1=1
             AND s.Satellite_Identifier = '{sat_id}'
             
             UNION ALL

             SELECT DISTINCT h.Target_link_table_physical_name, h.Target_Primary_Key_Physical_Name
             from standard_satellite s
             inner join standard_link h on s.Parent_Identifier = h.Link_Identifier
             WHERE 1=1
             AND s.Satellite_Identifier = '{sat_id}'
             """
    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_erd(cursor,source_list, generated_timestamp,model_path,hashdiff_naming):
    command = ""
    source_name_list = []
    source_object_list = []
    for source in source_list:
        #print(source)
        source_name,source_object = source.split('_')
        source_name_list.append(source_name)
        source_object_list.append(source_object)
    
    source_name_list = list(dict.fromkeys(source_name_list))
    source_object_list = list(dict.fromkeys(source_object_list))


    ##Hubs
    query = f"""
            SELECT DISTINCT Target_Hub_table_physical_name, Business_Key_Physical_Name, Target_Primary_Key_Physical_Name
            from standard_hub h
            inner join source_data src on src.Source_Table_Identifier = h.Source_Table_Identifier
            WHERE 1=1
            and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
            and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})
            """
    cursor.execute(query)
    results = cursor.fetchall()

    for object in results:

        object_name = object[0]
        #object_entity_type = object[1]
        object_columns = object[1]
        object_pk = object[2]
        color_code = '5BD1FA'

        command += f"""Table {object_name} [headercolor: #{color_code}]{{\n\n{object_pk} text [primary key]\n{object_columns} text\nldts timestamp_ntz\nrsrc text\n\n}}\n\n\n"""
        

    ##Links
    link_list = generate_link_list(cursor,source_name_list,source_object_list)
    for link in link_list:
        query2 = f"""SELECT DISTINCT 
        Target_link_table_physical_name, GROUP_CONCAT(Target_column_physical_name)
        from standard_link l 
        where l.Link_Identifier = '{link[0]}' 
        and l.Hub_identifier IS NULL 

        UNION ALL

        SELECT DISTINCT 
        Target_link_table_physical_name, GROUP_CONCAT(Target_column_physical_name)
        from non_historized_link l 
        where l.NH_Link_Identifier = '{link[0]}' 
        and l.Hub_identifier IS NULL 
        GROUP BY Target_link_table_physical_name 
        """
        cursor.execute(query2)
        results2 = cursor.fetchall()
        #print(results2)
        bks = ""
        for bk_list in results2:
            if not all(bk_list):
                pass
            else:
                for bk in bk_list[1].split(','):
                    bks += f"{bk} text\n"
        query = f"""

                SELECT DISTINCT Target_link_table_physical_name, GROUP_CONCAT(Target_column_physical_name ||';'|| Target_Hub_Table_Physical_Name),Target_Primary_Key_Physical_Name,Target_Column_Sort_Order
                FROM (
                SELECT DISTINCT l.Target_link_table_physical_name, h.Target_Hub_Table_Physical_Name, l.Target_column_physical_name,l.Target_Column_Sort_Order
                , l.Target_Primary_Key_Physical_Name
                from standard_link l
                LEFT JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
                inner join source_data src on src.Source_Table_Identifier = h.Source_Table_Identifier
                WHERE 1=1
                and l.Link_Identifier = '{link[0]}'
                )
                UNION ALL

                SELECT DISTINCT Target_link_table_physical_name, GROUP_CONCAT(Target_column_physical_name ||';'|| Target_Hub_Table_Physical_Name),Target_Primary_Key_Physical_Name,Target_Column_Sort_Order
                FROM (
                SELECT DISTINCT l.Target_link_table_physical_name, h.Target_Hub_Table_Physical_Name, l.Target_column_physical_name,l.Target_Column_Sort_Order
                , l.Target_Primary_Key_Physical_Name
                from non_historized_link l
                LEFT JOIN standard_hub h on l.Hub_identifier = h.Hub_Identifier
                inner join source_data src on src.Source_Table_Identifier = h.Source_Table_Identifier
                WHERE 1=1
                and l.NH_Link_Identifier = '{link[0]}'
                )
                GROUP BY Target_link_table_physical_name,Target_Primary_Key_Physical_Name
                ORDER BY Target_Column_Sort_Order

                """
        cursor.execute(query)
        results = cursor.fetchall()
        foreign_keys = ""
        #print(results)
        for object in results:
            if not all(object):
                pass
            else:
                object_name = object[0]
                #object_entity_type = object[1]
                object_columns = object[1].split(',')
                object_columns_list = list(dict.fromkeys(object_columns))
                for fk in object_columns_list:
                    foreign_keys += f"{fk.split(';')[0]} text [ref: <> {fk.split(';')[1]}.{fk.split(';')[0]}]\n"
                #print(object_columns_list)
                object_pk = object[2]
                color_code = 'EF7070'
                

                command += f"""Table {object_name} [headercolor: #{color_code}]{{\n\n{object_pk} text [primary key]\n{foreign_keys}{bks}ldts timestamp_ntz\nrsrc text\n\n}}\n\n\n"""
        


    ##Sats
    sat_list = generate_sat_list(cursor,source_name_list,source_object_list)
    for sat in sat_list:
        query = f"""SELECT DISTINCT s.Satellite_Identifier as Satellite_Identifier,s.Target_Satellite_Table_Physical_Name,GROUP_CONCAT(s.Target_Column_Physical_Name) as Target_Column_Physical_Name
                from standard_satellite s
                WHERE 1=1
                AND s.Satellite_Identifier = '{sat[0]}'

                UNION ALL

                SELECT DISTINCT s.MA_Satellite_Identifier as Satellite_Identifier,s.Target_Satellite_Table_Physical_Name,GROUP_CONCAT(s.Target_Column_Physical_Name ||','|| REPLACE(Multi_Active_Attributes,';',',')) as Target_Column_Physical_Name
                from multiactive_satellite s
                WHERE 1=1
                AND s.MA_Satellite_Identifier = '{sat[0]}'

                UNION ALL

                SELECT DISTINCT s.NH_Satellite_Identifier as Satellite_Identifier,s.Target_Satellite_Table_Physical_Name,GROUP_CONCAT(s.Target_Column_Physical_Name) as Target_Column_Physical_Name
                from non_historized_satellite s
                WHERE 1=1
                AND s.NH_Satellite_Identifier = '{sat[0]}'     

                

                GROUP BY Satellite_Identifier, Target_Satellite_Table_Physical_Name
                ORDER BY Target_Column_Physical_Name

                """
        cursor.execute(query)
        results = cursor.fetchall()
        for object in results:
            if not all(object):
                pass
            else:
                object_name = object[1]
                object_columns_list = object[2].split(',')
                object_columns_list = list(dict.fromkeys(object_columns_list))
                #object_columns_list = list(set(object_columns))
                color_code = "FBE870"
                columns = ""
                for column in object_columns_list:
                    columns += f"{column} text\n"

            ##Identify Parent
                parent_object = generate_parents(cursor,object[0])
                for parent in parent_object:
                    parent_name = parent[0]
                    parent_pk = parent[1]

                command += f"""Table {object_name} [headercolor: #{color_code}]{{\n\n{parent_pk} text [primary key,ref: <> {parent_name}.{parent_pk}]\n{hashdiff_naming.replace('@@SatName',object_name)} text \nldts timestamp_ntz\nrsrc text\n{columns}\n}}\n\n\n"""


    ##Pit

    query = f"""
            SELECT DISTINCT Pit_Physical_Table_Name, Dimension_Key_Physical_Name, Target_Primary_Key_Physical_Name,Tracked_Entity_Name
            FROM(
            
            SELECT DISTINCT 
            Pit_Physical_Table_Name
            ,COALESCE(p.Dimension_Key_Name,REPLACE(h.Target_Primary_Key_Physical_Name,'_h','_d')) AS Dimension_Key_Physical_Name
            ,h.Target_Primary_Key_Physical_Name
            ,h.Target_Hub_table_physical_name as Tracked_Entity_Name
            from pit p
            inner join standard_hub h on p.Tracked_Entity = h.Hub_Identifier
            inner join source_data src on src.Source_Table_Identifier = h.Source_Table_Identifier
            WHERE 1=1
            and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
            and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})

            UNION ALL

            SELECT DISTINCT 
            Pit_Physical_Table_Name
            ,COALESCE(p.Dimension_Key_Name,REPLACE(l.Target_Primary_Key_Physical_Name,'_l','_d')) AS Dimension_Key_Physical_Name
            ,l.Target_Primary_Key_Physical_Name
            ,l.Target_Link_table_physical_name as Tracked_Entity_Name
            from pit p
            inner join standard_link l on p.Tracked_Entity = l.Link_Identifier
            inner join source_data src on src.Source_Table_Identifier = l.Source_Table_Identifier
            WHERE 1=1
            and src.Source_System in ({str(source_name_list).replace('[','').replace(']','')})
            and src.Source_Object in ({str(source_object_list).replace('[','').replace(']','')})
            )
            """
    cursor.execute(query)
    results = cursor.fetchall()

    for object in results:

        object_name = object[0]
        #object_entity_type = object[1]
        object_columns = object[1]
        object_pk = object[2]
        tracked_entity = object[3]
        color_code = 'B9ECA6'

        command += f"""Table {object_name} [headercolor: #{color_code}]{{\n\n{object_columns} text [pk]\n{object_pk} text [ref: <> {tracked_entity}.{object_pk}]\nldts timestamp_ntz\nrsrc text\n\n}}\n\n\n"""
        

    ##TableGroups

    for source in source_list:
        source_system,source_object = source.split('_')
        group_name = get_source_group(cursor,source_system,source_object)
        query = f"""
                 SELECT DISTINCT Target_Hub_table_physical_name
                 from standard_hub h
                 inner join source_data src on h.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND h.Group_Name = '{group_name}'
                 AND h.Is_Primary_Source = '1'
                 
                 UNION ALL

                 SELECT DISTINCT Target_Link_table_physical_name
                 from standard_link l
                 inner join source_data src on l.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND l.Group_Name = '{group_name}'

                 UNION ALL

                 SELECT DISTINCT Target_Link_table_physical_name
                 from non_historized_link l
                 inner join source_data src on l.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND l.Group_Name = '{group_name}'

                 UNION ALL

                 SELECT DISTINCT Target_Satellite_table_physical_name
                 from standard_satellite s
                 inner join source_data src on s.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND s.Group_Name = '{group_name}'

                 UNION ALL

                 SELECT DISTINCT Target_Satellite_table_physical_name
                 from multiactive_satellite s
                 inner join source_data src on s.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND s.Group_Name = '{group_name}'

                 UNION ALL

                 SELECT DISTINCT Target_Satellite_table_physical_name
                 from non_historized_satellite s
                 inner join source_data src on s.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND s.Group_Name = '{group_name}'


                 UNION ALL

                 SELECT DISTINCT Pit_Physical_Table_Name
                 FROM(
                 SELECT DISTINCT Pit_Physical_Table_Name
                 from pit p
                 left join standard_hub h on p.Tracked_Entity = h.Hub_Identifier 
                 inner join source_data src on h.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND p.Group_Name = '{group_name}'

                 UNION ALL

                 SELECT DISTINCT Pit_Physical_Table_Name
                 from pit p
                 left join standard_link l on p.Tracked_Entity = l.Link_Identifier 
                 inner join source_data src on l.Source_Table_Identifier = src.Source_Table_Identifier
                 WHERE 1=1
                 AND src.Source_System = '{source_system}'
                 AND src.Source_Object = '{source_object}'
                 AND p.Group_Name = '{group_name}'
                 )

                 """
        cursor.execute(query)
        results = cursor.fetchall()
        tg_objects = ""
        for object in results:
            tg_objects += f"""{str(object).replace("('","").replace("',)","")}\n"""
        command += f"\nTableGroup {group_name} {{\n{tg_objects}\n}}\n"

######################################################################################################################################
    model_path = model_path.replace("@@SourceSystem","").replace("@@GroupName","").replace('@@timestamp',generated_timestamp)
    filename = os.path.join(model_path , "er_diagram.dbml")
          
    path = os.path.join(model_path)


    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(1))


    os.system(f'dbdocs build "{os.path.abspath(filename)}" --project ER_diagram')
