import os

def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from pit where Pit_Identifier = '{object_id}' LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_sat_names(cursor,sat_ids):
    sat_names = []
    
    for id in sat_ids:
        query = f"""SELECT DISTINCT Target_Satellite_Table_Physical_Name from standard_satellite where Satellite_Identifier = '{id}'"""
        cursor.execute(query)
        sat_names.append(cursor.fetchone()[0])

    return sat_names

def get_pit_list(cursor, source):
    source_name, source_object = source.split("_.._")
    query = f"""SELECT 
    p.Pit_Identifier
    ,p.Pit_Physical_Table_Name
    ,h.Target_Hub_table_physical_name
    ,h.Target_Primary_Key_Physical_Name
    ,p.Satellite_Identifiers
    ,COALESCE(p.Snapshot_Model_Name,'control_snap_v1')
    ,COALESCE(p.Snapshot_Trigger_Column,'is_active')
    ,COALESCE(p.Dimension_Key_Name
    ,REPLACE(h.Target_Primary_Key_Physical_Name,'_h','_d'))
    FROM pit p
    inner join standard_hub h on p.Tracked_Entity = h.Hub_Identifier
    inner join source_data src on h.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    
    UNION

    SELECT p.Pit_Identifier, p.Pit_Physical_Table_Name,l.Target_Link_table_physical_name,l.Target_Primary_Key_Physical_Name,p.Satellite_Identifiers
    ,COALESCE(p.Snapshot_Model_Name,'control_snap_v1'),COALESCE(p.Snapshot_Trigger_Column,'is_active')
    ,COALESCE(p.Dimension_Key_Name, REPLACE(l.Target_Primary_Key_Physical_Name,'_l','_d'))
    FROM pit p
    inner join standard_link l on p.Tracked_Entity = l.Link_Identifier
    inner join source_data src on l.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    """

    cursor.execute(query)
    results = cursor.fetchall() 
    return results

def generate_pit(cursor, source, generated_timestamp, model_path):
    pit_list = get_pit_list(cursor=cursor, source=source)
    
    sat_names = ''
    tracked_entity = ''
    pk = ''
    snapshot_model_name = ''
    snapshot_trigger_column = ''
    dimension_key_name = ''
    pit_name = ''

    for pit in pit_list:
        pit_name = pit[1]
        tracked_entity = pit[2]
        pk = pit[3]
        satellites = pit[4]
        snapshot_model_name = pit[5]
        snapshot_trigger_column = pit[6]
        dimension_key_name = pit[7]
        sat_ids = satellites.split(';')
        sat_names = get_sat_names(cursor = cursor,sat_ids = sat_ids)

        group_name = get_groupname(cursor,pit[0])
        
        source_name, source_object = source.split("_.._")
        model_path_v1 = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        model_path_control = model_path.replace('@@GroupName','control').replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
    all_satellite_names = ''
    for sat in sat_names:
        all_satellite_names += f"\n\t- {sat}"



    with open(os.path.join(".","templates","pit_v1.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@TrackedEntity', tracked_entity).replace('@@PK', pk).replace('@@SnapshotModelName', snapshot_model_name).replace('@@SnapshotTriggerColumn', snapshot_trigger_column).replace('@@DimensionKey',dimension_key_name).replace('@@SatNames',all_satellite_names)
    
    if sat_names != '':
        filename = os.path.join(model_path_v1 , f"{pit_name}.sql")
                
        path = os.path.join(model_path_v1)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
        print(f"Created Pit Model {pit_name}")


        #control_snap_v0
        with open('templates/control_snap_v0.txt') as f1:
            control_snap = f1.read()
        f1.close()

        filename_snap1 = os.path.join(model_path_control , f"control_snap_v0.sql")
                
        snap_path = os.path.join(model_path_control)

        # Check whether the specified path exists or not
        isExist = os.path.exists(snap_path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(snap_path)

        with open(filename_snap1, 'w') as f:
            f.write(control_snap.expandtabs(2))



        #control_snap_v1
        with open('templates/control_snap_v1.txt') as f1:
            control_snap = f1.read()
        f1.close()

        filename_snap0 = os.path.join(model_path_control , f"control_snap_v1.sql")
                
        snap_path = os.path.join(model_path_control)

        # Check whether the specified path exists or not
        isExist = os.path.exists(snap_path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(snap_path)

        with open(filename_snap0, 'w') as f:
            f.write(control_snap.expandtabs(2))

        

