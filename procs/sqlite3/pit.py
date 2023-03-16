import os



def get_sat_name(cursor,sat_ids):
    sat_names = []
    for id in sat_ids:
        query = f"""SELECT DISTINCT Target_Satellite_Table_Physical_Name from standard_satellite where Satellite_Identifier = '{id}'"""
        cursor.execute(query)
        sat_names.append(cursor.fetchone()[0])

    return sat_names

def get_pit_list(cursor, source):
    source_name, source_object = source.split("_")
    query = f"""SELECT 
    p.Pit_Identifier
    , p.Pit_Physical_Table_Name
    ,h.Target_Hub_table_physical_name
    ,h.Target_Primary_Key_Physical_Name
    ,p.Satellite_Identifiers
    ,COALESCE(p.Snapshot_Model_Name,'control_snap_v1')
    ,COALESCE(p.Snapshot_Trigger_Column,'is_active')
    ,COALESCE(p.Dimension_Key_Name
    ,REPLACE(h.Target_Primary_Key_Physical_Name,'_h','_d'))
    FROM pits p
    inner join hub_entities h on p.Tracked_Entity = h.Hub_Identifier
    inner join source_data src on h.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    
    UNION

    SELECT p.Pit_Identifier, p.Pit_Physical_Table_Name,l.Target_Link_table_physical_name,l.Target_Primary_Key_Physical_Name,p.Satellite_Identifiers
    ,COALESCE(p.Snapshot_Model_Name,'control_snap_v1'),COALESCE(p.Snapshot_Trigger_Column,'is_active')
    ,COALESCE(p.Dimension_Key_Name, REPLACE(l.Target_Primary_Key_Physical_Name,'_l','_d'))
    FROM pits p
    inner join link_entities l on p.Tracked_Entity = l.Link_Identifier
    inner join source_data src on l.Source_table_identifier = src.Source_table_identifier
    WHERE 1=1
    and src.Source_System = '{source_name}'
    and src.Source_Object = '{source_object}'
    """

    cursor.execute(query)
    results = cursor.fetchall() 

    #print(results)
    return results

def generate_pit(cursor, source, generated_timestamp, rdv_default_schema, model_path):
    pit_list = get_pit_list(cursor=cursor, source=source)

    for pit in pit_list:
        pit_name = pit[1]
        pit_id = pit[0]
        tracked_entity = pit[2]
        pk = pit[3]
        satellites = pit[4]
        snapshot_model_name = pit[5]
        snapshot_trigger_column = pit[6]
        dimension_key_name = pit[7]
        sat_ids = satellites.split(';')
        sat_names = get_sat_name(cursor,sat_ids)

    all_satellite_names = ''
    for sat in sat_names:
        all_satellite_names += f"\n\t- {sat}"

    source_name, source_object = source.split("_")
    model_path_v1 = model_path.replace('@@entitytype','Pit_v1').replace('@@SourceSystem',source_name)
    with open(os.path.join(".","templates","pit_v1.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@TrackedEntity', tracked_entity).replace('@@PK', pk).replace('@@SnapshotModelName', snapshot_model_name).replace('@@SnapshotTriggerColumn', snapshot_trigger_column).replace('@@DimensionKey',dimension_key_name).replace('@@SatNames',all_satellite_names)
    

    filename = os.path.join(model_path_v1, generated_timestamp , f"{pit_name}.sql")
            
    path = os.path.join(model_path_v1, generated_timestamp)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:   
    # Create a new directory because it does not exist 
      os.makedirs(path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))
      print(f"Created Pit Model {pit_name}")