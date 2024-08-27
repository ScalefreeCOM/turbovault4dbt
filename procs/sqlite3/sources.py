import os

def gen_sources(cursor,source_list,generated_timestamp, model_path, source_database):
    source_name_list = []
    source_object_list = []
    for source in source_list:
        #print(source)
        source_name,source_object = source.split('_')
        source_name_list.append(source_name)
        source_object_list.append(source_object)
    
    source_name_list = list(dict.fromkeys(source_name_list))
    source_object_list = list(dict.fromkeys(source_object_list))


    command = ""
    query = f"""SELECT DISTINCT Source_System, Source_Schema_Physical_Name, GROUP_CONCAT(Source_Table_Physical_Name)
    FROM source_data
    WHERE 1=1
    and Source_System in ({str(source_name_list).replace('[','').replace(']','')})
    and Source_Object in ({str(source_object_list).replace('[','').replace(']','')})
    GROUP BY Source_System, Source_Schema_Physical_Name"""
    cursor.execute(query)
    results = cursor.fetchall()
    timestamp = generated_timestamp

    for source in results:
        source_system = source[0]
        source_schema = source[1]
        #print(source[2])
        source_tables = source[2].split(',')
        if command == "":
            command = command + 'version: 2\nsources:\n'
        
        command = command + f'\t- name: {source_system}\n\t  schema: {source_schema}\n\t  database: {source_database}\n\t  tables:\n'
        for table in source_tables:
            command = command + f'\t\t-  name: {table}\n'
    model_path = model_path.replace("@@SourceSystem","").replace("@@GroupName","Sources").replace('@@timestamp',generated_timestamp)
    filename = os.path.join(model_path , "sources.yml")
          
    path = os.path.join(model_path)


    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))

    print(f"Created sources.yml")