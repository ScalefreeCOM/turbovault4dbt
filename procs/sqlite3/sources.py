import os

def gen_sources(cursor,generated_timestamp, model_path):
    command = ""
    query = f"""SELECT DISTINCT Source_System, Source_Schema_Physical_Name, GROUP_CONCAT(Source_Table_Physical_Name) 
    FROM source_data
    GROUP BY Source_System, Source_Schema_Physical_Name"""
    cursor.execute(query)
    results = cursor.fetchall()
    timestamp = generated_timestamp

    for source in results:
        source_system = source[0]
        source_schema = source[1]
        source_tables = source[2].split(',')
        if command == "":
            command = command + 'sources:\n'
        
        command = command + f'\t- name: {source_system}\n\t database: @@DB\n\t schema: {source_schema}\n\t tables:\n'
        for table in source_tables:
            command = command + f'\t\t- {table}\n'
    model_path = model_path.replace("/@@SourceSystem","")
    filename = os.path.join(model_path, timestamp , "sources.yml")
          
    path = os.path.join(model_path, timestamp)


    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))

    print(f"Created sources.yml")