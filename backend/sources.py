import os
from pathlib import Path

def gen_sources(data_structure):
    cursor = data_structure['cursor']
    source = data_structure['source']
    generated_timestamp = data_structure['generated_timestamp']
    source_list = data_structure['source_list']
    model_path = data_structure['model_path']
    source_name = data_structure['source_name']   
    source_object = data_structure['source_object'] 
    source_database = data_structure['source_database']
    source_name_list = []
    source_object_list = []
    for source in source_list:
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
        source_tables = source[2].split(',')
        if command == "":
            command = command + 'version: 2\nsources:\n'
        
        command = command + f'\t- name: {source_system}\n\t  schema: {source_schema}\n\t  database: {source_database}\n\t  tables:\n'
        for table in source_tables:
            command = command + f'\t\t-  name: {table}\n'
        
        # Cross-platform path handling using pathlib
        model_path_obj = Path(model_path.replace("@@SourceSystem", "").replace("@@GroupName", "Sources").replace('@@timestamp', generated_timestamp))
        filename = model_path_obj / "sources.yml"

        # Create directory if it doesn't exist
        model_path_obj.mkdir(parents=True, exist_ok=True)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
    if data_structure['console_outputs']:    
        print(f"Created sources.yml")