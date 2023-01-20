import os

from procs.bigquery.hub import generate_source_models

def generate_link_list(bigquery_client, source):

    source_name, source_object = source.split("_")

    query = f"""SELECT l.Link_Identifier,Target_link_table_physical_name,STRING_AGG(Target_column_physical_name order by l.Target_Column_Sort_Order)
                from `dbt_automation_test.link_entities` l
                inner join `dbt_automation_test.source_data` src on src.Source_table_identifier = l.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                group by 1,2
                """

    results = bigquery_client.query(query)

    return results


def generate_source_models(bigquery_client, link_id):

    command = ""

    query = f"""SELECT src.Source_Table_Physical_Name,STRING_AGG(l.Hub_primary_key_physical_name ORDER BY Target_Column_Sort_Order),src.Static_Part_of_Record_Source_Column
                FROM `dbt_automation_test.link_entities` l
                inner join `dbt_automation_test.source_data` src on l.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and Link_Identifier = '{link_id}'
                group by 1,3
                """

    results = bigquery_client.query(query)

    for source_table_row in results:
        source_table_name = source_table_row[0]
        fk_columns = source_table_row[1].split(',')

        if len(fk_columns) > 1: 
            fk_col_output = ""
            for fk in fk_columns: 
                fk_col_output += f"\n\t\t\t- '{fk}'"
        else:
            fk_col_output = "'" + fk_columns[0] + "'"
        
        command += f"\n\t{source_table_name}:\n\t\tfk_columns: {fk_col_output}"

        rsrc_static = source_table_row[2]

        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"

    return command


def generate_link_hashkey(bigquery_client, link_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM dbt_automation_test.link_entities
                WHERE link_identifier = '{link_id}'"""

    results = bigquery_client.query(query)

    for link_hashkey_row in results: #Usually a link only has one hashkey column, so results should only return one row
        link_hashkey_name = link_hashkey_row[0] 

    return link_hashkey_name
            

def main(bigquery_client, source, generated_timestamp, rdv_default_schema):

  link_list = generate_link_list(bigquery_client=bigquery_client, source=source)

  for link in link_list:
    
    link_name = link[1]
    link_id = link[0]
    fk_list = link[2].split(',')

    fk_string = ""
    for fk in fk_list:
      fk_string += f"\n\t- '{fk}'"

    source_models = generate_source_models(bigquery_client, link_id)
    link_hashkey = generate_link_hashkey(bigquery_client, link_id)
    
    source_name, source_object = source.split("_")
    model_path = model_path.replace('@@entitytype','Link').replace('@@SourceSystem',source_name)



    with open(os.path.join(".","templates","link.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@LinkHashkey', link_hashkey).replace('@@ForeignHashkeys', fk_string)
    

    filename = os.path.join(model_path, generated_timestamp , f"{link_name}.sql")
            
    path = os.path.join(model_path, generated_timestamp)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:   
    # Create a new directory because it does not exist 
      os.makedirs(path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))
      print(f"Created Link Model {link_name}")
