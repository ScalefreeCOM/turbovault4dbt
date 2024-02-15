import os
def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from standard_hub where Hub_Identifier = '{object_id}' ORDER BY Is_Primary_Source LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def generate_hub_list(cursor, source):

    source_name, source_object = source.split("_")

    query = f"""SELECT Hub_Identifier,Target_Hub_table_physical_name,GROUP_CONCAT(Business_Key_Physical_Name)
                from 
                (SELECT h.Hub_Identifier,h.Target_Hub_table_physical_name,(Business_Key_Physical_Name),h.Group_Name    
                FROM standard_hub h
                inner join source_data src on src.Source_table_identifier = h.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                and h.Is_Primary_Source = '1'
                ORDER BY h.Target_Column_Sort_Order
                )
                group by Hub_Identifier,Target_Hub_table_physical_name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_source_models(cursor, hub_id):

    command = ""

    query = f"""SELECT Source_Table_Physical_Name,GROUP_CONCAT(Source_Column_Physical_Name),Static_Part_of_Record_Source_Column
                FROM 
                (SELECT src.Source_Table_Physical_Name,h.Source_Column_Physical_Name,src.Static_Part_of_Record_Source_Column FROM standard_hub h
                inner join source_data src on h.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and Hub_Identifier = '{hub_id}'
                ORDER BY h.Target_Column_Sort_Order)
                group by Source_Table_Physical_Name,Static_Part_of_Record_Source_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()

    for source_table_row in results:
        source_table_name = 'stg_' + source_table_row[0].lower()
        bk_columns = source_table_row[1].split(',')

        if len(bk_columns) > 1: 
            bk_col_output = ""
            for bk in bk_columns: 
                bk_col_output += f"\n\t\t\t- '{bk}'"
        else:
            bk_col_output = "'" + bk_columns[0] + "'"
        
        command += f"\n\t{source_table_name}:\n\t\tbk_columns: {bk_col_output}"

        rsrc_static = source_table_row[2]

        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"

    return command


def generate_hashkey(cursor, hub_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM standard_hub
                WHERE hub_identifier = '{hub_id}'"""

    cursor.execute(query)
    results = cursor.fetchall()

    for hashkey_row in results: #Usually a hub only has one hashkey column, so results should only return one row
        hashkey_name = hashkey_row[0] 

    return hashkey_name

def generate_primarykey_constraint(cursor, hub_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Constraint_Name 
                FROM standard_hub
                WHERE hub_identifier = '{hub_id}'
                      AND Is_Primary_Source = 1 """

    cursor.execute(query)
    results = cursor.fetchall()

    for pk in results: #Usually a hub only has one hashkey column, so results should only return one row
        primarykey_constraint = pk[0]
        if primarykey_constraint == None:
            primarykey_constraint = ""

    return primarykey_constraint

def generate_hub(cursor,source, generated_timestamp,rdv_default_schema,model_path):

    hub_list = generate_hub_list(cursor=cursor, source=source)

    source_name, source_object = source.split("_")

    for hub in hub_list:

        hub_name = hub[1]
        hub_id = hub[0]
        bk_list = hub[2].split(',')
        group_name = get_groupname(cursor,hub_id)

        bk_string = ""
        for bk in bk_list:
            bk_string += f"\n\t- '{bk}'"

        source_models = generate_source_models(cursor, hub_id)
        hashkey = generate_hashkey(cursor, hub_id)
        primarykey_constraint = generate_primarykey_constraint(cursor, hub_id)
    
        with open(os.path.join(".","templates","hub.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@Hashkey', hashkey).replace('@@BusinessKeys', bk_string).replace('@@Target_Primary_Key_Constraint_Name', primarykey_constraint)
           
        model_path = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        filename = os.path.join(model_path,  f"{hub_name}.sql")
                
        path = os.path.join(model_path)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Hub Model {hub_name}")