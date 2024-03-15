from numpy import object_
import os
def get_groupname(cursor,object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from standard_satellite where Satellite_Identifier = '{object_id}' ORDER BY Target_Column_Sort_Order LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def gen_payload(payload_list):
    payload_string = ''
    
    for column in payload_list:
        payload_string = payload_string + f'\t- {column.lower()}\n'
    
    return payload_string

def generate_satellite_list(cursor, source):

    source_name, source_object = source.split("_.._")

    query = f"""SELECT DISTINCT Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,GROUP_CONCAT(Target_Column_Physical_Name),
                Source_Table_Physical_Name,Load_Date_Column,Group_Name
                from 
                (SELECT DISTINCT hs.Satellite_Identifier,hs.Target_Satellite_Table_Physical_Name,hs.Parent_Primary_Key_Physical_Name,hs.Target_Column_Physical_Name,
                src.Source_Table_Physical_Name,src.Load_Date_Column,hs.Group_Name FROM standard_satellite hs
                inner join source_data src on src.Source_table_identifier = hs.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                order by Target_Column_Sort_Order asc)
                group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Parent_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column"""

    cursor.execute(query)
    results = cursor.fetchall()

    return results
        
def generate_primarykey_constraint(cursor, object_id, version):

    query = f"""SELECT DISTINCT Target_Primary_Key_Constraint_Name, Parent_Primary_Key_Physical_Name
                FROM standard_satellite
                WHERE satellite_identifier = '{object_id}'
                      AND Target_Column_Sort_Order = 1 """

    cursor.execute(query)
    results = cursor.fetchall()

    for pk in results: #Usually a hub only has one hashkey column, so results should only return one row

        primarykey_constraint = pk[0]
        primarykey_column = pk [1]

        if primarykey_constraint == None:
            primarykey_constraint = ""
        else:
            if version == 1:
                primarykey_constraint += "1"

            primarykey_constraint = "\"{{ datavault4dbt.primary_key(name='"+primarykey_constraint+"', columns=['"+primarykey_column+"'], tabletype='satellite') }}\""

    return primarykey_constraint

def generate_foreignkey_constraints(cursor, object_id, version):

    query = f"""SELECT DISTINCT s.Target_Foreign_Key_Constraint_Name,
                    l.Target_link_table_physical_name,
                    l.Target_Primary_Key_Physical_Name,
                    s.Target_Satellite_Table_Physical_Name,
                    s.Target_Column_Physical_Name
                    FROM standard_satellite s INNER JOIN standard_link l ON s.Parent_Identifier = l.Link_Identifier  
                    WHERE s.Satellite_Identifier = '{object_id}'
                    AND s.Target_Foreign_Key_Constraint_Name IS NOT NULL
                    AND s.Target_Column_Sort_Order = 1
                UNION
                SELECT DISTINCT s.Target_Foreign_Key_Constraint_Name,
                    h.Target_Hub_table_physical_name,
                    h.Target_Primary_Key_Physical_Name,
                    s.Target_Satellite_Table_Physical_Name,
                    s.Target_Column_Physical_Name
                    FROM standard_satellite s INNER JOIN standard_hub h ON s.Parent_Identifier = h.Hub_Identifier  
                    WHERE s.Satellite_Identifier = '{object_id}'
                    AND s.Target_Foreign_Key_Constraint_Name IS NOT NULL
                    AND s.Target_Column_Sort_Order = 1

"""


    cursor.execute(query)
    results = cursor.fetchall()
    i = 0
    foreignkey_constraints = ""



    for fk in results:

        if fk[0] == None:
            foreignkey_constraints = ""
        else:
            Target_Foreign_Key_Constraint_Name = fk[0]
            pk_table_relation = fk[1]
            pk_column_names = fk[2]
            fk_table_relation = fk[3]
            fk_column_names = fk[2]

            if version == 1:
                Target_Foreign_Key_Constraint_Name += '1'

            if version == 0:
                fk_table_relation_splitted_list = fk_table_relation.split('_')
                fk_table_relation_splitted_list[-2] += '0'
                fk_table_relation = '_'.join(fk_table_relation_splitted_list)

            foreignkey_constraints += f"\"" + "{{ datavault4dbt.foreign_key(name=\'" + Target_Foreign_Key_Constraint_Name + "', pk_table_relation='" + pk_table_relation + "', pk_column_names=['" + pk_column_names + "'], fk_table_relation='" + fk_table_relation + "', fk_column_names=['" + fk_column_names + "']) }} \""

        i = i + 1
    return foreignkey_constraints

def generate_satellite(cursor,source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming, stage_prefix):
    
    satellite_list = generate_satellite_list(cursor=cursor, source=source)

    source_name, source_object = source.split("_.._")

    for satellite in satellite_list:
        satellite_id = satellite[0]
        satellite_name = satellite[1]
        hashkey_column = satellite[2]
        hashdiff_column = hashdiff_naming.replace('@@SatName',satellite_name)
        payload_list = satellite[3].split(',')
        source_model = stage_prefix+satellite[4].lower()
        loaddate = satellite[5]
        group_name = get_groupname(cursor,satellite[0])
        model_path_v0 = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)
        model_path_v1 = model_path.replace('@@GroupName',group_name).replace('@@SourceSystem',source_name).replace('@@timestamp',generated_timestamp)

        payload = gen_payload(payload_list)
        primarykey_constraint = generate_primarykey_constraint(cursor, satellite_id, 0)
        foreignkey_constraints = generate_foreignkey_constraints(cursor, satellite_id, 0)

        if primarykey_constraint != "" and foreignkey_constraints != "":
            primarykey_constraint += ", "

        #Satellite_v0
        with open(os.path.join(".","templates","sat_v0.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema).replace('@@PrimaryKeyConstraint', primarykey_constraint).replace('@@ForeignKeyConstraints', foreignkey_constraints)
  
        satellite_model_name_splitted_list = satellite_name.split('_')
        satellite_model_name_splitted_list[-2] += '0'
        satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

        filename = os.path.join(model_path_v0 , f"{satellite_model_name_v0}.sql")
                
        path = os.path.join(model_path_v0)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command_v0.expandtabs(2))
            print(f"Created Satellite Model {satellite_model_name_v0}")

        #Satellite_v1
        with open(os.path.join(".","templates","sat_v1.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        primarykey_constraint = generate_primarykey_constraint(cursor, satellite_id, 1)
        foreignkey_constraints = generate_foreignkey_constraints(cursor, satellite_id, 1)

        if primarykey_constraint != "" and foreignkey_constraints != "":
            primarykey_constraint += ", "

        command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema).replace('@@PrimaryKeyConstraint', primarykey_constraint).replace('@@ForeignKeyConstraints', foreignkey_constraints)
            
  

        filename_v1 = os.path.join(model_path_v1 , f"{satellite_name}.sql")
                
        path_v1 = os.path.join(model_path_v1)

        # Check whether the specified path exists or not
        isExist_v1 = os.path.exists(path_v1)

        if not isExist_v1:   
        # Create a new directory because it does not exist 
            os.makedirs(path_v1)

        with open(filename_v1, 'w') as f:
            f.write(command_v1.expandtabs(2))
            print(f"Created Satellite Model {satellite_name}")