import os




def get_groupname(cursor, object_id):
    query = f"""SELECT DISTINCT GROUP_NAME from link_eff_satellite where Link_Effectivity_Satellite = '{object_id}' ORDER BY Link_Effectivity_Satellite LIMIT 1"""
    cursor.execute(query)
    return cursor.fetchone()[0]


def generate_lef_sat_list(cursor, source):
    source_name, source_object = source.split("_.._")

    query = f"""SELECT Link_Effectivity_Satellite, Link_Identifier,Target_link_table_physical_name, Source_Table_Physical_Name, GROUP_CONCAT(COALESCE(Target_column_physical_name,Source_column_physical_name)) 
                FROM   (SELECT f.Link_Effectivity_Satellite, l.Link_Identifier,Target_link_table_physical_name, src.Source_Table_Physical_Name, Target_column_physical_name,Source_column_physical_name
                        FROM link_eff_satellite f inner join standard_link l on f.Link_Identifier = l.Link_Identifier
                        inner join source_data src on src.Source_table_identifier = l.Source_Table_Identifier
                        where 1=1
                        and src.Source_System = '{source_name}'
                        and src.Source_Object = '{source_object}'
                        order by l.Target_Column_Sort_Order)
                GROUP BY Link_Effectivity_Satellite, Link_Identifier, Target_link_table_physical_name, Source_Table_Physical_Name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_key_list(cursor, link_id, key_type):
    command = ""

    if key_type == "driving_key":
        key_type = 1
    else:
        key_type = 0

    query = f"""SELECT Target_column_physical_name
                FROM standard_link l
                where 1=1
                and Driving_Key = {key_type}
                and Link_Identifier = '{link_id}'
                """


    cursor.execute(query)
    results = cursor.fetchall()
    driving_key_output = ""

    for driving_key_row in results:
        driving_key = driving_key_row[0].upper()
        driving_key_output  += f"\n\t- '{driving_key}'"

    return driving_key_output


def generate_link_hashkey(cursor, link_id):
    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM standard_link
                WHERE link_identifier = '{link_id}'"""

    cursor.execute(query)
    results = cursor.fetchall()

    for link_hashkey_row in results:  # Usually a link only has one hashkey column, so results should only return one row
        link_hashkey_name = link_hashkey_row[0]

    return link_hashkey_name


def generate_primarykey_constraint(cursor, lef_sat_name, version):
    query = f"""SELECT DISTINCT f.Target_Primary_Key_Constraint_Name, l.Target_Primary_Key_Physical_Name 
                FROM link_eff_satellite f INNER JOIN standard_link l ON f.link_identifier = l.link_identifier
                WHERE f.Link_Effectivity_Satellite = '{lef_sat_name}'
                      AND l.Target_Column_Sort_Order = 1 """

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

            primarykey_constraint = "\"{{ datavault4dbt.primary_key(name='"+primarykey_constraint+"', columns=['"+primarykey_column+"'], tabletype='effectivity_satellite') }}\""

    return primarykey_constraint

def generate_foreignkey_constraints(cursor, lef_sat_name, version):
    query = f"""SELECT DISTINCT f.Target_Foreign_Key_Constraint_Name,
                    l.Target_link_table_physical_name,
                    l.Target_Primary_Key_Physical_Name,
                    f.Link_Effectivity_Satellite
                    FROM link_eff_satellite f INNER JOIN standard_link l ON f.link_identifier = l.link_identifier  
                    WHERE f.Link_Effectivity_Satellite = '{lef_sat_name}'
                    AND f.Target_Foreign_Key_Constraint_Name IS NOT NULL"""

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

            if i != 0:
                foreignkey_constraints += ","

            if version == 1:
                Target_Foreign_Key_Constraint_Name += '1'

            if version == 0:
                fk_table_relation_splitted_list = fk_table_relation.split('_')
                fk_table_relation_splitted_list[-2] += '0'
                fk_table_relation = '_'.join(fk_table_relation_splitted_list)

            foreignkey_constraints += f"\"" + "{{ datavault4dbt.foreign_key(name=\'" + Target_Foreign_Key_Constraint_Name + "', pk_table_relation='" + pk_table_relation + "', pk_column_names=['" + pk_column_names + "'], fk_table_relation='" + fk_table_relation + "', fk_column_names=['" + fk_column_names + "']) }} \""
            # foreignkey_constraints = ""#no bug execution
            # fk_string += f"\n\t- '{fk}'"
        i = i + 1
    return foreignkey_constraints


def generate_lef_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, stage_prefix):
    lef_sat_list = generate_lef_sat_list(cursor=cursor, source=source)

    for lefsat in lef_sat_list:

        lef_sat_name = lefsat[0]
        link_id = lefsat[1]
        source_model = stage_prefix + lefsat[3].lower()
        group_name = get_groupname(cursor, lef_sat_name)
        driving_key = generate_key_list(cursor, link_id, 'driving_key')
        secondary_fks = generate_key_list(cursor, link_id, 'secondary_fks')

        '''     fk_list = lefsat[2].split(',')
                fk_string = ""
                for fk in fk_list:
                    fk_string += f"\n\t- '{fk}'"
        '''

        #source_models = generate_source_models(cursor, link_id, stage_prefix)
        link_hashkey = generate_link_hashkey(cursor, link_id)

        # Satellite_v0
        primarykey_constraint = generate_primarykey_constraint(cursor, lef_sat_name,0)
        foreignkey_constraints = generate_foreignkey_constraints(cursor, lef_sat_name, 0)

        if primarykey_constraint != "" and foreignkey_constraints != "":
            primarykey_constraint += ", "

        source_name, source_object = source.split("_.._")
        model_path = model_path.replace('@@GroupName', group_name).replace('@@SourceSystem', source_name).replace('@@SourceModel', source_model).replace(
            '@@timestamp', generated_timestamp)

        with open(os.path.join(".", "templates", "eff_sat_v0.txt"), "r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModel', source_model).replace(
            '@@LinkHashkey', link_hashkey).replace('@@PrimaryKeyConstraint', primarykey_constraint).replace(
            '@@ForeignKeyConstraints', foreignkey_constraints).replace('@@DrivingKey', driving_key).replace('@@SecondaryForeignKeys', secondary_fks)

        lef_sat_name_splitted_list = lef_sat_name.split('_')
        lef_sat_name_splitted_list[-2] += '0'
        lef_sat_name_v0 = '_'.join(lef_sat_name_splitted_list)

        filename = os.path.join(model_path , f"{lef_sat_name_v0}.sql")

        path = os.path.join(model_path)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Link Model {lef_sat_name_v0}")

        #Satellite_v1
        primarykey_constraint = generate_primarykey_constraint(cursor, lef_sat_name, 1)
        foreignkey_constraints = generate_foreignkey_constraints(cursor, lef_sat_name, 1)

        if primarykey_constraint != "" and foreignkey_constraints != "":
            foreignkey_constraints = ", " + foreignkey_constraints


        with open(os.path.join(".", "templates", "eff_sat_v1.txt"), "r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@SatName', lef_sat_name_v0).replace('@@LinkHashkey', link_hashkey).replace('@@PrimaryKeyConstraint', primarykey_constraint).replace(
            '@@ForeignKeyConstraints', foreignkey_constraints)

        filename = os.path.join(model_path, f"{lef_sat_name}.sql")

        path = os.path.join(model_path)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Link Model {lef_sat_name}")
