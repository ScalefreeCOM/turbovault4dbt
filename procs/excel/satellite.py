from numpy import object_
import os

def gen_payload(payload_list):
    payload_string = ''
    for column in payload_list:
        payload_string = payload_string + f'\t- {column.lower()}\n'
    
    return payload_string

def generate_satellite_list(cursor, source):

    source_name, source_object = source.split("_")

    query = f"""SELECT DISTINCT Satellite_Identifier,Target_Satellite_Table_Physical_Name,Hub_Primary_Key_Physical_Name,GROUP_CONCAT(Target_Column_Physical_Name),
                Source_Table_Physical_Name,Load_Date_Column
                from 
                (SELECT DISTINCT hs.Satellite_Identifier,hs.Target_Satellite_Table_Physical_Name,hs.Hub_Primary_Key_Physical_Name,hs.Target_Column_Physical_Name,
                src.Source_Table_Physical_Name,src.Load_Date_Column FROM hub_satellites hs
                inner join source_data src on src.Source_table_identifier = hs.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                order by Target_Column_Sort_Order asc)
                group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Hub_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column

UNION

SELECT DISTINCT Satellite_Identifier,Target_Satellite_Table_Physical_Name,Link_primary_key_physical_name,GROUP_CONCAT(Target_Column_Physical_Name),
                Source_Table_Physical_Name,Load_Date_Column
                FROM(
                SELECT DISTINCT ls.Satellite_Identifier,ls.Target_Satellite_Table_Physical_Name,ls.Link_primary_key_physical_name,ls.Target_Column_Physical_Name,
                src.Source_Table_Physical_Name,src.Load_Date_Column
                from link_satellites ls
                inner join source_data src on src.Source_table_identifier = ls.Source_Table_Identifier
                where 1=1
                and src.Source_System = '{source_name}'
                and src.Source_Object = '{source_object}'
                order by Target_Column_Sort_Order asc)
                group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Link_primary_key_physical_name,Source_Table_Physical_Name,Load_Date_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results
        

def main(cursor,source, generated_timestamp, rdv_default_schema, model_path):
    
    satellite_list = generate_satellite_list(cursor=cursor, source=source)

    source_name, source_object = source.split("_")
    model_path_v0 = model_path.replace('@@entitytype','Satellites_v0').replace('@@SourceSystem',source_name)
    model_path_v1 = model_path.replace('@@entitytype','Satellites_v1').replace('@@SourceSystem',source_name)


    for satellite in satellite_list:
        satellite_id = satellite[0]
        satellite_name = satellite[1]
        hashkey_column = satellite[2]
        hashdiff_column = "hd_" + satellite_name
        payload_list = satellite[3].split(',')
        source_model = satellite[4]
        loaddate = satellite[5]

        payload = gen_payload(payload_list)
        
        #Satellite_v0
        with open(os.path.join(".","templates","sat_v0.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            
  
        satellite_model_name_splitted_list = satellite_name.split('_')
        satellite_model_name_splitted_list[-2] += '0'
        satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)

        filename = os.path.join(model_path_v0, generated_timestamp , f"{satellite_model_name_v0}.sql")
                
        path = os.path.join(model_path_v0, generated_timestamp)

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
        command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            
  

        filename_v1 = os.path.join(model_path_v1, generated_timestamp , f"{satellite_name}.sql")
                
        path_v1 = os.path.join(model_path_v1, generated_timestamp)

        # Check whether the specified path exists or not
        isExist_v1 = os.path.exists(path_v1)

        if not isExist_v1:   
        # Create a new directory because it does not exist 
            os.makedirs(path_v1)

        with open(filename_v1, 'w') as f:
            f.write(command_v1.expandtabs(2))
            print(f"Created Satellite Model {satellite_name}")





#v1
"""
{{ config(schema='public_release_test',
           materialized='view') }} 

{%- set yaml_metadata -%}
source_sat: 'opportunity_sfdc_data_lrn0_s'
src_hk: "hk_opportunity_h"
src_hd: 'hd_opportunity_data_sfdc_lrn_s'
src_ldts: "ldts"
ledts_alias='ledts'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sat_v1(source_sat=metadata_dict['source_sat'],
          src_hk=metadata_dict['src_hk'],
          src_hd=metadata_dict['src_hd'],
          src_ldts=metadata_dict['src_ldts'],
          ledts_alias=metadata_dict['ledts_alias']) }}

"""

#v0
"""
{{ config(schema='public_release_test',
           materialized='incremental',
           unique_key=['hk_opportunity_h', 'hd_opportunity_data_sfdc_lrn_s']) }} 



{%- set yaml_metadata -%}
source_model: "stage_opportunity" 
src_pk: "hk_opportunity_h"
src_hashdiff: 'hd_opportunity_data_sfdc_lrn_s'
src_payload:
  - encryption_key__c
  - product_type__c
  - opportunity_name_uppercase__c
  - hasopportunitylineitem
  - leadsource
  - name
  - type
  - forecastcategory
  - forecastcategoryname
  - isdeleted
src_rsrc: "rsrc"
src_ldts: "ldts"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sat_v0(src_pk=metadata_dict["src_pk"],
                src_hashdiff=metadata_dict["src_hashdiff"],
                src_payload=metadata_dict["src_payload"],
                src_ldts=metadata_dict["src_ldts"],
                src_rsrc=metadata_dict["src_rsrc"],
                source_model=metadata_dict["source_model"])   }}"""