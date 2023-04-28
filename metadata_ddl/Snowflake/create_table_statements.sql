CREATE TABLE standard_hub 
(
"Hub_Identifier" STRING,
"Target_Hub_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name" STRING,	
"Business_Key_Physical_Name" STRING,	
"Target_Column_Sort_Order" STRING,	
"Target_Primary_Key_Physical_Name" STRING
);

CREATE TABLE standard_link  
(
"Link_Identifier" STRING,
"Target_Link_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name"	 STRING,
"Prejoin_Table_Identifier" STRING,	
"Prejoin_Table_Column_Name" STRING,	
"Prejoin_Extraction_Column_Name" STRING,	
"Prejoin_Target_Column_Alias" STRING,	
"Hub_identifier" STRING,
"Hub_Primary_Key_Physical_Name" STRING,	
"Target_Column_Physical_Name" STRING,	
"Target_Column_Sort_Order" STRING,	
"Target_Primary_Key_Physical_Name" STRING
);

CREATE TABLE standard_satellite  
(
"Satellite_Identifier" STRING,
"Target_Satellite_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name" STRING,	
"Parent_Identifier" STRING,
"Parent_Primary_Key_Physical_Name" STRING,	
"Target_Column_Physical_Name" STRING,	
"Target_Column_Sort_Order" STRING
);

CREATE TABLE multiactive_satellite  
(
"MA_Satellite_Identifier" STRING,
"Target_Satellite_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name" STRING,	
"Parent_Identifier" STRING,
"Parent_Primary_Key_Physical_Name" STRING,	
"Multi_Active_Attributes" STRING,
"Target_Column_Physical_Name" STRING,	
"Target_Column_Sort_Order" STRING
);


CREATE TABLE non_historized_link  
(
"NH_Link_Identifier" STRING,
"Target_Link_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name"	 STRING,
"Prejoin_Table_Identifier" STRING,	
"Prejoin_Table_Column_Name" STRING,	
"Prejoin_Extraction_Column_Name" STRING,	
"Prejoin_Target_Column_Alias" STRING,	
"Hub_identifier" STRING,
"Hub_Primary_Key_Physical_Name" STRING,	
"Target_Column_Physical_Name" STRING,	
"Target_Column_Sort_Order" STRING,	
"Target_Primary_Key_Physical_Name" STRING
);

CREATE TABLE non_historized_satellite  
(
"NH_Satellite_Identifier" STRING,
"Target_Satellite_Table_Physical_Name" STRING,	
"Source_Table_Identifier" STRING,	
"Source_Column_Physical_Name" STRING,	
"Parent_Identifier" STRING,
"Parent_Primary_Key_Physical_Name" STRING,	
"Target_Column_Physical_Name" STRING	
);

CREATE TABLE pit  
(
"Pit_Identifier" STRING,
"Pit_Physical_Table_Name" STRING,	
"Tracked_Entity" STRING,	
"Satellite_Identifiers" STRING,	
"Snapshot_Model_Name" STRING,
"Snapshot_Trigger_Column" STRING,	
"Dimension_Key_Name" STRING	
);

CREATE TABLE source_data  
(
  "Source_Table_Identifier" STRING,
  "Source_System" STRING,
  "Source_Object" STRING,
  "Source_Schema_Physical_Name" STRING,
  "Source_Table_Physical_Name" STRING,
  "Record_Source_Column" STRING,
  "Static_Part_of_Record_Source_Column" STRING,
  "Load_Date_Column" STRING
);