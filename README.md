<h1 align=center>Welcome to TurboVault4dbt</h1>
<img src="https://user-images.githubusercontent.com/81677440/214857459-13fb4674-06e7-40d1-abb6-1b43133f2f8b.png" width=100% align=center url="https://www.scalefree.com/consulting/turbovault4dbt">

***

## What is TurboVault4dbt?
TurboVault4dbt is an open-source tool that automatically generates dbt models according to our [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) templates. It uses a metadata input of your Data Vault 2.0 from one of the supported databases and creates ready-to-process dbt-models.

## What are the prerequisites to use TurboVault4dbt?
- TurboVault4dbt requires a metadata analysis done by hand and stored in supported metadata storage.
- Python must be installed as TurboVault4dbt is a software written in Python.

<img src="https://www.getdbt.com/ui/img/logos/dbt-logo.svg" width=22% align=right>


- A [dbt project](https://docs.getdbt.com/docs/get-started/getting-started-dbt-core) is required to use the generated models.
- Additionally, our dbt package [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) must be used since the dbt models are using datavault4dbt macros. 


## How does my metadata needs to look like?

You can find DDL scripts and templates for the metadata tables and the Excel sheet [here]((https://github.com/ScalefreeCOM/turbovault4dbt/tree/main/metadata_ddl))]((https://github.com/ScalefreeCOM/turbovault4dbt/tree/main/metadata_ddl)). 

Your metadata needs to be stored in the following tables/worksheets: 
- [Source Data](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/source-data)
- [Standard Hubs](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/hubs)
- [Standard Links](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/links)
- [Non-Historized Links](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/non-Historized-links)
- [Standard Satellites](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/standard-satellites)
- [Non-Historized Satellites](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/non-Historized-satellites)
- [Multi-Active Satellites](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/multiactive-satellites)
- [Point-In-Time Tables](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/Point-In-Time)
- [Reference Tables](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/reference-tables)


[<img src="https://user-images.githubusercontent.com/81677440/196627704-e230a88f-270a-44b2-a07d-dcd06694bd48.jpg" width = 33% align = right>](https://www.scalefree.com)

## Where can I store my metadata?
Currently, TurboVault4dbt supports metadata input from 
- **Snowflake**
- **BigQuery**
- **Google Sheets**
- **Excel**
- **SQLite DB Files**

Our developers are constantly working on adding new connectors for more databases.

## How do I install TurboVault4dbt?
To install TurboVault4dbt, follow the instructions on [this page](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/installation).
## How do I connect TurboVault4dbt with my metadata?
You can configure the connection to your metadata storage in the config.ini located in turbovault4dbt/backend
/config/
Whichever section is found to be relevant to your project needs to be configured, and the rest could be deleted.
In the next run, the section configured in the config.ini will appear in Turbo Vault's UI.

## How do I run TurboVault4dbt?
To run TurboVault4dbt, you need to install Python, as well as the required packages. Then, simply execute main.py, which will open a GUI that looks like this: 

<div align="center" >
<img src="https://github.com/user-attachments/assets/3dc3d4a0-5770-44ed-b7c6-dc77f052a0ad" width=70% align=center>
</div>

First, choose your metadata input platform, where you have your metadata stored, from the dropdown menu.

From the Sources section, choose the source objects that you would like to use. At least one source object should be selected.

From the Entities section, you can select which types of entities you want to generate. These are: 
- [Stage](https://www.datavault4dbt.com/documentation/macro-instructions/staging/)
- [Standard Hub](https://www.datavault4dbt.com/documentation/macro-instructions/hubs/standard-hub/)
- [Standard Link](https://www.datavault4dbt.com/documentation/macro-instructions/links/standard-link/)
- Satellite (both [version 0](https://www.datavault4dbt.com/documentation/macro-instructions/satellites/standard-satellite/standard-satellite-v0) and [version 1](https://www.datavault4dbt.com/documentation/macro-instructions/satellites/standard-satellite/standard-satellite-v1))
- Multi-Active Satellite (both [version 0](https://www.datavault4dbt.com/documentation/macro-instructions/satellites/multi-active-satellite/multi-active-satellite-v0/) and [version 1](https://www.datavault4dbt.com/documentation/macro-instructions/satellites/multi-active-satellite/multi-active-satellite-v1/))
- [Non-Historized Link](https://www.datavault4dbt.com/documentation/macro-instructions/links/non-historized-link/)
- [Non-Historized Satellite](https://www.datavault4dbt.com/documentation/macro-instructions/satellites/non-historized-satellite/)
- [Dependent Child Link](https://www.datavault4dbt.com/documentation/macro-instructions/links/dependent-child-keys/)
- [Point-In-Time](https://www.datavault4dbt.com/documentation/macro-instructions/business-vault/pit/)
- [Reference Table](https://www.datavault4dbt.com/documentation/macro-instructions/reference-data/reference-tables/)

By using the checkboxes:
-  a [sources.yml](https://github.com/ScalefreeCOM/turbovault4dbt/yml-generation) can be generated,
-  properties [.yml](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/yml-generation) files can be generated for each source object,
-  and a [Data Vault Model Visualization](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/Visualization-of-Your-Raw-Vault) can be created using the external tool DBDocs.

Now you can click on the START button and TurboVault4dbt will generate all necessary dbt models that work with [datavault4dbt](https://www.datavault4dbt.com)!

## Releases
[v2.0 (14.11.2024)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v2.0) - Current Version<br>
[v1.2 (18.09.2024)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.2)<br>
[v1.1.2 (23.01.2024)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.1.2)<br>
[v1.1.1 (24.05.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.1.1)<br>
[v1.1.0 (22.05.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.1.0)<br>
[v1.0.3 (16.02.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.0.3)<br>
[v1.0.2 (13.02.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.0.2)<br>
[v1.0.1 (30.01.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.0.1)<br>
[v1.0.0 (26.01.2023)](https://github.com/ScalefreeCOM/turbovault4dbt/releases/tag/v1.0.0)<br>
*** 
<h1 style="text-align: center;">Designed for</h1>


[<img src="https://user-images.githubusercontent.com/81677440/195860893-435b5faa-71f1-4e01-969d-3593a808daa8.png" width=100% align=center>](https://www.datavault4dbt.com)
