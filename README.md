<h1 align=center>Welcome to turbovault4dbt!</h1>
<img src="https://user-images.githubusercontent.com/81677440/214857459-13fb4674-06e7-40d1-abb6-1b43133f2f8b.png" width=100% align=center>

***

## What is TurboVault4dbt?
TurboVault4dbt is an open source tool that automatically generates dbt models according to our [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt)-templates. It uses a metadata input of Your Data Vault 2.0 from one of the supported databases and creates ready-to-process dbt-models.

## What are the prerequisites to use TurboVault4dbt?
TurboVault4dbt requires a metadata analysis done by hand and stored in a supported metadata storage. Furthermore, Python must be installed as TurboVault4dbt is a software written in Python.

<img src="https://www.getdbt.com/ui/img/logos/dbt-logo.svg" width=33% align=right>



To use the generated models, a [dbt project](https://docs.getdbt.com/docs/get-started/getting-started-dbt-core) is required. Addtionally, our dbt package [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) must be used, because the dbt models are calling macros of this package. 


## How does my metadata needs to look like?

You can find DDL scripts and templates for the metadata tables and the excel sheet [here]((https://github.com/ScalefreeCOM/turbovault4dbt/tree/main/metadata_ddl)). 

Your metadata needs to be stored in the following five tables/worksheets: 
- [source_data](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/source-data)
- [hub_entities](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/hubs)
- [link_entities](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/links)
- [hub_satellites](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/hub-satellites)
- [link_satellites](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/link-satellites)



[<img src="https://user-images.githubusercontent.com/81677440/196627704-e230a88f-270a-44b2-a07d-dcd06694bd48.jpg" width = 45% align = right>](https://www.scalefree.com)

## Where can I store my metadata?
Currently, TurboVault4dbt supports metadata input from 
- **Snowflake**
- **BigQuery**
- **Google Sheets**
- **Excel**


Our developers are constantly working on adding new connectors for more databases.

## How do I install TurboVault4dbt?
To install Turbovault4dbt, follow the instructions on [this page](https://github.com/ScalefreeCOM/turbovault4dbt/wiki/installation).
## How do I connect TurboVault4dbt with my metadata?
You can configure the connection to Your metadata storage in the [config.ini](config.md). Further explanation for the configuration input can be found [here](connect-to-metadata.md).

## How do I execute TurboVault4dbt?
To execute TurboVault4dbt, You need Python installed. Execute the script according to Your database, where Your metadata is stored e.g. Snowflake --> turbovault_snowflake.py, BigQuery --> turbovault_bigquery.py, and so on.

Then, a GUI will open that looks like this: 


<div align="center" >
<img src="https://user-images.githubusercontent.com/81677440/214863383-1d9d2005-1912-4748-9979-87caad65b6b7.png" width=70% align=center>
</div>

On the left side you can select which object types you want to generate. These are: 
- [Stage](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Staging)
- [Hub](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Hubs)
- Satellite (both [version 0](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Standard-Satellite-v0) and [version 1](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Standard-Satellite-v1))
- [Link](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Standard-Link)


The right side lists all available source objects inside the connected metadata storage. You can select as many of them as you like.

Now you can click on "start" and Turbovault4dbt will generate all neccessary dbt models that work with datavault4dbt!

*** 
<h1 style="text-align: center;">Designed for</h1>


[<img src="https://user-images.githubusercontent.com/81677440/195860893-435b5faa-71f1-4e01-969d-3593a808daa8.png" width=100% align=center>](https://www.scalefree.com/datavault4dbt/)
