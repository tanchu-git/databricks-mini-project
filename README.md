# Mini project with Databricks and Azure.

The Ergast Developer API provides a historical record of motor racing data for non-commercial purposes. The API provides data for the Formula One series, from the beginning of the world championships in 1950.

I will be messing around with a few dataset, utilizing Unity Catalog in Azure Databricks workspace.

## [Ergast](http://ergast.com/mrd/) Formula One database
Here's a [overview](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/876ca38e-569c-49d8-879e-ab99a9a2a504) of the database, with detailed attributes of the tables [here](http://ergast.com/docs/f1db_user_guide.txt). I will be using ```drivers``` and ```results``` tables, downloaded as ```JSON``` files and stored in Azure Data Lake Storage Gen2.

## Architecture overview
![Screenshot 2023-08-11 160806](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/70c2587a-d123-4090-87a0-a2dc905ecf46)

## Databricks workspace, Azure storage account and access connector
First step is creating a Databricks Workspace through Azure portal. Unity Catalog Metastore needs to be created by navigating to [manage account](https://accounts.azuredatabricks.net/) page. I will need to configure a storage container and Azure managed identity that Unity Catalog can use to store and access managed table data. Then, assign my Databricks workspace to the metastore.

I've created a storage account ```externalucstorage``` as the external storage location. Since I plan to create 3 schemas (databases) with Unity Catalog, I will keep them separately stored in 3 containers (```bronze```, ```silver``` and ```gold```).

![Screenshot 2023-08-11 161039](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/629d5b9c-aec5-4f46-af69-7266542f1c76)

```drivers.json``` and ```results.json``` will be uploaded to ```bronze``` container.

### Access Connector For Azure Databricks
Databricks Unity Catalog can be configured to use an Azure managed identity to access storage containers in Azure. After creating a ```Access Connector For Azure Databricks``` in Azure portal, I assigned the role ```Storage Blob Data Contributor``` to the connector within ```externalucstorage``` storage account.

Then a storage credential ```external_storage_cred```, using the ```Access connector ID``` is created in Databricks workspace. Now Databricks can connect and authenticate to ```externalucstorage``` via the connector.

![Screenshot 2023-08-11 165447](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/83323ad6-9f3f-4fd7-99b1-6876181e7861)

Finally, I created 3 ```External Locations```. One for each of my containers (```bronze```, ```silver``` and ```gold```), using their specific paths and ```external_storage_cred```.

![Screenshot 2023-08-11 171919](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/fe4ec567-0fff-44f1-a95c-fafe2b8c5b29)

Now my Databricks workspace have all the neccessary permissions to work with the containers.

### Notebooks
#### First [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/1_create_external_locations.ipynb) creates the external locations using - 
```sql
CREATE EXTERNAL LOCATION <location-name>
 URL 'abfss://<container-name>@<storage-account>.dfs.core.windows.net/<path>'
 WITH ([STORAGE] CREDENTIAL <storage-credential-name>);
```
Using ```dbutils.fs.ls``` to check I can actually access the containers.

![Screenshot 2023-08-11 180118](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/6e02e3e6-9799-41a0-9305-3fbdf440fa46)

#### Second [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/2_create_catalog_schema.ipynb) creates the Catalog (```formula1_dev```) and Schemas (```bronze```, ```silver``` and ```gold```) using -
```sql
CREATE CATALOG IF NOT EXISTS example;
```
Providing storage location path will store the *managed* tables in this schema in a location that is different than the catalog’s or metastore’s root storage location.
```sql
CREATE { DATABASE | SCHEMA } [ IF NOT EXISTS ] <schema-name>
    [ MANAGED LOCATION '<location-path>' ];
```
Schemas are named after their respective external containers in Azure.

![Screenshot 2023-08-11 184635](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/28d65e10-e5ee-459f-8ccc-46af94d853bf)

#### Third [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/3_create_bronze_tables.ipynb) creates *external* tables in bronze schema using -
```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table-name>
(
  <column-specification>
)
USING <format>
LOCATION 'abfss://<bucket-path>/<table-directory>';
```
When you run ```DROP TABLE``` on an *external* table, Unity Catalog does not delete the underlying data. Here we can see the tables type being external.

![Screenshot 2023-08-11 185001](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/b8460f56-18d0-4c88-9ff6-98dea3e39d3b)

#### Fourth [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/4_create_silver_tables.ipynb) creates *managed* tables in silver schema using the *external* tables in bronze schema -
```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table-name>
AS
SELECT
  <column>
FROM <catalog>.<schema>.<table-name>;
```
*Managed* tables in silver schema is stored in a Azure storage account. When a *managed* table is dropped, its underlying data is deleted from your cloud tenant within 30 days. 

![Screenshot 2023-08-11 194949](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/e87739dd-367d-4716-8297-ec24f17e6d6e)

#### Last [notebook]()
