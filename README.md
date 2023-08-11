# Mini project with Azure Databricks.

The Ergast Developer API provides a historical record of motor racing data for non-commercial purposes. The API provides data for the Formula One series, from the beginning of the world championships in 1950.

I did some ingestion and data discovery work in the [demo folder](https://github.com/tanchu-git/databricks_mini_project/tree/main/demo), utilizing Unity Catalog in Azure Databricks workspace.

## [Ergast](http://ergast.com/mrd/) Formula One database
Here's a [overview](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/876ca38e-569c-49d8-879e-ab99a9a2a504) of the database, with detailed attributes of the tables [here](http://ergast.com/docs/f1db_user_guide.txt). I will be using ```drivers``` and ```results``` tables, to find the dominant drivers.

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

Finally, 3 ```External Locations``` are created. One for each of my containers (```bronze```, ```silver``` and ```gold```), using their specific paths and ```external_storage_cred```.

![Screenshot 2023-08-11 171919](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/fe4ec567-0fff-44f1-a95c-fafe2b8c5b29)

Now my Databricks workspace have all the neccessary permissions to work with the containers.

### Notebooks
#### First [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/1_create_external_locations.ipynb) creates the external locations for ```bronze```, ```silver``` and ```gold``` - 
```sql
CREATE EXTERNAL LOCATION external_storage_bronze
 URL 'abfss://bronze@externalucstorage.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL external_storage_cred);
```
```sql
CREATE EXTERNAL LOCATION external_storage_silver
 URL 'abfss://silver@externalucstorage.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL external_storage_cred);
```
```sql
CREATE EXTERNAL LOCATION external_storage_gold
 URL 'abfss://gold@externalucstorage.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL external_storage_cred);
```
Using ```dbutils.fs.ls``` to check I can actually access the containers.

![Screenshot 2023-08-11 180118](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/6e02e3e6-9799-41a0-9305-3fbdf440fa46)

#### Second [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/2_create_catalog_schema.ipynb) creates the Catalog (```formula1_dev```) and Schemas (```bronze```, ```silver``` and ```gold```) -
```sql
CREATE CATALOG IF NOT EXISTS formula1_dev;
```
Providing storage location path will store the *managed* tables in this schema in a location that is different than the catalog’s or metastore’s root storage location. *Managed* tables always use the Delta table format.
```sql
CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://bronze@externalucstorage.dfs.core.windows.net/"
```
```sql
CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@externalucstorage.dfs.core.windows.net/"
```
```sql
CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@externalucstorage.dfs.core.windows.net/"
```
Schemas are named after their respective external containers in Azure.

![Screenshot 2023-08-11 184635](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/28d65e10-e5ee-459f-8ccc-46af94d853bf)

#### Third [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/3_create_bronze_tables.ipynb) creates *external* table ```drivers``` and ```results``` in bronze schema -
```sql
DROP TABLE IF EXISTS formula1_dev.bronze.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://bronze@externalucstorage.dfs.core.windows.net/drivers.json");
```
```sql
DROP TABLE IF EXISTS formula1_dev.bronze.results;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results
(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS (path "abfss://bronze@externalucstorage.dfs.core.windows.net/results.json");
```
When you run ```DROP TABLE``` on an *external* table, Unity Catalog does not delete the underlying data. Here we can see the tables type being external.

![Screenshot 2023-08-11 185001](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/b8460f56-18d0-4c88-9ff6-98dea3e39d3b)

#### Fourth [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/4_create_silver_tables.ipynb) creates *managed* tables in silver schema using the *external* tables in bronze schema with some light transformation -
```sql
DROP TABLE IF EXISTS formula1_dev.silver.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.drivers
AS
SELECT 
  driverId AS driver_id,
  driverRef AS driver_ref,
  number,
  code,
  concat(name.forename, " ", name.surname) AS name,
  dob,
  nationality,
  current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.drivers;
```
```sql
DROP TABLE IF EXISTS formula1_dev.silver.results;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.results
AS
SELECT 
  resultId AS result_id,
  raceId AS race_id,
  driverId AS driver_id,
  constructorId AS constructor_id,
  number,
  grid,
  position,
  positionText AS position_text,
  positionOrder AS position_order,
  points,
  laps,
  time,
  milliseconds,
  fastestLap,
  rank,
  fastestLapTime AS fastest_lap_time,
  fastestLapSpeed AS fastest_lap_speed,
  statusId AS status_id,
  current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.results;
```
*Managed* tables in silver schema is stored in a Azure storage account. When a *managed* table is dropped, its underlying data is deleted from your cloud tenant within 30 days. 

![Screenshot 2023-08-11 194949](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/e87739dd-367d-4716-8297-ec24f17e6d6e)

#### Last [notebook](https://github.com/tanchu-git/databricks_mini_project/blob/main/notebooks/5_create_gold_tables.ipynb) creates *managed* table ```driver_wins``` in gold schema with a simple ```JOIN``` clause -
```sql
DROP TABLE IF EXISTS formula1_dev.gold.driver_wins;

CREATE TABLE IF NOT EXISTS formula1_dev.gold.driver_wins
AS
SELECT d.name, count(1) as number_of_wins
  FROM formula1_dev.silver.drivers d
  JOIN formula1_dev.silver.results r
    on (d.driver_id = r.driver_id)
  WHERE r.position = 1
GROUP BY d.name;
```
![Screenshot 2023-08-11 210951](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/4d492de1-1c60-4d06-8cf6-f9a8162d0d5c)

### Unity Catalog
Unity Catalog automatically captures user-level audit logs that record access to your data. Unity Catalog also captures lineage data, which we can see here - 
![Screenshot 2023-08-11 213650](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/11f11ee8-8d86-416e-ae3e-5ada709de81c)
