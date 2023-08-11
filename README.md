# Mini project with Databricks and Azure.
The Ergast Developer API provides a historical record of motor racing data for non-commercial purposes. The API provides data for the Formula One series, from the beginning of the world championships in 1950.

I will be messing around with a few dataset from them. 

## [Ergast](http://ergast.com/mrd/) Formula One database
Here's a [overview](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/876ca38e-569c-49d8-879e-ab99a9a2a504) of the database, with detailed attributes of the tables [here](http://ergast.com/docs/f1db_user_guide.txt). I will be using ```drivers``` and ```results``` tables, downloaded as ```JSON``` files and stored in Azure Data Lake Storage Gen2.

## Architecture overview
![Screenshot 2023-08-11 160806](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/70c2587a-d123-4090-87a0-a2dc905ecf46)

## Azure storage account, containers and access conecctor
I will keep the dataset separate from Databricks, using storage account ```externalucstorage``` as external storage location, with 3 containers (```bronze```, ```silver``` and ```gold```).

![Screenshot 2023-08-11 161039](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/629d5b9c-aec5-4f46-af69-7266542f1c76)

### Access Connector For Azure Databricks
Databricks Unity Catalog can be configured to use an Azure managed identity to access storage containers in Azure. After creating a ```Access Connector For Azure Databricks``` in Azure portal, I assigned the role ```Storage Blob Data Contributor``` to the connector within ```externalucstorage``` storage account.

Then a storage credential ```external_storage_cred```, using the ```Access connector ID``` is created in Databricks workspace. Now Databricks can connect and authenticate to ```externalucstorage``` via the connector.

![Screenshot 2023-08-11 165447](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/83323ad6-9f3f-4fd7-99b1-6876181e7861)

Finally, I created 3 ```External Locations```. One for each of my containers (```bronze```, ```silver``` and ```gold```), using their specific paths and ```external_storage_cred```.

![Screenshot 2023-08-11 171919](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/fe4ec567-0fff-44f1-a95c-fafe2b8c5b29)

Now my Databricks workspace have all the neccessary permissions to work with the containers.

### Notebooks
