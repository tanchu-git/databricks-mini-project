# Setup and exploring dataset
### Working with ```PySpark```, ingestion and data discovery is done on ```circuits```, ```races```, ```constructors```, ```drivers```, ```results```  and ```pitstops``` tables.
![overview](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/876ca38e-569c-49d8-879e-ab99a9a2a504)

This part of the project has not been setup with Unity Catalog. Nor, am I using the Auto Loader. Databricks enables users to mount cloud object storage to the Databricks File System (DBFS), even though Databricks recommends migrating away from using mounts. It stills makes for a good learning experience, together with Azure Key Vault.

## Azure Key Vault-backed scopes
To reference secrets stored in an Azure Key Vault, a secret scope is created here - ```https://<databricks-instance>#secrets/createScope```. 

![Screenshot 2023-08-14 220022](https://github.com/tanchu-git/databricks_mini_project/assets/139019601/94d47b25-9207-44cf-bfbd-e83320f34bf4)

Input necessary Azure Key Vault properties, I can then leverage all of the secrets in the corresponding Key Vault instance, using - ```dbutils.secrets.get(<scope-name>, <secret-key>)```. Now I can [mount](https://github.com/tanchu-git/databricks_mini_project/blob/main/discovery/mount_containers.ipynb) a Azure storage account using an Azure Active Directory (Azure AD) application service principal for authentication.
