# Center of Governance System -> cog_system
Connect and append system tables from different cloud accounts.
System Tables Requirements

1.	Create a Service Principal on each account with account admin role privileges and access to all the catalogs/metastores inside that account.
![](README resources/account admin SP.png)
- i.	(remember if we want the information to be considered we need to ensure each catalog/metastore shares the system information with the objective catalog)
1.	For this picture example, we would need to run the system_account_provider.app in each catalog a total of 5 times because we have 5 catalogs:
![](README resources/catalog metastore.png)
2.	If there is and admin group already associated to each workspace, we can add our “admin account Service Principal” to that admin group:
![](README resources/admin group permission.png)
3.	If step No. 2 was successfully done jump to step 5
4.	The created admin Service Principal needs to be added to the workspace as well as admin:
a.	 ![](README resources/wp admin for SP.png)
5.	In the account console at the metastore (catalog) level select the “Allow Delta Sharing with parties outside your organization” checkbox for every account required in this solution.
a.	Unselect expiration “Delta Sharing token lifetime”
b.	Give an organization name \
i. ![](README resources/enable delta sharing.png) \
ii.	Copy sharing identifier from the workspace in delta sharing UI\
  ![](README resources/sharing id.png)
6.	Create new recipients in sharing accounts
a.	 ![](README resources/new recipient.png)
7.	Generate coe_account_name_system_tables because system tables cannot be directly shared.
a.	Run coe_account_name_system_tables script
8.	Create share grants
a.	Add assets
i.	Grant permissions and select each schema to share.
ii.	Missing permissions
1.	To enable sharing this schema, you need the "SELECT,” "EXECUTE,” and "READ VOLUME" permissions; by clicking "Grant permission and selecting" below, "USER@databricks.com will be granted these permissions on schema "SYSTEM_TABLE. “
a.	 ![](README resources/share schema compute.png)\
iii.	Add recipients to share these assets to:\
![](README resources/add recipients.png)\
b.	To view and use the data, create a catalog from a share. The data will appear under your account’s shared catalogs.\
 ![](README resources/create catalog from share.png)
9.	Run the Center of Excellence System Tables source of truth script, which will merge and append new records only into our CoE system tables.


