/* 
============================================================
CREATE DATABASE AND SCHEMAS

Script Purpose : 
	Script ini untuk membuat database baru yang dinamakan 'DataWarehouse' setelah 
	mengecek terlebih dahulu apakah database itu sudah ada di dalam master atau belum. 
	Kemudian menambahkan schema bronze, silver, dan gold. 

============================================================
*/

USE master;
GO

-- Drop or Recreate 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO 

USE DataWarehouse;
GO 

-- Create Schemas Bronze, Silver, Gold
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
