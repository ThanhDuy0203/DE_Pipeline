SELECT [PersonID]
      ,[FullName]
      ,[PreferredName]
      ,[IsEmployee]
      ,[IsSalesperson]
      ,[Photo]
      ,CAST (VALIDFROM AS VARCHAR(30)) AS VALIDFROM
      ,CAST (VALIDTO AS VARCHAR(30)) AS VALIDTO
  FROM [WideWorldImporters].[Application].[People]
