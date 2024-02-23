SELECT SUPPLIERID
      ,SUPPLIERNAME
      ,SUPPLIERCATEGORYID
      ,PRIMARYCONTACTPERSONID
      ,SUPPLIERREFERENCE
      ,PAYMENTDAYS
      ,DELIVERYPOSTALCODE
      ,CAST (VALIDFROM AS VARCHAR(30)) AS VALIDFROM
      ,CAST (VALIDTO AS VARCHAR(30)) AS VALIDTO
  FROM Purchasing.Suppliers;