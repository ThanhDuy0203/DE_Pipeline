SELECT PAYMENTMETHODID
      ,PAYMENTMETHODNAME
      ,CAST (VALIDFROM AS VARCHAR(30)) AS VALIDFROM
      ,CAST (VALIDTO AS VARCHAR(30)) AS VALIDTO
  FROM Application.PaymentMethods;