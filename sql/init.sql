-- Idempotent database bootstrap/migration. Run with sqlcmd (which honors GO).
IF DB_ID(N'FraudDB') IS NULL
BEGIN
    CREATE DATABASE FraudDB;
END;
GO

USE FraudDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'FraudDB' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO

IF OBJECT_ID(N'dbo.Transactions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Transactions
    (
        TransactionId BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_Transactions PRIMARY KEY,
        SourceRowNumber BIGINT NOT NULL,
        Step INT NOT NULL,
        TransactionType VARCHAR(20) NOT NULL,
        Amount DECIMAL(18,2) NOT NULL,
        NameOrig VARCHAR(20) NOT NULL,
        OldBalanceOrg DECIMAL(18,2) NOT NULL,
        NewBalanceOrig DECIMAL(18,2) NOT NULL,
        NameDest VARCHAR(20) NOT NULL,
        OldBalanceDest DECIMAL(18,2) NOT NULL,
        NewBalanceDest DECIMAL(18,2) NOT NULL,
        IsFraud BIT NOT NULL,
        IsFlaggedFraud BIT NOT NULL,
        CreatedAt DATETIME2(3) NOT NULL
            CONSTRAINT DF_Transactions_CreatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_Transactions_SourceRowNumber UNIQUE (SourceRowNumber)
    );
END;
GO

IF COL_LENGTH(N'dbo.Transactions', N'SourceRowNumber') IS NULL
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM cdc.change_tables
        WHERE source_object_id = OBJECT_ID(N'dbo.Transactions')
    )
    BEGIN
        EXEC sys.sp_cdc_disable_table
            @source_schema = N'dbo',
            @source_name = N'Transactions',
            @capture_instance = N'all';
    END;

    ALTER TABLE dbo.Transactions ADD SourceRowNumber BIGINT NULL;
    UPDATE dbo.Transactions SET SourceRowNumber = TransactionId;
    ALTER TABLE dbo.Transactions ALTER COLUMN SourceRowNumber BIGINT NOT NULL;
    ALTER TABLE dbo.Transactions
        ADD CONSTRAINT UQ_Transactions_SourceRowNumber UNIQUE (SourceRowNumber);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(N'dbo.Transactions')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Transactions',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO
