CREATE DATABASE CableTV_DW;
GO

USE CableTV_DW;
GO
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,          -- yyyymmdd
    FullDate DATE NOT NULL,
    [Year] INT NOT NULL,
    [Month] INT NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    [Quarter] INT NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,            -- 1 = Monday
    DayName NVARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL
);
CREATE TABLE DimSubscribers (
    SubscriberKey INT IDENTITY(1,1) PRIMARY KEY,
    SubscriberID INT NOT NULL,            -- business key з OLTP
    FullName NVARCHAR(200),
    Email NVARCHAR(100),
    Address NVARCHAR(255),
    RegistrationDate DATE,
    RegistrationDateKey INT,              -- FK > DimDate
    IsActive BIT
);
CREATE TABLE DimChannelGroup (
    GroupKey INT IDENTITY(1,1) PRIMARY KEY,
    GroupID INT NOT NULL,                 -- business key
    GroupName NVARCHAR(50),
    MonthlyPrice DECIMAL(10,2),
    ParentGroupID INT NULL
);
CREATE TABLE DimChannel (
    ChannelKey INT IDENTITY(1,1) PRIMARY KEY,
    ChannelID INT NOT NULL,
    ChannelName NVARCHAR(100),
    Genre NVARCHAR(50),
    GroupID INT                            -- з OLTP
);
CREATE TABLE DimMovie (
    MovieKey INT IDENTITY(1,1) PRIMARY KEY,
    MovieID INT NOT NULL,
    Title NVARCHAR(255),
    Genre NVARCHAR(50),
    ReleaseYear INT
);

ALTER TABLE DimMovie
ADD RentalPrice DECIMAL(10,2) NULL;

CREATE TABLE DimPaymentMethod (
    PaymentMethodKey INT IDENTITY(1,1) PRIMARY KEY,
    MethodName NVARCHAR(50) UNIQUE
);
CREATE TABLE FactOrders (
    OrderID INT,
    DateKey INT,
    SubscriberID INT,
    MovieID INT,
    Amount DECIMAL(10,2)
);
CREATE TABLE FactSubscriptions (
    SubscriptionID INT,
    SubscriberID INT,
    GroupID INT,
    StartDateKey INT,
    EndDateKey INT NULL,
    IsActive BIT
);
CREATE TABLE FactInvoices (
    InvoiceID INT,
    DateKey INT,
    SubscriberID INT,
    TotalAmount DECIMAL(10,2),
    IsPaid BIT
);
CREATE TABLE FactPayments (
    PaymentID INT,
    DateKey INT,
    SubscriberID INT,
    InvoiceID INT,
    PaymentMethodKey INT,
    AmountPaid DECIMAL(10,2)
);
