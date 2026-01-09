CREATE DATABASE CableTV_OLTP;
GO

USE CableTV_OLTP;
GO

CREATE TABLE ChannelGroups (
    GroupID INT IDENTITY(1,1) PRIMARY KEY,
    GroupName NVARCHAR(50) NOT NULL UNIQUE,
    MonthlyPrice DECIMAL(10,2) NOT NULL
        CHECK (MonthlyPrice >= 0),

    ParentGroupID INT NULL,

    CONSTRAINT FK_ChannelGroups_Parent
        FOREIGN KEY (ParentGroupID)
        REFERENCES ChannelGroups(GroupID)
);

CREATE TABLE Channels (
    ChannelID INT IDENTITY(1,1) PRIMARY KEY,
    ChannelName NVARCHAR(100) NOT NULL,
    GroupID INT NOT NULL,

    CONSTRAINT FK_Channels_ChannelGroups
        FOREIGN KEY (GroupID)
        REFERENCES ChannelGroups(GroupID)
);
ALTER TABLE Channels
ADD Genre NVARCHAR(50);


CREATE TABLE Subscribers (
    SubscriberID INT IDENTITY(1,1) PRIMARY KEY,
    FullName NVARCHAR(200) NOT NULL,
    Email NVARCHAR(100) NOT NULL UNIQUE,
    Address NVARCHAR(255),
    RegistrationDate DATE NOT NULL DEFAULT GETDATE(),
    IsActive BIT NOT NULL DEFAULT 1
);

CREATE TABLE Subscriptions (
    SubscriptionID INT IDENTITY(1,1) PRIMARY KEY,
    SubscriberID INT NOT NULL,
    GroupID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    IsActive BIT NOT NULL DEFAULT 1,

    CONSTRAINT FK_Subscriptions_Subscriber
        FOREIGN KEY (SubscriberID)
        REFERENCES Subscribers(SubscriberID),

    CONSTRAINT FK_Subscriptions_ChannelGroup
        FOREIGN KEY (GroupID)
        REFERENCES ChannelGroups(GroupID),

    CONSTRAINT CK_Subscriptions_Dates
        CHECK (EndDate IS NULL OR EndDate >= StartDate)
);

CREATE TABLE Movies (
    MovieID INT IDENTITY(1,1) PRIMARY KEY,
    Title NVARCHAR(255) NOT NULL,
    Genre NVARCHAR(50),
    ReleaseYear INT
        CHECK (ReleaseYear BETWEEN 1900 AND YEAR(GETDATE())),
    RentalPrice DECIMAL(10,2) NOT NULL
        CHECK (RentalPrice > 0)
);

CREATE TABLE Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    SubscriberID INT NOT NULL,
    MovieID INT NOT NULL,
    OrderDate DATETIME NOT NULL DEFAULT GETDATE(),
    Amount DECIMAL(10,2) NOT NULL
        CHECK (Amount > 0),

    CONSTRAINT FK_Orders_Subscriber
        FOREIGN KEY (SubscriberID)
        REFERENCES Subscribers(SubscriberID),

    CONSTRAINT FK_Orders_Movie
        FOREIGN KEY (MovieID)
        REFERENCES Movies(MovieID)
);


CREATE TABLE Invoices (
    InvoiceID INT IDENTITY(1,1) PRIMARY KEY,
    SubscriberID INT NOT NULL,
    InvoiceDate DATE NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL
        CHECK (TotalAmount >= 0),
    DueDate DATE NOT NULL,
    IsPaid BIT NOT NULL DEFAULT 0,

    CONSTRAINT FK_Invoices_Subscriber
        FOREIGN KEY (SubscriberID)
        REFERENCES Subscribers(SubscriberID)
);

CREATE TABLE Payments (
    PaymentID INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceID INT NOT NULL,
    PaymentDate DATETIME NOT NULL DEFAULT GETDATE(),
    AmountPaid DECIMAL(10,2) NOT NULL
        CHECK (AmountPaid > 0),
    PaymentMethod NVARCHAR(50) NOT NULL,

    CONSTRAINT FK_Payments_Invoice
        FOREIGN KEY (InvoiceID)
        REFERENCES Invoices(InvoiceID)
);

CREATE INDEX IX_Subscriptions_Active
    ON Subscriptions (SubscriberID, IsActive);

CREATE INDEX IX_Orders_OrderDate
    ON Orders (OrderDate);

CREATE INDEX IX_Invoices_InvoiceDate
    ON Invoices (InvoiceDate);

SELECT name
FROM sys.tables;

SELECT COUNT(*) FROM Subscribers;
SELECT TOP 20 *
FROM Subscribers;
SELECT * FROM ChannelGroups;

SELECT COUNT(*) AS TotalChannels
FROM Channels;

SELECT COUNT(*) FROM Movies;
SELECT TOP 20
    Title,
    Genre,
    ReleaseYear,
    RentalPrice
FROM Movies;

SELECT COUNT(*) FROM Orders;

SELECT TOP 20
    OrderID,
    SubscriberID,
    MovieID,
    OrderDate,
    Amount
FROM Orders;

SELECT COUNT(*) FROM Subscriptions;

SELECT 
    COUNT(s.SubscriberID) AS WithSubscription,
    COUNT(sub.SubscriberID) AS TotalSubscribers
FROM Subscribers sub
LEFT JOIN Subscriptions s
    ON sub.SubscriberID = s.SubscriberID;

SELECT g.GroupName, COUNT(*) AS Cnt
FROM Subscriptions s
JOIN ChannelGroups g ON s.GroupID = g.GroupID
GROUP BY g.GroupName;

TRUNCATE TABLE Invoices;

WITH Months AS (
    SELECT 
        DATEADD(
            MONTH,
            v.number,
            DATEADD(
                MONTH,
                DATEDIFF(MONTH, 0, GETDATE()) - 59,
                0
            )
        ) AS InvoiceMonth
    FROM master.dbo.spt_values v
    WHERE v.type = 'P'
      AND v.number BETWEEN 0 AND 59
),
Subs AS (
    SELECT
        s.SubscriberID,
        CAST(s.StartDate AS DATE) AS StartDate,
        g.MonthlyPrice,
        CASE 
            WHEN ABS(CHECKSUM(NEWID())) % 100 < 30 THEN 60
            WHEN ABS(CHECKSUM(NEWID())) % 100 < 60 THEN 36
            WHEN ABS(CHECKSUM(NEWID())) % 100 < 80 THEN 24
            ELSE 18
        END AS ActiveMonths
    FROM Subscriptions s
    JOIN ChannelGroups g ON s.GroupID = g.GroupID
    WHERE s.IsActive = 1
),
OrdersMonthly AS (
    SELECT
        SubscriberID,
        DATEADD(MONTH, DATEDIFF(MONTH, 0, OrderDate), 0) AS OrderMonth,
        SUM(Amount) AS MovieSum
    FROM Orders
    GROUP BY
        SubscriberID,
        DATEADD(MONTH, DATEDIFF(MONTH, 0, OrderDate), 0)
)
INSERT INTO Invoices (
    SubscriberID,
    InvoiceDate,
    TotalAmount,
    IsPaid,
    DueDate
)
SELECT
    s.SubscriberID,
    m.InvoiceMonth,
    s.MonthlyPrice + ISNULL(o.MovieSum, 0),
    0,
    DATEADD(MONTH, 1, m.InvoiceMonth)
FROM Subs s
JOIN Months m
    ON m.InvoiceMonth >= DATEADD(MONTH, DATEDIFF(MONTH, 0, s.StartDate), 0)
   AND DATEDIFF(MONTH, s.StartDate, m.InvoiceMonth) < s.ActiveMonths
LEFT JOIN OrdersMonthly o
    ON o.SubscriberID = s.SubscriberID
   AND o.OrderMonth = m.InvoiceMonth;

SELECT COUNT(*) FROM Invoices;

SELECT 
    YEAR(InvoiceDate) AS Year,
    COUNT(*) AS Cnt
FROM Invoices
GROUP BY YEAR(InvoiceDate)
ORDER BY Year;


INSERT INTO Payments (
    InvoiceID,
    PaymentDate,
    AmountPaid,
    PaymentMethod
)
SELECT
    i.InvoiceID,
    DATEADD(
        DAY,
        ABS(CHECKSUM(NEWID())) % 25,
        i.InvoiceDate
    ) AS PaymentDate,
    i.TotalAmount AS AmountPaid,
    CASE ABS(CHECKSUM(NEWID())) % 3
        WHEN 0 THEN 'Card'
        WHEN 1 THEN 'Cash'
        ELSE 'Online'
    END AS PaymentMethod
FROM Invoices i
WHERE ABS(CHECKSUM(NEWID())) % 100 < 70;

UPDATE i
SET IsPaid = 1
FROM Invoices i
JOIN Payments p ON i.InvoiceID = p.InvoiceID;

SELECT COUNT(*) FROM Payments;

SELECT 
    IsPaid,
    COUNT(*) AS Cnt
FROM Invoices
GROUP BY IsPaid;

UPDATE Invoices SET IsPaid = 0;
INSERT INTO Payments (
    InvoiceID,
    PaymentDate,
    AmountPaid,
    PaymentMethod
)
SELECT TOP (70) PERCENT
    i.InvoiceID,
    DATEADD(
        DAY,
        ABS(CHECKSUM(NEWID())) % 25,
        i.InvoiceDate
    ),
    i.TotalAmount,
    CASE ABS(CHECKSUM(NEWID())) % 3
        WHEN 0 THEN 'Card'
        WHEN 1 THEN 'Cash'
        ELSE 'Online'
    END
FROM Invoices i
ORDER BY NEWID();

UPDATE i
SET IsPaid = 1
FROM Invoices i
JOIN Payments p ON i.InvoiceID = p.InvoiceID;


SELECT 
    IsPaid,
    COUNT(*) AS Cnt
FROM Invoices
GROUP BY IsPaid;

SELECT COUNT(*) AS TotalSubscribers
FROM Subscriber;

SELECT COUNT(*) AS TotalSubscribers
FROM Subscribers;

SELECT COUNT(*) AS TotalOrders
FROM [Orders];

SELECT 
    MIN(OrderDate) AS FirstOrderDate,
    MAX(OrderDate) AS LastOrderDate
FROM [Orders];

SELECT 
    COUNT(*) AS TotalSubscriptions,
    SUM(CASE WHEN EndDate IS NULL THEN 1 ELSE 0 END) AS ActiveSubscriptions
FROM Subscriptions;








