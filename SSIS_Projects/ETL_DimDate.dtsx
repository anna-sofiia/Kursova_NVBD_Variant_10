SET DATEFIRST 1;

TRUNCATE TABLE DimDate;

DECLARE @StartDate DATE = '2015-01-01';
DECLARE @EndDate   DATE = '2030-12-31';

DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO DimDate (
        DateKey,
        FullDate,
        [Year],
        [Month],
        MonthName,
        [Quarter],
        DayOfMonth,
        DayOfWeek,
        DayName,
        IsWeekend
    )
    VALUES (
        CONVERT(INT, CONVERT(VARCHAR(8), @CurrentDate, 112)), -- YYYYMMDD
        @CurrentDate,
        YEAR(@CurrentDate),
        MONTH(@CurrentDate),
        DATENAME(MONTH, @CurrentDate),
        DATEPART(QUARTER, @CurrentDate),
        DAY(@CurrentDate),
        DATEPART(WEEKDAY, @CurrentDate),      -- 1 = Monday
        DATENAME(WEEKDAY, @CurrentDate),
        CASE 
            WHEN DATEPART(WEEKDAY, @CurrentDate) IN (6,7) 
            THEN 1 ELSE 0 
        END
    );

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;
