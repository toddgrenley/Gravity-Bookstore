
-- GRAVITY BOOKS
-- Here we are constructing an advanced query, step by step becoming more complex, in order to manage inventory and orders for a fictional book store.

-- Part I. Construct the Query
-- 1. We'll start with simple order quantities.

SELECT CAST(order_date AS date) AS order_day, COUNT(*) AS num_orders
FROM cust_order
GROUP BY CAST(order_date AS date)
ORDER BY order_day

-- 2. Then add other order information.

SELECT *
FROM cust_order AS co
INNER JOIN order_line AS ol
	ON co.order_id = ol.order_id
ORDER BY order_date

-- 3. Here we'll start aggregating the orders by date, totaling up quantities as well as making additional calculations for analysis.

SELECT CAST(order_date AS date) AS order_day, 
	FORMAT(order_date, 'yyyy-MM') AS order_month, 
	COUNT(DISTINCT co.order_id) AS num_orders, 
	COUNT(ol.book_id) AS num_books, 
	SUM(ol.price) AS total_price,
	SUM(COUNT(ol.book_id)) OVER (PARTITION BY FORMAT(order_date, 'yyyy-MM') ORDER BY CAST(order_date AS date)) AS rolling_num_books,
	SUM(SUM(ol.price)) OVER (PARTITION BY FORMAT(order_date, 'yyyy-MM') ORDER BY CAST(order_date AS date)) AS rolling_total_price
FROM cust_order AS co
INNER JOIN order_line AS ol
	ON co.order_id = ol.order_id
GROUP BY CAST(order_date AS date), FORMAT(order_date, 'yyyy-MM')
ORDER BY order_day

-- 4. We'll add another column to get the previous year's books as well as insert a subquery into the FROM statement to sort of "pre-format" our dates. This will 
-- increase efficiency by only needing to format once and it will make selecting and ordering easier. The query is longer, but performs better.

SELECT order_day, 
	order_month, 
	COUNT(DISTINCT order_id) AS num_orders, 
	COUNT(book_id) AS num_books, 
	SUM(price) AS total_price,
	SUM(COUNT(book_id)) OVER (PARTITION BY order_month ORDER BY order_day) AS rolling_num_books,
	SUM(SUM(price)) OVER (PARTITION BY order_month ORDER BY order_day) AS rolling_total_price,
	LAG(COUNT(book_id), 7) OVER (ORDER BY order_day) AS prev_books
FROM (
	SELECT CAST(order_date AS date) AS order_day, 
		FORMAT(order_date, 'yyyy-MM') AS order_month,
		co.order_id,
		ol.book_id,
		ol.price
	FROM cust_order AS co
	INNER JOIN order_line AS ol
		ON co.order_id = ol.order_id
		) sub
GROUP BY order_day, order_month
ORDER BY order_day

-- Small queries just for reference to see the original tables.

SELECT *
FROM cust_order

SELECT *
FROM order_line


-- Part II. Create Calendar Table
-- At this point we have a working query, but there is the one potential issue of missing dates in the data. This could throw off our analysis, so to remedy it we need
-- to create a Calendar Table that will serve as a basis for the other tables. This way we can ensure there is a row for every single day, even if there were no orders.

-- 1. We'll create the table structure first.

IF EXISTS (SELECT * FROM information_schema.tables WHERE Table_Name = 'Calendar' AND Table_Type = 'BASE TABLE')
BEGIN
DROP TABLE [Calendar]
END

CREATE TABLE [Calendar]
(
	[CalendarDate] DATETIME,
	[CalendarDay] varchar(10),
	[CalendarMonth] varchar(10),
	[CalendarQuarter] varchar(10),
	[CalendarYear] varchar(10),
	[DayOfWeekNum] varchar(10),
	[DayOfWeekName] varchar(10),
	[DateNum] varchar(10),
	[QuarterCD] varchar(10),
	[MonthNameCD] varchar(10),
	[FullMonthName] varchar(10),
	[HolidayName] varchar(50),
	[HolidayFlag] varchar(10)
)
GO

-- 2. Do some formatting.

DECLARE @StartDate DATE
DECLARE @EndDate DATE
SET @StartDate = '2020-01-01'
SET @EndDate = GETDATE()

-- 3. And then insert the date records into it.

WHILE @StartDate <= @EndDate
	BEGIN
		INSERT INTO [Calendar]
			(
				CalendarDate,
				CalendarDay,
				CalendarMonth,
				CalendarQuarter,
				CalendarYear,
				DayOfWeekNum,
				DayOfWeekName,
				DateNum,
				QuarterCD,
				MonthNameCD,
				FullMonthName,
				HolidayName,
				HolidayFlag
			)
            SELECT @StartDate,
				   DAY(@StartDate),
				   MONTH(@StartDate),
				   DATEPART(QUARTER, (@StartDate)),
				   YEAR(@StartDate),
				   DATEPART(WEEKDAY, (@StartDate)),
   				   DATENAME(WEEKDAY, (@StartDate)),
				   CONVERT(VARCHAR(10), @StartDate, 112),
				   CONVERT(VARCHAR(10), YEAR(@StartDate)) + 'Q' + CONVERT(VARCHAR(10), DATEPART(QUARTER, (@StartDate))),
   				   LEFT(DATENAME(MONTH, (@StartDate)), 3),
				   DATENAME(MONTH, (@StartDate)),
				   NULL,
				   'N'
            SET @StartDate = DATEADD(dd, 1, @StartDate)
	END


-- Test it out.

SELECT *
FROM Calendar


-- Part III. Update Original Query
-- Now we just need to go back and change our original query to use the newly created calendar table.

-- 1. Change references to calendar table.

SELECT C.CalendarDate, 
	C.CalendarYear,
	C.CalendarMonth,
	C.DayOfWeekName,
	COUNT(DISTINCT order_id) AS num_orders, 
	COUNT(book_id) AS num_books, 
	SUM(price) AS total_price,
	SUM(COUNT(book_id)) OVER (PARTITION BY C.CalendarYear, C.CalendarMonth ORDER BY C.CalendarDate) AS rolling_num_books,
	SUM(SUM(price)) OVER (PARTITION BY C.CalendarYear, C.CalendarMonth ORDER BY C.CalendarDate) AS rolling_total_price,
	LAG(COUNT(book_id), 7) OVER (ORDER BY C.CalendarDate) AS prev_books
FROM Calendar AS C
LEFT JOIN (
	SELECT CAST(order_date AS date) AS order_day, 
		FORMAT(order_date, 'yyyy-MM') AS order_month,
		co.order_id,
		ol.book_id,
		ol.price
	FROM cust_order AS co
	INNER JOIN order_line AS ol
		ON co.order_id = ol.order_id
		) sub ON C.CalendarDate = sub.order_day
GROUP BY C.CalendarDate, C.CalendarYear, C.CalendarMonth, C.DayOfWeekName
ORDER BY C.CalendarDate

-- 2. Now that we have the calendar table, we do not need the date formatting we had inserted earlier. This can be removed, which will not only streamline the query, 
-- but will improve performance to boot. We'll also change the JOIN to a LEFT JOIN on the calendar table so that all rows including empty ones will show.

SELECT C.CalendarDate, 
	C.CalendarYear,
	C.CalendarMonth,
	C.DayOfWeekName,
	COUNT(DISTINCT co.order_id) AS num_orders, 
	COUNT(book_id) AS num_books, 
	SUM(price) AS total_price,
	SUM(COUNT(book_id)) OVER (PARTITION BY C.CalendarYear, C.CalendarMonth ORDER BY C.CalendarDate) AS rolling_num_books,
	SUM(SUM(price)) OVER (PARTITION BY C.CalendarYear, C.CalendarMonth ORDER BY C.CalendarDate) AS rolling_total_price,
	LAG(COUNT(book_id), 7) OVER (ORDER BY C.CalendarDate) AS prev_books
FROM Calendar AS C
LEFT JOIN cust_order AS co
	ON C.CalendarDate = CAST(co.order_date AS date)
LEFT JOIN order_line AS ol
	ON co.order_id = ol.order_id
GROUP BY C.CalendarDate, C.CalendarYear, C.CalendarMonth, C.DayOfWeekName
ORDER BY C.CalendarDate

-- FINISHED. There we have it: a high-performing, finished query that satisfies all our needs.



-- VISUALIZATION TIME
-- The following queries were created to produce tables that would be used for visualizations in Tableau.

-- All Book/Author Info

SELECT *
FROM author AS a
JOIN book_author AS ba
	ON a.author_id = ba.author_id
JOIN book AS b
	ON ba.book_id = b.book_id
ORDER BY title

SELECT *
FROM author

SELECT *
FROM book

SELECT *
FROM book_author


-- All Book/Author/Order Info (Centered around Books so they appear even if not ordered and no authors listed)
-- Row for every author, book, order combination (author will appear every time one of their books is ordered)

SELECT a.author_id, author_name, b.book_id, title, publication_date, ol.order_id, price, order_date, dest_address_id
FROM author AS a
JOIN book_author AS ba
	ON a.author_id = ba.author_id
RIGHT JOIN book AS b
	ON ba.book_id = b.book_id
LEFT JOIN order_line AS ol
	ON b.book_id = ol.book_id
LEFT JOIN cust_order AS co
	 ON ol.order_id = co.order_id
ORDER BY author_name

-- Book/Order Info (Books appear even if not ordered)

SELECT *
FROM book AS b
LEFT JOIN order_line AS ol
	ON b.book_id = ol.book_id
LEFT JOIN cust_order AS co
	 ON ol.order_id = co.order_id


-- All Order Info

SELECT *
FROM cust_order

SELECT *
FROM order_line

SELECT *
FROM cust_order AS co
JOIN order_line AS ol
	ON co.order_id = ol.order_id
ORDER BY order_date


-- All Order/Address Info

SELECT *
FROM address

SELECT *
FROM country

SELECT city, COUNT(order_id) AS Count
FROM (
SELECT ol.order_id, book_id, price, order_date, street_number, street_name, city, country_name
FROM order_line AS ol
JOIN cust_order AS co
	ON ol.order_id = co.order_id
JOIN address AS a
	ON co.dest_address_id = a.address_id
JOIN country AS c
	ON a.country_id = c.country_id
WHERE country_name = 'United States of America') s1
GROUP BY city
ORDER BY Count DESC