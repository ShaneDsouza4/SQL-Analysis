-- Set Up
CREATE DATABASE gms_project;

-- Combining Datasets

CREATE TABLE gms_project.data_combined AS (

	SELECT * FROM gms_project.data_10
    
    UNION ALL
    
    SELECT * FROM gms_project.data_11
    
    UNION ALL
    
    SELECT * FROM gms_project.data_12
);

-- DATA EXPLORATION
SELECT * FROM gms_project.data_combined
LIMIT 5;

-- CHECK NUMBER OF ROWS AND IF visitid is the unique and populated for all
SELECT 
	COUNT(*) AS total_rows,
	COUNT(visitid) AS non_null_rows
FROM gms_project.data_combined;

-- Check if visitid is unique with no duplicates 
SELECT 
	-- visitid,
    fullvisitorid,
    COUNT(*) as total_rows
FROM gms_project.data_combined
GROUP BY 1
HAVING COUNT(*) > 1 -- Check records that appear more then 1
LIMIT 5;

-- visitorid has duplicates, so we need to use something else as a unique identifier by combining both 
SELECT 
	CONCAT(fullvisitorid, '-', visitid) AS unique_sessions_id,
    COUNT(*) AS total_rows -- CHeck if can be the unique identifier
FROM gms_project.data_combined
GROUP BY 1
HAVING COUNT(*) > 1 
LIMIT 5;

-- Still duplicate exists
-- Visit starts 11pm and goes to 1am. That can cause a duplicate
SELECT
		CONCAT(fullvisitorid, '-', visitid) AS unique_session_id,
		FROM_UNIXTIME(date) + INTERVAL -7 HOUR AS date,
	  COUNT(*) as total_rows
FROM gms_project.data_combined
GROUP BY 1,2
HAVING unique_session_id = "0368176022600320212-1477983528"
LIMIT 5;

-- Website Engagement by Day

-- Date and Sessions table
SELECT
	date, -- We want the date
    COUNT(DISTINCT unique_session_id) AS sessions -- We want to count the session ids, and call the column sessions
FROM ( -- sub querie
	SELECT 
		DATE(FROM_UNIXTIME(date)) AS date, -- selecting date
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id -- unique identifier
	FROM gms_project.data_combined
    GROUP BY 1,2 -- group results even without aggregator. Group by for duplicates
) t1 -- name of subquery
GROUP BY 1 -- As we have the aggregate COUNT so have to do group by date
ORDER BY 1;

-- Double checking the above output csv to observe weekends
SELECT
	DAYNAME(date) AS weekday,
    COUNT(DISTINCT unique_session_id) AS sessions 
FROM ( 
	SELECT 
		DATE(FROM_UNIXTIME(date)) AS date, 
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id -- unique identifier
	FROM gms_project.data_combined
    GROUP BY 1,2 
) t1 
GROUP BY 1 
ORDER BY 2 DESC;

-- Website Engagement & Monetization by Day

SELECT 
	DAYNAME(date) AS weekday, -- select weekday
    COUNT(DISTINCT unique_session_id) AS sessions, -- count of unique session ids
    SUM(converted) AS conversions, -- Incase there was Ye or No, sum it, even if done twice, we need only 1
    ((SUM(converted)/COUNT(DISTINCT unique_session_id))*100) AS conversion_rate -- Sum thge conversion rate in %
FROM ( 
	SELECT -- date converted unique session ids columns
		DATE(FROM_UNIXTIME(date)) AS date, -- date from date column
        CASE -- Check if transaction made in each session
			WHEN transactions >= 1 THEN 1
            ELSE 0
		END AS converted,
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id -- unique identifier
	FROM gms_project.data_combined
    GROUP BY 1,2,3 
) t1 
GROUP BY 1 -- 3 aggregrate funcs so group by will be for only 1
ORDER BY 2 DESC; -- want to order by count of sessions



-- Website Engagement & Monetization by Device

SELECT 
	deviceCategory, -- selecting device category
    COUNT(DISTINCT unique_session_id) AS sessions, -- counting sessions
    ((COUNT(DISTINCT unique_session_id)/SUM(COUNT(DISTINCT unique_session_id)) OVER())*100) AS session_percentage, -- summing the 3 devices and getting percentages of each
    SUM(transactionrevenue)/1e6 AS revenue, -- 1e6 formats with 2 decimal places * 10^6
    ((SUM(transactionrevenue)/SUM(SUM(transactionrevenue)) OVER ())*100) AS revenue_percentage
FROM ( 
	SELECT -- device catrgory, transaction revenue for each session
		deviceCategory,
        transactionrevenue,
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id -- unique identifier
	FROM gms_project.data_combined
    GROUP BY 1,2,3 
) t1 
GROUP BY 1; -- 3 aggregrate funcs so group by will be for only 1


-- Website Engagement & Monetization by Region for MOBILE

SELECT 
	deviceCategory, -- selecting device category
    region,
    COUNT(DISTINCT unique_session_id) AS sessions, -- counting sessions
    ((COUNT(DISTINCT unique_session_id)/SUM(COUNT(DISTINCT unique_session_id)) OVER())*100) AS session_percentage, -- summing the 3 devices and getting percentages of each
    SUM(transactionrevenue)/1e6 AS revenue, -- 1e6 formats with 2 decimal places * 10^6
    ((SUM(transactionrevenue)/SUM(SUM(transactionrevenue)) OVER ())*100) AS revenue_percentage
FROM ( 
	SELECT -- device catrgory, region, transaction revenue for each session
		deviceCategory,
        CASE
			WHEN region = '' OR region IS NULL THEN 'NA'
            ELSE region
		END AS region,
        transactionrevenue,
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id -- unique identifier
	FROM gms_project.data_combined
    WHERE deviceCategory = "mobile"
    GROUP BY 1,2,3,4 
) t1 
GROUP BY 1, 2 -- 3 aggregrate funcs so group by will be for only 1
ORDER BY 3 DESC; -- sessions

-- Website Retention
SELECT 
	CASE
		WHEN newVisits = 1 THEN 'New Visitor' -- Call the value New Visitor
        ELSE 'Returning Visitor'
	END AS visitor_type,
    COUNT(DISTINCT fullVisitorId) AS visitors,
    ((COUNT(DISTINCT fullVisitorId)/SUM(COUNT(DISTINCT fullVisitorId)) OVER ())*100) AS visitors_percentage
FROM gms_project.data_combined
GROUP BY 1;

-- Website Acquisition by bounce rate
SELECT
	COUNT(DISTINCT unique_session_id) AS sessions,
    SUM(bounces) AS bounces,
    ((SUM(bounces)/COUNT(DISTINCT unique_session_id))*100) AS bounce_rate
FROM (
	SELECT
		bounces,
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id
	FROM gms_project.data_combined
    GROUP BY 1,2
) t1
ORDER BY 1 DESC;

-- Website Acquisition by Channel
SELECT
	channelGrouping,
	COUNT(DISTINCT unique_session_id) AS sessions,
    SUM(bounces) AS bounces,
    ((SUM(bounces)/COUNT(DISTINCT unique_session_id))*100) AS bounce_rate
FROM (
	SELECT
		channelGrouping,
		bounces,
        CONCAT(fullvisitorid, '-', visitid) AS unique_session_id
	FROM gms_project.data_combined
    GROUP BY 1,2,3
) t1
GROUP BY 1
ORDER BY 2 DESC; -- SESSIONS

-- Website Acquisition by Channel
SELECT
		channelGrouping, -- analysce by channel
		COUNT(DISTINCT unique_session_id) AS sessions,
		SUM(bounces) AS bounces,
		((SUM(bounces)/COUNT(DISTINCT unique_session_id))*100) AS bounce_rate, -- check bounce rate
		(SUM(pageviews)/COUNT(DISTINCT unique_session_id)) AS avg_pagesonsite, -- How many pages visited per session 
		(SUM(timeonsite)/COUNT(DISTINCT unique_session_id)) AS avg_timeonsite,
		SUM(CASE WHEN transactions >= 1 THEN 1 ELSE 0 END) AS conversions,
		((SUM(CASE WHEN transactions >= 1 THEN 1 ELSE 0 END)/COUNT(DISTINCT unique_session_id))*100) AS conversion_rate,
		SUM(transactionrevenue)/1e6 AS revenue
FROM (
		SELECT
			channelGrouping,
			bounces,
			pageviews,
			timeonsite,
			transactions,
			transactionrevenue,
			CONCAT(fullvisitorid, '-', visitid) AS unique_session_id
	FROM gms_project.data_combined
	GROUP BY 1,2,3,4,5,6,7
) t1
GROUP BY 1
ORDER BY 2 DESC;









