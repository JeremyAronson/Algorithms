msck repair table tblevents;

--DROP TABLE IF EXISTS tblevent_clickedExperience;
--CREATE TABLE tblevent_clickedExperience(
--    experience_name STRING,
--    experience_sku STRING,
--    webID STRING
--    clickValue FLOAT,
--    destination STRING,
--    list_grid_variation STRING,
--    reviews STRING,
--    rating STRING,
--    price STRING,
--    search_position INT,
--    is_in_shelf STRING,
--    shelf_name STRING,
--    shelf_position INT,
--    session_id STRING,
--    page_load_id STRING,
--    PageOfClick STRING,
--    PagePreClick STRING
--)
--PARTITIONED BY (pp_date STRING)
--;

--set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE tblevent_clickedExperience PARTITION (pp_date)
SELECT 
    a.experience_name,
    a.experience_sku,
    SPLIT(
        REGEXP_REPLACE(
            SPLIT(a.mp_builtin_current_url,'-')[0],
            'https://moments.marriott.com/destination/','')
        ,'/')[0] AS webID,
    CASE WHEN DATEDIFF(CURRENT_DATE, a.pp_date) <= 14 THEN 1 
        ELSE 1/(DATEDIFF(CURRENT_DATE, a.pp_date) - 14) END clickValue,
    REGEXP_REPLACE(UPPER(SPLIT(a.destination, '[\,]')[0])," ","_") destination, --captializes and connects all words with underscore appearing pre comma
    a.list_grid_variation,
    a.reviews,
    a.rating,
    a.price,
    a.search_position,
    a.is_in_shelf,
    a.shelf_name,
    a.shelf_position,
    a.session_id,
    a.page_load_id,
    a.mp_builtin_current_url PageOfClick,
    a.mp_builtin_referrer PagePreClick,
    a.pp_date
FROM tblevents a
WHERE a.event = 'experience clicked'
    AND a.pp_date >= DATE_SUB(CURRENT_DATE, 5)
;   

--DROP TABLE IF EXISTS tblevent_viewedPage;
--CREATE TABLE tblevent_viewedPage(
--    page_type STRING,
--    City STRING,
--    Country STRING,
--    current_URL STRING,
--    page_load_id STRING,
--    session_ID STRING
--)
--PARTITIONED BY (pp_date STRING)
--;

INSERT OVERWRITE TABLE tblevent_viewedPage PARTITION (pp_date)
SELECT
    a.page_type,
    a.mp_builtin_city City, -- for restriction of tracking i.e. bots/pp/Geoff
    a.mp_builtin_country Country,
    a.mp_builtin_current_url current_URL,
    a.page_load_id,
    a.session_ID,
    a.pp_date
FROM tblevents a
WHERE a.pp_date >= DATE_SUB(CURRENT_DATE, 5)
    AND a.event = 'viewed page'
;

--DROP TABLE IF EXISTS tblevent_vieweddestination;
--CREATE TABLE tblevent_vieweddestination(
--    page_load_id STRING,
--    session_ID STRING,
--    destination STRING,
--    current_URL STRING
--)
--PARTITIONED BY (pp_date STRING)
--;
--
----capture every time someone viewed a destination page- WITH destination capture
--INSERT OVERWRITE TABLE tblevent_vieweddestination PARTITION (pp_date)
--SELECT
--    a.page_load_ID,
--    a.session_ID,
--    REGEXP_REPLACE(UPPER(SPLIT(a.destination, '[\,]')[0])," ","_") destination,
--    a.mp_builtin_current_url current_URL,
--    a.pp_date
--FROM tblevents a
--WHERE a.pp_date >= DATE_SUB(CURRENT_DATE,65)
--    AND a.event = 'experience viewed destination'
--;

--DROP TABLE IF EXISTS tblevent_viewedsearchResults;
--CREATE TABLE tblevent_viewedsearchResults(
--    current_URL STRING,
--    session_ID STRING,
--    page_load_ID STRING
--)
--PARTITIONED BY (pp_date STRING)
--;


INSERT OVERWRITE TABLE tblevent_viewedsearchResults PARTITION(pp_date)
SELECT
    a.mp_builtin_current_url AS current_url,
    a.session_ID,
    a.page_load_ID,
    a.pp_date
FROM tblevents a
WHERE a.pp_date >= DATE_SUB(CURRENT_DATE, 5)
    AND a.event ='experience viewed search results'
;

--DROP TABLE IF EXISTS tblShelf_SeeAllClicks;
--CREATE TABLE tblShelf_SeeAllClicks(
--    shelf_name STRING,
--    shelf_position INT,
--    page_load_id STRING,
--    session_ID STRING,
--    current_URL STRING
--)
--PARTITIONED BY (pp_date STRING)
--;

-- capture all see all clicks on shelves here:
INSERT OVERWRITE TABLE tblShelf_SeeAllClicks PARTITION (pp_date)
SELECT
    a.shelf_name,
    a.shelf_position,
    a.page_load_ID,
    a.session_ID,
    a.mp_builtin_current_url current_URL,
    a.pp_date
FROM tblevents a
WHERE a.pp_date >= DATE_SUB(CURRENT_DATE, 5)
    AND a.event = 'experience clicked see all in shelf'
;

--DROP TABLE IF EXISTS tblevent_viewedCart;
--CREATE TABLE tblevent_viewedCart(
--    experience_sku STRING,
--    experience_name STRING,
--    price FLOAT,
--    total_price FLOAT,
--    ticket_type array<string>,
--    ticket_type_obj map<string, int>,
--    session_ID STRING,
--    page_load_id STRING
--)
--PARTITIONED BY (pp_date STRING)
--;

INSERT OVERWRITE TABLE tblevent_viewedCart PARTITION (pp_date)
SELECT 
    a.experience_sku,
    a.experience_name,
    a.price,
    a.total_price,
    a.ticket_type,
    a.ticket_type_obj,
    a.session_id,
    a.page_load_id,
    a.pp_date
FROM tblevents a
WHERE a.pp_date >= DATE_SUB(CURRENT_DATE, 5)
    AND a.event = 'experience viewed cart'
;
