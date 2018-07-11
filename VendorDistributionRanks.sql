DROP TABLE IF EXISTS tblMaxRevCt_VendorWebID;
CREATE TABLE tblMaxRevCt_VendorWebID AS
SELECT 
    MAX(b.reviews) MaxReviewCount,
    b.vendor,
    b.pplocation.webid AS webid
FROM tblproductcatalogue b
WHERE b.avgrating > 0
    AND b.isbookable = true 
GROUP BY 
    b.vendor,
    b.pplocation.webid
;

DROP TABLE IF EXISTS tblReviewNormalization_webIDLevel_MBoost;
CREATE TABLE tblReviewNormalization_webIDLevel_MBoost AS
SELECT 
     q.rrmetric
    ,q.normalizedreviews
    ,q.reviews
    ,q.maxreviewcount
    ,q.vendor
    ,q.avgrating
    ,q.city
    ,q.addressstruct
    ,q.firstlocal
    ,q.webid
    ,q.country
    ,q.destinationid
    ,q.price
    ,q.objectid
    ,q.productname
    ,q.isbookable
    ,ROW_NUMBER() OVER (PARTITION BY q.webid ORDER BY q.RRmetric DESC) ordRank
FROM (
    SELECT
        CASE WHEN a.vendor = 'Musement' AND a.reviews/x.MaxReviewCount < 1
            THEN a.reviews/x.MaxReviewCount * a.avgrating * 0.8
            ELSE a.reviews/x.MaxReviewCount * a.avgrating END AS RRmetric,
        a.reviews/x.MaxReviewCount  normalizedReviews,
        a.reviews,
        x.MaxReviewCount,
        a.vendor,
        a.avgrating,
        a.city,
        a.pplocation.formattedaddresses AS addressStruct,
        a.pplocation.formattedaddresses[0] AS firstLocal,
        a.pplocation.webid AS webID,
        a.country,
        a.destinationid,
        a.price,
        a.objectid,
        a.productname,
        a.isbookable
    FROM tblMaxRevCt_VendorWebID x
    INNER JOIN tblproductcatalogue a ON x.vendor = a.vendor
        AND x.webid = a.pplocation.webid
    WHERE a.isbookable = true -- 0 or 1 works too here
        AND a.vendor IN ('Viator','Musement')
    ) q
;

DROP TABLE IF EXISTS tblDemoExport;
CREATE TABLE tblDemoExport AS
SELECT 
    a.objectID AS productID,
    ROUND((a.rrmetric * 1000000), 0) metric
FROM tblReviewNormalization_webIDLevel_MBoost a
;

--DROP TABLE IF EXISTS tblDefaultRankExport;
--CREATE EXTERNAL TABLE tblDefaultRankExport(
--    productid STRING,
--    rank BIGINT
--)
--ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
--WITH SERDEPROPERTIES ('paths'='productID, metric')
--LOCATION 's3://ppdp/exports/tblDefaultRankExport/'
--;

INSERT OVERWRITE TABLE tblDefaultRankExport
SELECT
    productid,
    metric AS rank
FROM tblDemoexport
;
