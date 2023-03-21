CREATE DATABASE wiki_so;
CREATE SCHEMA experiments;

//This query will run in ~90 seconds.
CREATE TABLE wiki_so.experiments.wikidata_original AS (SELECT * FROM wikidata.wikidata.wikidata_original);

//This query will run in ~7 seconds.
CREATE TABLE wiki_so.experiments.entity_is_subclass_of AS (SELECT * FROM wikidata.wikidata.entity_is_subclass_of);

// To get the size of the table
select (bytes/1073741824) table_size_in_gb, snowflake.account_usage.tables.*
from snowflake.account_usage.tables
where table_name = 'WIKIDATA_ORIGINAL' and table_schema = 'EXPERIMENTS' and table_catalog = 'WIKI_SO' and DELETED IS NULL;

// To get the approximate values of the columns
select approx_count_distinct(label,description), hll(label,description) 
from wikidata_original; // approx_count_distinct = 71,309,676

// Defining Search Optimization on VARCHAR fields
ALTER TABLE wikidata_original ADD SEARCH OPTIMIZATION ON EQUALITY(id, label, description);

// Defining Search Optimization on VARCHAR fields optimized for Wildcard search
ALTER TABLE wikidata_original ADD SEARCH OPTIMIZATION ON SUBSTRING(description);

// Defining Search Optimization on VARIANT field
ALTER TABLE wikidata_original ADD SEARCH OPTIMIZATION ON EQUALITY(labels);

// Defining Search Optimization on LASTREVID
ALTER TABLE wikidata_original ADD SEARCH OPTIMIZATION ON EQUALITY(LASTREVID);
ALTER TABLE wikidata_original ADD SEARCH OPTIMIZATION ON EQUALITY(label);



//Ensure Search Optimization first time indexing is complete
//Now, let's verify that Search Optimization is enabled and the backend process has finished indexing our data. It will take about 2 minutes for that to happen as the optimized search access paths are being built for these columns by Snowflake.

//Run the below query against the newly created database (WIKI_SO)

DESCRIBE SEARCH OPTIMIZATION ON wikidata_original;

SHOW TABLES LIKE '%wikidata_original%';

// Query 1
SELECT * 
  FROM wikidata_original
  WHERE 
    label= 'iPhone' AND 
    description ILIKE '%wikimedia%page%';

// Query 2
SELECT * 
  FROM wikidata_original
  WHERE 
    label= 'iPhone' AND 
    description='Wikimedia disambiguation page';

// Query 3
SELECT * 
  FROM wikidata_original 
  WHERE 
    description ILIKE '%blog post%';


// Query 4 : Range Query
SELECT * 
  FROM wikidata_original 
  WHERE LASTREVID BETWEEN 586097930 and 586918960;


// Cost of enabling Search Optimization Service on the table
select * from table(information_schema.search_optimization_history());

// Defining Search Optimization on VARIANT field
ALTER TABLE entity_is_subclass_of ADD SEARCH OPTIMIZATION ON EQUALITY(entity_id);
ALTER TABLE entity_is_subclass_of ADD SEARCH OPTIMIZATION ON EQUALITY(subclass_of_name);

// CHeck if Optimization is complete or not
SHOW TABLES LIKE '%entity_is_subclass_of%';

//To clear the data in the cache to check the new query plan
ALTER SESSION SET USE_CACHED_RESULT = false;
ALTER WAREHOUSE COMPUTE_WH SUSPEND;

// Join Query
SELECT *  
  FROM entity_is_subclass_of AS e 
  JOIN wikidata_original AS o ON (e.subclass_of_name = o.label)
  WHERE e.entity_id IN ('Q1437617','Q8564669','Q1968','Q5470299') ;





// Taking out the search optimization
ALTER TABLE wikidata_original DROP SEARCH OPTIMIZATION;
