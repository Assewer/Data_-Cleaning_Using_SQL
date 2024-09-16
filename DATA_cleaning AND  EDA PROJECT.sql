SELECT *
FROM  layoffs;

-- -- TO CLEAN DATA 
-- 1- Remove Duplicates
-- 2- Standardize the data 
-- 3-Null Values or Blank Values 
-- 4- Remove unecessary columns 

-- CREATE A COPY OF THE DATASET 
CREATE TABLE layoffs_staging
LIKE layoffs ;
INSERT INTO layoffs_staging
SELECT * from layoffs;

SELECT *
 FROM layoffs_staging;
 
 
 -- REMOVE DUPLICATES 
	DROP TABLE IF EXISTS dataset_copy;
	 CREATE  TABLE dataset_copy
	 SELECT *,
	 ROW_NUMBER() OVER(PARTITION BY company ,location , percentage_laid_off,
	 industry, total_laid_off, date, stage,country,funds_raised_millions) AS row_num
	 FROM layoffs_staging;
	 
	 DELETE
	 FROM dataset_copy
	 WHERE row_num >1;
	 
 SELECT count(*) 
 FROM dataset_copy;
 
 
 
 -- Standardizing DATA 
	 
	 SELECT distinct country , TRIM(trailing '.' from country) as _dot_removed
	from dataset_copy2
	WHERE UPPER(country) like "UNITED%"
	ORDER BY 1;

	 SELECT company, UPPER(trim(company))
	 FROM dataset_copy;
	 
	 UPDATE dataset_copy
	 SET company=UPPER(trim(company)) ,
	 industry=UPPER(trim(industry)), 
	 location=UPPER(trim(location)),
	 stage=upper(trim(stage));

	select distinct(UPPER(country)) as upper_country
		 from dataset_copy
		 WHERE  UPPER(country) like 'UNITED%'  OR 'UNITED_%'
		 ORDER BY 1
		;
	 UPDATE dataset_copy
		SET country =UPPER(country); 
	 UPDATE dataset_copy
		SET country='UNITED_STATES'
		WHERE UPPER(country) like 'UNITED%STATE%' ;
	 
	 SELECT DISTINCT(upper(stage))
	 FROM dataset_copy
	 ORDER BY 1;
	 
	SELECT * FROM dataset_copy;

	SELECT distinct country , TRIM(trailing '.' from country)
	from dataset_copy2
	WHERE UPPER(country) like "UNITED%"
	ORDER BY 1;

	-- CHANGE THE TYPE OF THE DATE ( FROM TEXT TO DATE)
	SELECT date,
	str_to_date(date, '%m/%d/%Y') as formated_date
	from dataset_copy;
	UPDATE dataset_copy
	SET date=str_to_date(date, '%m/%d/%Y');

	ALTER TABLE dataset_copy
	MODIFY column date  DATE;

	SELECT * 
	 FROM dataset_copy
	;
    
    -- CHANGE THE TYPE OF THE DATE ( FROM TEXT TO DATE) using CAST 
ALTER TABLE  dataset_copy2
CHANGE COLUMN date date_layoofs TEXT;

UPDATE dataset_copy2
SET date_layoofs =str_to_date(date_layoofs,'%m/%d/%Y');

    SELECT *,
		CAST(date_layoofs AS date) as formatted_date 
    FROM dataset_copy2;
    
ALTER TABLE dataset_copy2
ADD COLUMN formatted_date DATE;

UPDATE  dataset_copy2
SET formatted_date = CAST(date_layoofs AS date);

SELECT *
FROM dataset_copy2;

ALTER TABLE dataset_copy2
DROP COLUMN date_layoofs ;
	 
     
     
     
     
     
 -- WORKING WITH NULL AND BLANK VALUES 
	SELECT * 
	 FROM dataset_copy
     WHERE industry IS NULL or industry=''
	;
    
   SELECT* 
   from dataset_copy
   WHERE company ="AIRBNB" and location ="SF BAY AREA";
   
   SELECT t1.company, t1.industry , t2.industry
   FROM dataset_copy as t1
   JOIN dataset_copy as t2
   ON t1.company = t2.company 
   AND t1.location = t2.location 
   WHERE t1.industry is null or t1.industry= '' 
   AND t2.industry is not null ;
   
   UPDATE dataset_copy
   SET industry=NULL
   WHERE industry='';
   
   
   UPDATE dataset_copy t1
   JOIN dataset_copy as t2
	   ON t1.company = t2.company 
	SET t1.industry = t2.industry
    WHERE (t1.industry is null or t1.industry= '' )
   AND t2.industry is not null ;
   
   
   SELECT industry,
   COALESCE(industry,"NOT FOUND")
   FROM  dataset_copy
   where industry IS NULL or industry ='';
   
   UPDATE dataset_copy
   SET industry = COALESCE(industry,"NOT AVAILABLE")	
   WHERE industry IS NULL;
   
   
   
-- REPLACING NULL INT VALUES WITH AVERAGE 
SELECT 
	coalesce(total_laid_off , AVG(total_laid_off) OVER (ORDER BY total_laid_off))
FROM dataset_copy 
;

SELECT total_laid_off,
       COALESCE(total_laid_off, AVG(total_laid_off) OVER (ORDER BY total_laid_off ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS filled_total_laid_off
FROM dataset_copy;

-- OR SIMPLY
SELECT total_laid_off,
       COALESCE(total_laid_off, AVG(total_laid_off) OVER ()) AS filled_total_laid_off
FROM dataset_copy;

-- NOW LET'S UPDATE OUR TABLE 
 WITH avg_ttl_laid_off AS (
		SELECT AVG(total_laid_off) as avg_total_laid_off
        FROM dataset_copy)
UPDATE dataset_copy
SET total_laid_off=(SELECT * FROM avg_ttl_laid_off)
WHERE total_laid_off IS NULL 
;

SELECT * FROM dataset_copy
WHERE total_laid_off IS NULL or total_laid_off= '' ; -- MISSION ACCOMPLISED YESSSS

SELECT * FROM dataset_copy
WHERE percentage_laid_off is null or percentage_laid_off =''; 



-- LET'S DO THE SAME THING AS TOTAL LAID OFF WITH THE PERCENTAGE LAID LOFF
SELECT count(*)
 FROM dataset_copy2
 WHERE percentage_laid_off is null or percentage_laid_off='';
 
 SELECT count(*)
 FROM dataset_copy
 WHERE percentage_laid_off is null or percentage_laid_off='';
 
 SELECT company ,percentage_laid_off
 FROM dataset_copy
 ORDER BY 1;
 
SELECT percentage_laid_off,
		COALESCE(percentage_laid_off, AVG(percentage_laid_off) OVER()) as avg_perc_laid_off
FROM dataset_copy;

SELECT company, percentage_laid_off,
		COALESCE(percentage_laid_off, AVG(percentage_laid_off) OVER(PARTITION BY company))  as avg_perc_laid_off
FROM dataset_copy
ORDER BY 1;

WITH avg_perc_laidoff_by_company AS (
		SELECT company, percentage_laid_off,
		COALESCE(percentage_laid_off, AVG(percentage_laid_off) OVER(PARTITION BY company))  as avg_perc_laid_off
FROM dataset_copy
ORDER BY 1)
UPDATE dataset_copy dc
JOIN avg_perc_laidoff_by_company  apl
	ON dc.company=apl.company
SET dc.percentage_laid_off = apl.avg_perc_laid_off
WHERE dc.percentage_laid_off IS NULL and apl.avg_perc_laid_off is not null
;

SELECT company ,percentage_laid_off
 FROM dataset_copy
 ORDER BY 1;   -- MISSION ACCOMPLISHED SUCCESSFULLY 
 
 UPDATE dataset_copy
 SET percentage_laid_off = COALESCE(percentage_laid_off, "NOT AVAILABLE")
 WHERE percentage_laid_off IS NULL or percentage_laid_off ='';
 
 SELECT count(*)
 from dataset_copy2
 WHERE funds_raised_millions IS NULL;
 
 SELECT count(*)
 from dataset_copy
 WHERE funds_raised_millions IS NULL;
 
 SELECT * FROM dataset_copy
 where company = 'AKERNA';
 
  SELECT *,
	 coalesce(funds_raised_millions , AVG(funds_raised_millions) OVER(PARTITION BY company)) as formated_funds
 FROM dataset_copy;
 
 WITH formatted_funds_t as (
 SELECT *,
	 coalesce(funds_raised_millions , AVG(funds_raised_millions) OVER(PARTITION BY company)) as formated_funds
 FROM dataset_copy)
 UPDATE dataset_copy t1
 JOIN formatted_funds_t ft
	ON t1.company=ft.company
SET t1.funds_raised_millions = ft.formated_funds
WHERE t1.funds_raised_millions IS NULL;
 
  SELECT *
 from dataset_copy
 where percentage_laid_off ="NOT FOUND"
 AND funds_raised_millions IS NULL;
 
 -- DELETE RECORDS THAT DOES'T HAVE NEITHER PERCENTAGE_LAID_OF NEITHER FUNDS_RAISED 
 DELETE
 from dataset_copy
 where percentage_laid_off ="NOT FOUND"
 AND funds_raised_millions IS NULL
;

UPDATE dataset_copy
SET funds_raised_millions = coalesce(funds_raised_millions, 0)
WHERE funds_raised_millions IS NULL;

UPDATE dataset_copy
SET stage = coalesce(stage, 'UNKNOWN')
WHERE stage IS NULL;

ALTER TABLE  dataset_copy
DROP COLUMN row_num;
 
 -- THAT'S OUR FINAL CLEAN DATA
 SELECT *
 from dataset_copy
;


-- -- -- EXPLORATORY DATA ANALYSIS EDA -- -- -- --  -- -- 
select max(percentage_laid_off) as max_perc_layoffs
from dataset_copy
where percentage_laid_off NOT LIKE 'NOT FOUND';


-- EXPLORING THE TOP COMPANIES WHO LAID OFF THE MOST
SELECT company , SUM(total_laid_off)
FROM dataset_copy
GROUP BY company
ORDER BY 2 DESC ;

-- EXPLORING THE TOP YEARS OF LAID OFF FOR EACH COMPANY 
SELECT company , YEAR(date)  , SUM(total_laid_off)  , AVG(percentage_laid_off)
from dataset_copy
GROUP BY 1 , 2;

-- EXPLORING THE LAID OFFS BY YEAR 
SELECT YEAR(date) , SUM(total_laid_off)
FROM dataset_copy
GROUP BY 1
ORDER BY 1; 

--  EXPLORING THE INDUSTRIES THAT ARE MORE AFFECTD BY THE LAYOFFS
SELECT industry , SUM(total_laid_off)
FROM dataset_copy
GROUP BY industry
ORDER BY 2 DESC;

-- EXPLORING THE TOTAL LAID OFF BY EACH COUNTRY 
SELECT country , SUM(total_laid_off)
FROM dataset_copy
GROUP BY country 
order by 2 desc; -- the united states is the top of firing or loosing employess

-- EXPLORING HOW THE TOAL LAID OFFS PROGRESSS OVER TIME 
SELECT SUBSTRING(date,1,7),
SUM(total_laid_off) as sum_of_total_laidoff_by_month
FROM dataset_copy
WHERE date IS NOT NULL
GROUP BY   SUBSTRING(date,1,7)
ORDER BY 1
 ;
 
 -- ROLLING TOTAL  CUMULATIVE TOTAL
WITH rolling_total(date_month, sum_of_total_laidoff_by_month)
  AS(
SELECT SUBSTRING(date,1,7),
SUM(total_laid_off) as sum_of_total_laidoff_by_month
FROM dataset_copy
WHERE date IS NOT NULL
GROUP BY   SUBSTRING(date,1,7)
ORDER BY 1
)
SELECT date_month ,
	sum_of_total_laidoff_by_month ,
    SUM(sum_of_total_laidoff_by_month) OVER (ORDER BY date_month) as rolling_total
FROM rolling_total;

-- EXPLORING THE  TOP COMPANIES BY YEAR
 WITH laidoff_by_year(company,_year ,total_lo) as (
 SELECT company , 
		YEAR(date), 
        SUM(total_laid_off)
 from dataset_copy
 WHERE date IS NOT NULL
 GROUP BY 1,2
 ORDER BY SUM(total_laid_off) DESC
  ) , ranking as (
 SELECT *,
		dense_rank() OVER (PARTITION BY _year ORDER BY total_lo desc) as _rank
from laidoff_by_year

 )

SELECT * FROM ranking
WHERE _rank <=5
;









