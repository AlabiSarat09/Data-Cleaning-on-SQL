-- Data Cleaning
USE world_layoffs2;


SELECT * FROM layoffs_data2;

-- Remove Duplicates
-- Standardize the Data
-- Null Values or Blank Values
-- Remove any columns

CREATE TABLE layoffs_staging
LIKE layoffs_data2;


SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs_data2;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, 
total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry,
total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM layoffs_staging
WHERE company = 'Hibob';

USE world_layoffs2;
SELECT * FROM layoffs_staging;


CREATE TABLE layoffs_staging2
(
	company text,
	location text,
	industry text,
	total_laid_off int DEFAULT NULL,
	percentage_laid_off text,
	date text,
	stage text,
	country text,
	funds_raised int DEFAULT NULL,
	row_num int
);


DESC layoffs_staging;
DESC layoffs_staging2;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry,
total_laid_off, percentage_laid_off, date,
stage, country, funds_raised) AS row_num
FROM layoffs_staging;

ALTER TABLE layoffs_staging2 MODIFY
COLUMN total_laid_off text;

DELETE FROM layoffs_staging2
WHERE row_num > 1;


SELECT * FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

-- Standardizing data

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2 SET
industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry IS NULL;

DELETE FROM layoffs_staging2
WHERE industry IS NULL;

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2 SET
location = 'FÃ¸rde'
WHERE location ='Førde';


SELECT date
-- str_to_date(date, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2 SET
date = str_to_date(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = ''
AND total_laid_off = ''
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT ti.industry, t2.industry
FROM layoffs_staging2 ti
JOIN layoffs_staging2 t2
	ON ti.company = t2.company
	AND ti.location = t2.location
WHERE (ti.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 SET
industry = 'Education'
WHERE industry = NULL;

USE world_layoffs2;

SELECT COUNT(industry) FROM layoffs_staging2
WHERE industry = 'Education';

UPDATE layoffs_staging2 SET
industry = 'Education'
WHERE industry IS NULL;

SET SQL_SAFE_UPDATES = 0;

SELECT * FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';

UPDATE layoffs_staging2 SET
total_laid_off = null , percentage_laid_off = null
WHERE total_laid_off = '' AND percentage_laid_off = '';

SELECT COUNT(percentage_laid_off) FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

DELETE FROM layoffs_staging
WHERE percentage_laid_off = '' 
AND total_laid_off = '';


SELECT * FROM layoffs_Staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

























