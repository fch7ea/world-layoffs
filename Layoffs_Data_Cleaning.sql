-- Data Cleaning

-- 0. Duplicate table (so the original stays safe)
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Unecessary Columns/Rows 

-- 0. Duplicate table (so the original stays safe)

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- (easier with a unique id column or similar... but this table doesn't have one)

-- Find duplicates 
-- We need to partition by everything because some rows differ only in the last row, as seen here:
-- Look at the returned values (because it wouldn't show up if couldn't be aggregated
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

-- A CTE can;t be updated
SELECT * # If we made this DELETE, it wouldn't work because you can't updates cte's
FROM duplicate_cte
WHERE row_num > 1;


-- Instead, let's make another table to delete from.
-- copy to clipboard -> create statement
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT # add this!
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Insert the stuff with the row table into layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

-- Now look at the duplicates:
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Looks good! Duplicate the select statement, but use the delete command.
-- Make sure safe updates is off!
-- "Safe mode requires that the condition references a key column to minimize the chance of accidentally deleting too many rows."
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- It works! Now look at the whole table:
SELECT *
FROM layoffs_staging2;

-- 2. Standardize the Data
-- (Finding issues in data and fixing it)
-- White space is something we can fix
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# Let's look at the industry too.
# Observations: 2 null, crypto has 3 categories
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

# look at crypto real quick
# Most are "Crypto" so we'll just update them all to that
SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

# Now let's look at location
# Observations: Looks good!
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

# Now country
# Observations: United States has 2 entries, one with a period after it
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

# Let's check what it's suppoesd to be
# As expected, "United States" is the standard, not "United States."
SELECT *
FROM layoffs_staging2
WHERE country LIKE "United States%"
ORDER BY 1;

# A trick with TRIM to remove the period easily!
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

# Another fix we can make is turning the date column into a date, not just text.
# Uppercase Y represents a 4 digit year, lowercase seems to be for a 2 digit year.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

# Now officially update it
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

# Check our work:
SELECT `date`
FROM layoffs_staging2;

# The date columns is still text - 
# but now it's in date format text, so we can convert it to a date!
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null Values or Blank Values

# These are fairly useless - we'll save this query for step 4
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# There were some null values in the industry column...
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# Check if it's populable, like for Airbnb.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# Convert blanks spaces like '' to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

# It exists, so let's run a join on itself to get the blanks filled.
# First make a query
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

# Bally's Interactive was still NULL, check if it has another row we can copy from.
# Observations: It's the only row so we just don't know the industry.
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

# That's all we'll do for null values becaues for total_laid_off,
# percentage_laid_off, funds_raised_millions, etc. we can't really fill
# it in, unless we had company totals but we don't. We could
# web scrape for funds raised but that's not what this project is about.

-- 4. Remove Unecessary Columns/Rows 

# Let's look at that query again:
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# We can delete them because they aren't useful for data analysis
# (the data isn't there).
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

# And now we can remove the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

# Data cleaning complete! This dataset is now ready for data exploration!