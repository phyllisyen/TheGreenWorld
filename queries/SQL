#Q1 How many countries are captured in [owid_energy_data]?

/* there are multiple ways to interpret how many countries there are:
1. according to iso standard which currently comprises 249 countries
2. UN standard which recognises 193 sovereign states and 2 permanent observer states: 195 countries
3. some may say that there are 197 countries if we include Taiwan and Kosovo

Since it is not defined specifically, we will be using the iso_code to determine the number of countries in this dataset. 
albeit, this may not be the most precise way to determine countries, but it does not require us to import additional tables.
another would be to use an IN clause and include all 195/197 country names but that creates additional hassle. */

SELECT COUNT(DISTINCT(country)) 
FROM owid_energy_data
WHERE iso_code NOT LIKE "%OWID_%"
AND iso_code != "";   # 217


# Q2 Find the earliest and latest year in [owid_energy_data]. What are the countries having a record in <owid_energy_data> every year throughout the entire period (from the earliest year to the latest year)?

/* rationale: 
1. narrow down to countries
2. find those with 122 distinct year records 
3. need not change data type of year since SQL can conduct mathematical operations on string */

# from q1, we only want to include countries using iso code
CREATE VIEW country AS
SELECT *
FROM owid_energy_data
WHERE iso_code NOT LIKE "%OWID_%"
AND iso_code != "";


SELECT MIN(year), MAX(year) 
FROM owid_energy_data;

SELECT country, count FROM
(SELECT country, COUNT(DISTINCT(year)) AS count
FROM country
GROUP BY country) t1
WHERE count = (SELECT COUNT(DISTINCT(year)) FROM owid_energy_data);


# Q3 Specific to Singapore, in which year does <fossil_share_energy> stop being the full source of energy (i.e., <100)? Accordingly, show the new sources of energy.

select * from greenworld2022.owid_energy_data
where country = 'Singapore' and fossil_share_energy < 100.0
Limit 1; 

#fossil share energy dropped from 100 to 99.857
# with comparison to the dataset, the low carbon share energy increased from 0 to 0.143 percent after 1986, consisting all from other renewable energy sources

select country,year,fossil_share_energy, low_carbon_share_energy,hydro_share_energy,nuclear_share_energy,other_renewables_share_energy,renewables_share_energy,solar_share_energy,wind_share_energy from greenworld2022.owid_energy_data
where country = 'Singapore' and fossil_share_energy < 100.0
Limit 1; 

# Q4 Compute the average <GDP> of each ASEAN country from 2000 to 2021 (inclusive of both years). Display the list of countries based on the descending average GDP value.
select country, avg(nullif(gdp,0)) as avgGDP 
from greenworld2022.owid_energy_data
where country in ("Singapore", "Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Thailand", "Vietnam")
and year between '2000' and '2021'
group by country
order by avgGDP desc;

# Q5 (Without creating additional tables/collections) For each ASEAN country, from 2000 to 2021 (inclusive of both years), compute the 3-year moving average of <oil_consumption> (e.g., 1st: average oil consumption from 2000 to 2002, 2nd: average oil consumption from 2001 to 2003, etc.). Based on the 3-year moving averages, identify instances of negative changes (e.g., An instance of negative change is detected when 1st 3-yo average = 74.232, 2nd 3-yo average = 70.353). Based on the pair of 3-year averages, compute the corresponding 3-year moving averages in GDP.
SELECT
    country,
    year,
    oil_consumption,
    gdp,
    CASE
        WHEN year >= 2000 AND year <= 2021 THEN
            AVG(NULLIF(oil_consumption, 0)) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    END AS three_year_moving_avg_oil_consumption,
    CASE 
        WHEN year >= 2000 AND year <= 2021 THEN
            CASE
                WHEN  AVG(NULLIF(oil_consumption, 0)) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) < 
                AVG(NULLIF(oil_consumption, 0)) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) 
                THEN 'An instance of negative change is detected'
            END
    END AS oil_avg_negative_change,
    CASE
        WHEN year >= 2000 AND year <= 2021 THEN
            AVG(NULLIF(gdp, 0)) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    END AS three_year_moving_avg_gdp
FROM
    owid_energy_data
    WHERE
    country IN ('Brunei', 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Myanmar', 'Philippines', 'Singapore', 'Thailand', 'Vietnam')
    AND oil_consumption <> ””
    AND year >= 2000
    AND year <= 2021
    ORDER BY country, year ASC;


# Q6 For each <energy_products> and <sub_products>, display the overall average of <value_ktoe> from [importsofenergyproducts] and [exportsofenergyproducts].
# disabled full group by: 
SELECT a.energy_products, a.sub_products, a.import_avgktoe, b.export_avgktoe
FROM 
(SELECT energy_products,sub_products,avg(value_ktoe) AS import_avgktoe 
FROM importsofenergyproducts GROUP BY sub_products) a 
LEFT JOIN 
(SELECT energy_products,sub_products,avg(value_ktoe) AS export_avgktoe 
FROM exportsofenergyproducts GROUP BY sub_products) b 
ON a.sub_products=b.sub_products;

# enabled full group by 
select i.energy_products, i.sub_products,avg_import_ktoe, avg_export_ktoe
from (select energy_products, sub_products, avg(value_ktoe) as avg_import_ktoe
from importsofenergyproducts
group by energy_products, sub_products) as i left join 
(select energy_products, sub_products, avg(value_ktoe) as avg_export_ktoe
from exportsofenergyproducts
group by energy_products, sub_products) as e
on i.energy_products = e.energy_products and i.sub_products = e.sub_products;


# Q7 For each combination of <energy_products> and <sub_products>, find the yearly difference in <value_ktoe> from [importsofenergyproducts] and [exportsofenergyproducts]. Identify those years where more than 4 instances of export value > import value can be detected.
SELECT c.year, c.energy_products, c.sub_products, c.exports, c.imports, exports-imports AS diff 
FROM (SELECT a.year, a.energy_products, a.sub_products, a.value_ktoe AS exports, b.value_ktoe AS imports 
FROM exportsofenergyproducts a 
LEFT JOIN importsofenergyproducts b 
ON a.sub_products=b.sub_products AND a.year=b.year) c; 

SELECT c.year
FROM (SELECT a.year, a.energy_products, a.sub_products, a.value_ktoe AS exports, b.value_ktoe AS imports 
FROM exportsofenergyproducts a 
LEFT JOIN importsofenergyproducts b 
ON a.sub_products=b.sub_products AND a.year=b.year
WHERE a.value_ktoe-b.value_ktoe> 0) c
GROUP BY c.year 
HAVING count(c.year) >4;

