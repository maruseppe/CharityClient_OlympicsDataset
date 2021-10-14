-- Databricks notebook source
SELECT * FROM default.athlete_events_14_csv AS SS WHERE TEAM LIKE "Uni%" LIMIT 100

-- COMMAND ----------

SELECT TEAM
       , COUNT(Sex) AS MALES
FROM (
    
      SELECT * 
      FROM (
        SELECT *
        FROM default.athlete_events_14_csv
        ) AS ori
      INNER JOIN (
        SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
              ELSE TEAM
            END AS TEAM_ERRATUM
        FROM default.athlete_events_14_csv
        ) AS mod
      ON ori.ID=mod.ID
      ) AS sport_stats_erratum
  
WHERE Sex = "M"
GROUP BY TEAM
SORT BY MALES DESC


-- COMMAND ----------

SELECT TEAM
       , COUNT(Sex) AS FEMALES
FROM (
    
      SELECT * 
      FROM (
        SELECT *
        FROM default.athlete_events_14_csv
        ) AS ori
      INNER JOIN (
        SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
              ELSE TEAM
            END AS TEAM_ERRATUM
        FROM default.athlete_events_14_csv
        ) AS mod
      ON ori.ID=mod.ID
      ) AS sport_stats_erratum
  
WHERE Sex = "F"
GROUP BY TEAM
SORT BY FEMALES DESC

-- COMMAND ----------

SELECT TEAM 
      , FEMALES
FROM (     
SELECT TEAM
       , COUNT(Sex) AS FEMALES
FROM (
    
      SELECT * 
      FROM (
        SELECT *
        FROM default.athlete_events_14_csv
        ) AS ori
      INNER JOIN (
        SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
              ELSE TEAM
            END AS TEAM_ERRATUM
        FROM default.athlete_events_14_csv
        ) AS mod
      ON ori.ID=mod.ID
      ) AS sport_stats_erratum
  
WHERE Sex = "F"
GROUP BY TEAM
SORT BY FEMALES DESC
)
WHERE TEAM = "Mauritania"     --Islam > 70%,  Pop> 10 mil
      OR TEAM = "Somalia"         
      OR TEAM = "Tunisia"         
      OR TEAM = "Afghanistan"      
      OR TEAM = "Algeria"
      OR TEAM = "Iran"
      OR TEAM = "Yemen"
      OR TEAM = "Morocco"
      OR TEAM = "Niger"
      OR TEAM = "Saudi Arabia"
      OR TEAM = "Jordan"
      OR TEAM = "Sudan"
      OR TEAM = "Azerbajan"
      OR TEAM = "Pakistan"
      OR TEAM = "Senegal"
      OR TEAM = "Iraq"
      OR TEAM = "Mali"
      OR TEAM = "Bangladesh"
      OR TEAM = "Egypt"
      OR TEAM = "Turkey"
      OR TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Syria"

-- COMMAND ----------

--REMOVE -1 or -2 from TEAM
SELECT ID
           , TEAM
           , SUBSTRING (TEAM, 0, INSTR(TEAM, '-')-1 )
FROM default.athlete_events_5_csv
WHERE INSTR(TEAM, '-')>0

-- COMMAND ----------

--REMOVE -1 or -2 from TEAM
SELECT * 
FROM (
  SELECT *
  FROM default.athlete_events_5_csv
  ) AS ori
INNER JOIN (
  SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-')-1)
              ELSE TEAM
            END AS TEAM_ERRATUM
  FROM default.athlete_events_5_csv
     ) AS mod
ON ori.ID=mod.ID
    
    

-- COMMAND ----------

-- Here we report the percentage of women across countries summed up from London 2012  up  to 2016. 2012 has been recorded as the first olympics where women participated worldwilde (ex: first time that Saudi Arabia women participated)

SELECT FemTab.TEAM_ERRATUM as TEAM
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS FEM_PERC_FROM_LONDON2012
FROM (
    SELECT TEAM_ERRATUM
           , COUNT(Sex) AS FEMALES
    FROM (
    
      SELECT * 
      FROM (
        SELECT *
        FROM default.athlete_events_5_csv
        ) AS ori
      INNER JOIN (
        SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
              ELSE TEAM
            END AS TEAM_ERRATUM
        FROM default.athlete_events_5_csv
        ) AS mod
      ON ori.ID=mod.ID
      ) AS sport_stats_erratum
      
    WHERE Sex = "F"
          AND YEAR >= 2012
    GROUP BY TEAM_ERRATUM
    ) AS FemTab
    
  INNER JOIN (
  
    SELECT TEAM_ERRATUM
           , COUNT(Sex) AS MALES
    FROM (
    
      SELECT * 
      FROM (
        SELECT *
        FROM default.athlete_events_5_csv
        ) AS ori
      INNER JOIN (
        SELECT ID
          , CASE 
              WHEN INSTR(TEAM, '-') > 0
              THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
              ELSE TEAM
            END AS TEAM_ERRATUM
        FROM default.athlete_events_5_csv
        ) AS mod
      ON ori.ID=mod.ID
      ) AS sport_stats_erratum
      
    WHERE Sex = "M"    
          AND YEAR >= 2012
    GROUP BY TEAM_ERRATUM
    ) AS MaleTab
    
  ON 
    FemTab.TEAM_ERRATUM = MaleTab.TEAM_ERRATUM
SORT BY FEM_PERC_FROM_LONDON2012 DESC
    

-- COMMAND ----------

-- Here we report the average percentage of women across countries from 1980. This allows to normalize over the individual political history of countries up to date. As we report average, and not sum of percentage, it does not matter when a given country has entered the Olympics, but rather the average percentage attendance of women at Olympics ever attended. Consequentely we could extrapolate a map of women rights and thus democracy across countries up to date.

SELECT Fem_Years.TEAM_ERRATUM as TEAM
       , AVG(Fem_Years.FEM_PERC) AS AVERAGE_FEM_PERC_FROM_LONDON2012
FROM (
    SELECT FemTab.TEAM_ERRATUM
          , FemTab.Year
          , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS FEM_PERC
    FROM (
      SELECT TEAM_ERRATUM
             , Year
             , COUNT(Sex) AS FEMALES
      FROM (
            
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_7_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_7_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
  
      WHERE Sex = "F"
            AND Year >= 1980
      GROUP BY TEAM_ERRATUM 
            , Year
              ) AS FemTab
    INNER JOIN (
      SELECT TEAM_ERRATUM
             , Year
             , COUNT(Sex) AS MALES
      FROM (
            
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_7_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_7_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

      WHERE Sex = "M"    
            AND Year >= 1980
      GROUP BY TEAM_ERRATUM 
            , Year) AS MaleTab
    ON 
      FemTab.TEAM_ERRATUM = MaleTab.TEAM_ERRATUM
    AND 
      FemTab.Year = MaleTab.Year
    ) AS Fem_Years
--WHERE Fem_Years.TEAM="United States"
GROUP BY Fem_Years.TEAM_ERRATUM
SORT BY HYSTORICAL_AVERAGE_FEM_PERC DESC
    

-- COMMAND ----------

-- Here we report the average percentage of women across countries from 1980. This allows to normalize over the individual political history of countries up to date. As we report average, and not sum of percentage, it does not matter when a given country has entered the Olympics, but rather the average percentage attendance of women at Olympics ever attended. Consequentely we could extrapolate a map of women rights and thus democracy across countries up to date.
-- Here we report the percentage of women across countries summed up from London 2012  up  to 2016. 2012 has been recorded as the first olympics where women participated worldwilde (ex: first time that Saudi Arabia women participated)
SELECT Fem_Years.TEAM_ERRATUM as TEAM
       , AVG(Fem_Years.FEM_PERC) AS Average_Women_Perc_From_London2012
FROM (
    SELECT FemTab.TEAM_ERRATUM
          , FemTab.Year
          , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS FEM_PERC
    FROM (
      SELECT TEAM_ERRATUM
             , Year
             , COUNT(Sex) AS FEMALES
      FROM (
            
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_7_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_7_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
  
      WHERE Sex = "F"
            AND Year >= 2012
      GROUP BY TEAM_ERRATUM 
            , Year
              ) AS FemTab
    INNER JOIN (
      SELECT TEAM_ERRATUM
             , Year
             , COUNT(Sex) AS MALES
      FROM (
            
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_7_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_7_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

      WHERE Sex = "M"    
            AND Year >= 2012
      GROUP BY TEAM_ERRATUM 
            , Year) AS MaleTab
    ON 
      FemTab.TEAM_ERRATUM = MaleTab.TEAM_ERRATUM
    AND 
      FemTab.Year = MaleTab.Year
    ) AS Fem_Years
--WHERE Fem_Years.TEAM="United States"
GROUP BY Fem_Years.TEAM_ERRATUM
SORT BY Average_Women_Perc_From_London2012 DESC
    

-- COMMAND ----------

-- Here we report the attendance percentage of women summed up worlwilde for each Olympics starting from 1900. 
--We can notice two main slope changes : western countries women rights in the 50s and perhaps arab countries women rights in the 90s

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS WORLD_FEM_PERC
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
            
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "F"
      --AND TEAM = "United States"
      AND YEAR > 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                  
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "M"
       --AND TEAM = "United States"
       AND YEAR > 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

-- Here we report the attendance percentage of men summed up worlwilde for each Olympics starting from 1900.

SELECT FemTab.YEAR as YEAR
       , MaleTab.MALES*100/(FemTab.FEMALES+MaleTab.MALES) AS WORLD_MALE_PERC
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
                 
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "F"
      --AND TEAM = "United States"
      AND YEAR > 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                      
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "M"
       --AND TEAM = "United States"
       AND YEAR > 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS US_FEM_PERC
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
                     
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "F"
      AND TEAM_ERRATUM = "United States"
      AND YEAR > 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                    
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "M"
       AND TEAM_ERRATUM = "United States"
       AND YEAR > 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

     SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS ARAB_FEM_PERC
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
                     
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_11_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_11_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "F"
      AND (TEAM_ERRATUM = "Mauritania"     --Islam > 70%,  Pop> 10 mil
      OR TEAM_ERRATUM = "Somalia"         
      OR TEAM_ERRATUM = "Tunisia"         
      OR TEAM_ERRATUM = "Afghanistan"      
      OR TEAM_ERRATUM = "Algeria"
      OR TEAM_ERRATUM = "Iran"
      OR TEAM_ERRATUM = "Yemen"
      OR TEAM_ERRATUM = "Morocco"
      OR TEAM_ERRATUM = "Niger"
      OR TEAM_ERRATUM = "Saudi Arabia"
      OR TEAM_ERRATUM = "Jordan"
      OR TEAM_ERRATUM = "Sudan"
      OR TEAM_ERRATUM = "Azerbajan"
      OR TEAM_ERRATUM = "Pakistan"
      OR TEAM_ERRATUM = "Senegal"
      OR TEAM_ERRATUM = "Iraq"
      OR TEAM_ERRATUM = "Mali"
      OR TEAM_ERRATUM = "Bangladesh"
      OR TEAM_ERRATUM = "Egypt"
      OR TEAM_ERRATUM = "Turkey"
      OR TEAM_ERRATUM = "Tajikistan"
      OR TEAM_ERRATUM = "Uzbekistan"
      OR TEAM_ERRATUM = "Kazakhstan"
      OR TEAM_ERRATUM = "Syria"
      )
      AND YEAR > 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                    
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_11_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_11_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "M"
       AND (TEAM_ERRATUM = "Mauritania"     --Islam > 70%,  Pop> 10 mil
      OR TEAM_ERRATUM = "Somalia"         
      OR TEAM_ERRATUM = "Tunisia"         
      OR TEAM_ERRATUM = "Afghanistan"      
      OR TEAM_ERRATUM = "Algeria"
      OR TEAM_ERRATUM = "Iran"
      OR TEAM_ERRATUM = "Yemen"
      OR TEAM_ERRATUM = "Morocco"
      OR TEAM_ERRATUM = "Niger"
      OR TEAM_ERRATUM = "Saudi Arabia"
      OR TEAM_ERRATUM = "Jordan"
      OR TEAM_ERRATUM = "Sudan"
      OR TEAM_ERRATUM = "Azerbajan"
      OR TEAM_ERRATUM = "Pakistan"
      OR TEAM_ERRATUM = "Senegal"
      OR TEAM_ERRATUM = "Iraq"
      OR TEAM_ERRATUM = "Mali"
      OR TEAM_ERRATUM = "Bangladesh"
      OR TEAM_ERRATUM = "Egypt"
      OR TEAM_ERRATUM = "Turkey"
      OR TEAM_ERRATUM = "Tajikistan"
      OR TEAM_ERRATUM = "Uzbekistan"
      OR TEAM_ERRATUM = "Kazakhstan"
      OR TEAM_ERRATUM = "Syria")
       AND YEAR  1900
    GROUP BY YEAR  ) AS MaleTabA
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

     SELECT FemTab.YEAR as YEAR  
       , TEAM_ERRATUM
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS ARAB_FEM_PERC
  FROM (
    SELECT YEAR
           , TEAM_ERRATUM
           , COUNT(Sex) AS FEMALES
    FROM (
                     
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_11_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_11_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "F"
      AND (TEAM_ERRATUM = "Mauritania"     --Islam > 70%,  Pop> 10 mil
      OR TEAM_ERRATUM = "Somalia"         
      OR TEAM_ERRATUM = "Tunisia"         
      OR TEAM_ERRATUM = "Afghanistan"      
      OR TEAM_ERRATUM = "Algeria"
      OR TEAM_ERRATUM = "Iran"
      OR TEAM_ERRATUM = "Yemen"
      OR TEAM_ERRATUM = "Morocco"
      OR TEAM_ERRATUM = "Niger"
      OR TEAM_ERRATUM = "Saudi Arabia"
      OR TEAM_ERRATUM = "Jordan"
      OR TEAM_ERRATUM = "Sudan"
      OR TEAM_ERRATUM = "Azerbajan"
      OR TEAM_ERRATUM = "Pakistan"
      OR TEAM_ERRATUM = "Senegal"
      OR TEAM_ERRATUM = "Iraq"
      OR TEAM_ERRATUM = "Mali"
      OR TEAM_ERRATUM = "Bangladesh"
      OR TEAM_ERRATUM = "Egypt"
      OR TEAM_ERRATUM = "Turkey"
      OR TEAM_ERRATUM = "Tajikistan"
      OR TEAM_ERRATUM = "Uzbekistan"
      OR TEAM_ERRATUM = "Kazakhstan"
      OR TEAM_ERRATUM = "Syria"
      )
      AND YEAR < 1940
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                    
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_11_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_11_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum

    WHERE Sex = "M"
       AND (TEAM_ERRATUM = "Mauritania"     --Islam > 70%,  Pop> 10 mil
      OR TEAM_ERRATUM = "Somalia"         
      OR TEAM_ERRATUM = "Tunisia"         
      OR TEAM_ERRATUM = "Afghanistan"      
      OR TEAM_ERRATUM = "Algeria"
      OR TEAM_ERRATUM = "Iran"
      OR TEAM_ERRATUM = "Yemen"
      OR TEAM_ERRATUM = "Morocco"
      OR TEAM_ERRATUM = "Niger"
      OR TEAM_ERRATUM = "Saudi Arabia"
      OR TEAM_ERRATUM = "Jordan"
      OR TEAM_ERRATUM = "Sudan"
      OR TEAM_ERRATUM = "Azerbajan"
      OR TEAM_ERRATUM = "Pakistan"
      OR TEAM_ERRATUM = "Senegal"
      OR TEAM_ERRATUM = "Iraq"
      OR TEAM_ERRATUM = "Mali"
      OR TEAM_ERRATUM = "Bangladesh"
      OR TEAM_ERRATUM = "Egypt"
      OR TEAM_ERRATUM = "Turkey"
      OR TEAM_ERRATUM = "Tajikistan"
      OR TEAM_ERRATUM = "Uzbekistan"
      OR TEAM_ERRATUM = "Kazakhstan"
      OR TEAM_ERRATUM = "Syria")
       AND YEAR  1900
    GROUP BY YEAR  ) AS MaleTabA
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS US_Athletics_Fem_Perc
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
                       
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
     
    WHERE Sex = "F"
      AND SPORT = "Athletics"
      AND TEAM_ERRATUM = "United States"
      AND YEAR >= 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_5_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_5_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "M"
       AND SPORT = "Athletics"
       AND TEAM_ERRATUM = "United States"
       AND YEAR >= 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS MOROCCO_FEM_PERC
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM 
      default.athlete_events_5_csv
    WHERE Sex = "F"
      AND TEAM = "Morocco"
      AND YEAR > 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM 
      default.athlete_events_5_csv
    WHERE Sex = "M"
       AND TEAM = "Morocco"
       AND YEAR > 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS Morocco_Athletics_Fem_Perc
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM 
      default.athlete_events_5_csv
    WHERE Sex = "F"
      AND SPORT = "Athletics"
      AND TEAM = "Morocco"
      AND YEAR >= 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM 
      default.athlete_events_5_csv
    WHERE Sex = "M"
       AND SPORT = "Athletics"
       AND TEAM = "Morocco"
       AND YEAR >= 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

SELECT FemTab.YEAR as YEAR
       , FemTab.FEMALES*100/(FemTab.FEMALES+MaleTab.MALES) AS Saudi_Arabia_Fem_Perc
  FROM (
    SELECT YEAR
           , COUNT(Sex) AS FEMALES
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      --AND SPORT = "Athletics"
      AND TEAM = "Saudi Arabia"
      AND YEAR >= 1900
    GROUP BY YEAR ) AS FemTab
  INNER JOIN (
    SELECT YEAR
           , COUNT(Sex) AS MALES
    FROM 
      (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "M"
       --AND SPORT = "Athletics"
       AND TEAM = "Saudi Arabia"
       AND YEAR >= 1900
    GROUP BY YEAR  ) AS MaleTab
  ON 
    FemTab.YEAR = MaleTab.YEAR
SORT BY YEAR ASC
    

-- COMMAND ----------

    SELECT  --Sport 
            TEAM
            --, Medal
           , COUNT(Medal) AS Medals
    FROM (
                         
        SELECT *
        FROM (
          SELECT *
          FROM default.athlete_events_14_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_14_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      --AND SPORT = "Athletics"
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
    GROUP BY TEAM
SORT BY Medals DESC
    

-- COMMAND ----------

    SELECT  Season 
            --, TEAM
            --, Medal
           , COUNT(Medal) AS Medals
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      --AND SPORT = "Athletics"
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
    GROUP BY Season
SORT BY Medals DESC
    

-- COMMAND ----------

    SELECT  Sport 
            --, TEAM
            --, Medal
           , COUNT(Medal) AS Medals
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      --AND SPORT = "Athletics"
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
    GROUP BY Sport
SORT BY Medals DESC
    

-- COMMAND ----------

    SELECT  Event
            --, TEAM
            , COUNT(Medal) AS Medals
           ,AVG( CAST(Weight AS INT)/POWER(CAST(Height AS INT)/100, 2) ) AS AVG_BMI
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      AND SPORT = "Athletics"
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
      
    GROUP BY Event
SORT BY Medals DESC
    

-- COMMAND ----------

    SELECT  --sport_stats_erratum.Name
            --, TEAM
            --, Medal
           CAST(Weight AS INT) AS Weight
           , CAST(Height AS INT)/100  AS Height
           , CAST(SPORT AS STRING) Sport
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_13_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_13_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      AND (SPORT = "Athletics"
      OR SPORT = "Wrestling")
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
SORT BY Sport DESC
    

-- COMMAND ----------

    SELECT  Event
            --, TEAM
            , COUNT(Medal) AS Medals
           ,AVG( CAST(Weight AS INT)/POWER(CAST(Height AS INT)/100, 2) ) AS AVG_BMI
    FROM (
                         
        SELECT * 
        FROM (
          SELECT *
          FROM default.athlete_events_8_csv
          ) AS ori
        INNER JOIN (
          SELECT ID
            , CASE 
                WHEN INSTR(TEAM, '-') > 0
                THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                ELSE TEAM
              END AS TEAM_ERRATUM
          FROM default.athlete_events_8_csv
          ) AS mod
        ON ori.ID=mod.ID
        ) AS sport_stats_erratum
        
    WHERE Sex = "F"
      AND SPORT = "Wrestling"
      --AND TEAM = "Saudi Arabia"
      AND (TEAM = "Tajikistan"
      OR TEAM = "Uzbekistan"
      OR TEAM = "Kazakhstan"
      OR TEAM = "Tunisia"
      OR TEAM = "Algeria"
      OR TEAM = "Turkey"
      OR TEAM = "Syria"
      OR TEAM = "Iran"
      OR TEAM = "Morocco"
      OR TEAM = "Egypt")
      AND Medal != "NA"
      
    GROUP BY Event
SORT BY Medals DESC
    

-- COMMAND ----------

SELECT Weight
       , Height
       , CASE WHEN Sport = "Athletics"
           THEN 0
           ELSE 1
         END AS Sport
       FROM (
       
           SELECT  --sport_stats_erratum.Name
              --, TEAM
              --, Medal
             CAST(Weight AS INT) AS Weight
             , CAST(Height AS INT)/100  AS Height
             , CAST(SPORT AS STRING) Sport
           FROM (
                                
               SELECT * 
               FROM (
                 SELECT *
                 FROM default.athlete_events_13_csv
                 ) AS ori
               INNER JOIN (
                 SELECT ID
                   , CASE 
                       WHEN INSTR(TEAM, '-') > 0
                       THEN SUBSTRING (TEAM, 0, INSTR(TEAM, '-') -1)
                       ELSE TEAM
                     END AS TEAM_ERRATUM
                 FROM default.athlete_events_13_csv
                 ) AS mod
               ON ori.ID=mod.ID
               ) AS sport_stats_erratum
               
           WHERE Sex = "F"
             AND (SPORT = "Athletics"
             OR SPORT = "Wrestling")
             --AND TEAM = "Saudi Arabia"
             AND (TEAM = "Tajikistan"
             OR TEAM = "Uzbekistan"
             OR TEAM = "Kazakhstan"
             OR TEAM = "Tunisia"
             OR TEAM = "Algeria"
             OR TEAM = "Turkey"
             OR TEAM = "Syria"
             OR TEAM = "Iran"
             OR TEAM = "Morocco"
             OR TEAM = "Egypt")
             AND Medal != "NA"
           SORT BY Sport DESC
           )
       

-- COMMAND ----------


