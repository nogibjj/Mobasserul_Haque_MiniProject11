```sql
SELECT 
    airline,
    avail_seat_km_per_week,
    SUM(total_incidents) AS Total_Incidents,
    SUM(total_fatalities) AS Total_Fatalities,
    SUM(fatal_accidents_85_99) AS Total_Fatal_Accidents_85_99,
    SUM(fatal_accidents_00_14) AS Total_Fatal_Accidents_00_14
FROM 
    mh720_week11.airline_safety_transformed
GROUP BY 
    airline, avail_seat_km_per_week
HAVING 
    SUM(total_incidents) > 0
ORDER BY 
    SUM(total_incidents) DESC;
```


```sql
SELECT rg.Major, rg.Total AS Total_Undergrad_Grads, gs.Grad_total AS Total_Grad_Students, rg.Median AS Undergrad_Median_Salary, gs.Grad_median AS Grad_Median_Salary FROM RecentGradsDB rg JOIN GradStudentsDB gs ON rg.Major_code = gs.Major_code WHERE rg.Total > 10000 ORDER BY rg.Total DESC, rg.Median DESC LIMIT 5;
```

```sql
SELECT Major, 'Undergrad' AS Degree_Level, Total AS Total_Students FROM RecentGradsDB WHERE Total > 5000 UNION SELECT Major, 'Graduate' AS Degree_Level, Grad_total AS Total_Students FROM GradStudentsDB WHERE Grad_total > 5000 ORDER BY Total_Students DESC;
```


```sql
SELECT 
                rg.Major, 
                rg.Employed AS Undergrad_Employed, 
                gs.Grad_employed AS Grad_Employed,
                rg.Unemployment_rate AS Undergrad_Unemployment_Rate,
                gs.Grad_unemployment_rate AS Grad_Unemployment_Rate,
                (gs.Grad_median - rg.Median) AS Salary_Premium
            FROM RecentGradsDB rg
            JOIN GradStudentsDB gs
                ON rg.Major_code = gs.Major_code
            WHERE rg.Unemployment_rate < 0.05  -- High undergraduate employment rate
              AND gs.Grad_unemployment_rate < 0.05  -- High graduate employment rate
            ORDER BY Salary_Premium DESC;
```

```

```sql

        SELECT 
            rg.Major, 
            rg.Employed AS Undergrad_Employed, 
            gs.Grad_employed AS Grad_Employed,
            rg.Unemployment_rate AS Undergrad_Unemployment_Rate,
            gs.Grad_unemployment_rate AS Grad_Unemployment_Rate,
            (gs.Grad_median - rg.Median) AS Salary_Premium
        FROM RecentGradsDB rg
        JOIN GradStudentsDB gs ON rg.Major_code = gs.Major_code
        WHERE rg.Unemployment_rate < 0.05  
          AND gs.Grad_unemployment_rate < 0.05  
        ORDER BY Salary_Premium DESC
        LIMIT 5;
    
```

