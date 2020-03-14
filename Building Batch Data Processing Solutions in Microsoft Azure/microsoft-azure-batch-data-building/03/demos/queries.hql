SELECT * FROM delays LIMIT 5




INSERT OVERWRITE DIRECTORY '/tutorials/flightdelays/output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT regexp_replace(origin_city_name, '''', ''),
    avg(weather_delay)
FROM delays
WHERE weather_delay IS NOT NULL
GROUP BY origin_city_name;

// SSH and Sqoop export
ssh admin@testhdi999.azurehdinsight.net

sqoop list-databases --connect jdbc:sqlserver://azsql704.database.windows.net:1433 --username tim -P

sqoop export --connect "jdbc:sqlserver://azsql704.database.windows.net:1433;database=psazsqldb704" --username tim -P --table 'delays' --export-dir '/tutorials/flightdelays/output' --fields-terminated-by '\t' -m 1
