

  copy dimproduct
  from 's3://redshift-load-queue/dimproduct.csv' 
  iam_role '' 
  region ''
  format csv
  delimiter ','
  
