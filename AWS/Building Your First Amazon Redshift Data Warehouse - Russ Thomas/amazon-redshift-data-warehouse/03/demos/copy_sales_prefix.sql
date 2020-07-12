

  copy factsales
  from 's3://redshift-load-queue/sales' 
  iam_role '' 
  region ''
  delimiter '|'
  gzip
  
