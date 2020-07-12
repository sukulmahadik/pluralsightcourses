

   copy dimdate
   from 's3://redshift-load-queue/dimdate.json'
   region 'us-west-2'
   iam_role ''
   json as 'auto'
