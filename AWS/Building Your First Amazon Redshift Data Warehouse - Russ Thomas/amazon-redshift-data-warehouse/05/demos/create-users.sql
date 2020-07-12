
-- list of finance users
create user jeffhandy with password 'ChangeLat3r';
create user jillclever with password 'ChangeLat3r';
create user kimmathy with password 'ChangeLat3r';
create user financeapp with password '20jfaslJWEO01nOOWkjsf2';

-- create a group for finance users
create group finance;

-- add users to the group
alter group finance add user jeffhandy;
alter group finance add user jillclever;
alter group finance add user kimmathy;
alter group finance add user financeapp;

-- grant privleges
grant select on table dimproduct to group finance;
grant select on table factsales to group finance;

grant update on table factsales to financeapp;



 
-- test new rights
set session authorization 'jeffhandy';
select * from dimproduct limit 10;



