

--create new schema
create schema secure;

-- create two tables within the schema
create table secure.accountnum
(accountkey int, 
 accountnum varchar(10), 
 accountloc varchar(25), 
 payoutcode varchar(15));
 
create table secure.accountholder
(accountkey int, 
 account_name varchar(50), 
 account_contact varchar(25));

 
-- grant usage to finance group
grant usage on schema "secure" to group finance;
 
-- grant select to all tables in group
grant select on all tables in schema "secure" to group finance;
 
-- alter default privileges so finance can select on future tables 
alter default privileges in schema "secure" grant select on tables to group finance;
 