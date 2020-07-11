

SELECT  
 	pg_group.groname 
	,pg_group.grosysid 
	,pg_user.* 
FROM pg_group, pg_user  
WHERE pg_user.usesysid = ANY(pg_group.grolist)
