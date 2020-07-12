
create or replace function month_name(n integer)
returns varchar(3)

stable as $$

if n==1:    return 'Jan'
elif n==2:  return 'Feb'
elif n==3:  return 'Mar'
elif n==4:  return 'Apr'
elif n==5:  return 'May'
elif n==6:  return 'Jun'
elif n==7:  return 'Jul'
elif n==8:  return 'Aug'
elif n==9:  return 'Sep'
elif n==10: return 'Oct'
elif n==11: return 'Nov'
elif n==12: return 'Dec'
    
$$ language plpythonu;



