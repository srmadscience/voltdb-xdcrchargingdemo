select * from user_table where userid = 2;

select * from user_usage_table where userid = 2;

delete from user_recent_transactions where userid = 2 and amount = 0 ;

select * from user_recent_transactions where userid = 2 and amount  >  0 order by userid ;

select sum(amount) from user_recent_transactions where userid = 2 and amount > 0  ;


select * from user_recent_transactions where userid = 2 and amount  <  0 order by userid ;

select sum(amount) from user_recent_transactions where userid = 2 and amount < 0  ;


select sum(amount) from user_recent_transactions where userid = 2 ;
