select * from user_table where userid = 2;

select * from user_usage_table where userid = 2;

select * from max_user_cluster_balances where userid = 2;

select * from user_clusters where userid = 2 order by userid;

select * from user_cluster_balances where userid = 2 order by userid,BALANCE_TIMESTAMP , clusterid;

delete from user_recent_transactions where userid = 2 and amount = 0 ;

select * from user_recent_transactions where userid = 2 and amount  >  0 order by userid ;

select sum(amount) from user_recent_transactions where userid = 2 and amount > 0  ;


select * from user_recent_transactions where userid = 2 and amount  <  0 order by userid ;

select sum(amount) from user_recent_transactions where userid = 2 and amount < 0  ;


select sum(amount) from user_recent_transactions where userid = 2 ;
