select * from user_table where userid = 5857;

select * from user_usage_table where userid = 5857;

select sum(uut.allocated_units * p.unit_cost )  allocated    from user_usage_table uut 
					     , product_table p    where uut.userid = 5857    and   p.productid = uut.productid;

select * from user_recent_transactions where userid = 5857 order by userid , txn_time;

