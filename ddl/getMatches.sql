select ucm.userid, ucm.balance_timestamp, ucm.user_balance, count(*)
from user_cluster_balances  ucm
   , user_clusters          uc
where ucm.userid = uc.userid
and   uc.clusterid = 4
and   uc.VALIDATED_BALANCE_TIMESTAMP < ucm.balance_timestamp
group by ucm.userid, ucm.balance_timestamp, ucm.user_balance
having count(*) = 3
order by ucm.balance_timestamp desc, ucm.userid
limit 1000 ;
