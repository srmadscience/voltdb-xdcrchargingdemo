drop index mucb_idx1 ;
create index mucb_idx1 on max_user_cluster_balances(clusterid, balance_timestamp, userid);

