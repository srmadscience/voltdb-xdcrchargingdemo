
load classes ../lib/gson-2.7.jar;
load classes ../jars/voltdb-chargingdemo.jar;

file -inlinebatch END_OF_BATCH


CREATE TABLE cluster_table
(cluster_id tinyint not null primary key
,cluster_name varchar(50) not null
,watched_by_cluster_id tinyint not null 
,cluster_purge_minutes tinyint default 3 not null  );

DR TABLE cluster_table;

CREATE table product_table
(productid bigint not null primary key
,productname varchar(50) not null
,unit_cost bigint not null);

DR TABLE product_table;

CREATE table user_table
(userid bigint not null primary key
,user_json_object varchar(30000)
,user_last_seen TIMESTAMP DEFAULT NOW
,user_softlock_sessionid bigint 
,user_softlock_expiry TIMESTAMP
,user_owning_cluster TINYINT NOT NULL
,user_validated_balance BIGINT DEFAULT 0
,user_validated_balance_timestamp TIMESTAMP  DEFAULT NOW);

create index ut_del on user_table(user_last_seen);

create index ut_clu on user_table(user_owning_cluster);

create index ut_loyaltycard on user_table (field(user_json_object, 'loyaltySchemeNumber'));

PARTITION TABLE user_table ON COLUMN userid;

DR table user_table;



create table user_recent_transactions
 MIGRATE TO TARGET user_transactions
(userid bigint not null 
,user_txn_id varchar(128) NOT NULL
,replace_user_txn_id varchar(128) 
,clusterid tinyint NOT NULL
,txn_time TIMESTAMP DEFAULT NOW  not null 
,productid bigint
,amount bigint 
,purpose  varchar(128)
,primary key (userid, user_txn_id,clusterid));

PARTITION TABLE user_recent_transactions ON COLUMN userid;

CREATE INDEX urt_del_idx ON user_recent_transactions(userid, txn_time) ;

CREATE INDEX urt_del_idx2 ON user_recent_transactions(userid, txn_time)  WHERE NOT MIGRATING;

CREATE INDEX urt_del_idx3 ON user_recent_transactions(txn_time);

CREATE INDEX urt_del_idx4 ON user_recent_transactions(txn_time) WHERE NOT MIGRATING;

echo needed by last_cluster_activity
CREATE INDEX urt_del_idx5 ON user_recent_transactions(clusterid, txn_time );

DR table user_recent_transactions;

create table user_usage_table
 MIGRATE TO TARGET user_usage_table_stale_entries
(userid bigint not null
,productid bigint not null
,allocated_units bigint not null
,sessionid bigint  not null
,clusterid tinyint NOT NULL
,lastdate timestamp not null
,primary key (userid, productid,sessionid,clusterid));

CREATE INDEX uut_del_idx ON user_usage_table(lastdate,clusterid,userid, productid,sessionid);

CREATE INDEX uut_del_idx2 ON user_usage_table(userid,lastdate);

PARTITION TABLE user_usage_table ON COLUMN userid;

CREATE STREAM user_addcredit_events 
partition on column userid
export to target user_addcredit_events
(userid bigint not null 
,amount bigint not null
,user_txn_id varchar(128) not null
,message varchar(80) not null);


create view cluster_activity_by_users as 
select userid, clusterid, count(*) how_many
from user_recent_transactions
group by userid, clusterid;

create view cluster_activity as 
select clusterid, truncate(minute, txn_time) txn_time, count(*) how_many
from user_recent_transactions
group by clusterid, truncate(minute, txn_time) ;

create view last_cluster_activity as 
select clusterid, max(txn_time) txn_time
from user_recent_transactions
group by clusterid;

create view cluster_users as 
select user_owning_cluster, count(*) how_many
from user_table
group by user_owning_cluster;




create view allocated_by_product
as
select productid, count(*) how_many, sum(allocated_units) allocated_units
from user_usage_table
group by productid;




create procedure showTransactions
PARTITION ON TABLE user_table COLUMN userid
as 
select * from user_recent_transactions where userid = ? ORDER BY txn_time, user_txn_id;


create procedure FindByLoyaltyCard as select * from user_table where field(user_json_object, 'loyaltySchemeNumber') = CAST(? AS VARCHAR);

DROP  PROCEDURE ShowCurrentAllocations  IF EXISTS;

CREATE PROCEDURE ShowCurrentAllocations__promBL AS
BEGIN
select 'showCurrentAllocationsUnits' statname, 
       'units currently reserved' stathelp ,
p.productname
, a.allocated_units statvalue
from product_table p, allocated_by_product a
where p.productid = a.productid
order by p.productid;
select 'showCurrentAllocationsValues' statname, 
       'amounts currently reserved' stathelp ,
p.productname, a.allocated_units * p.unit_cost statvalue
from product_table p, allocated_by_product a
where p.productid = a.productid
order by p.productid;
select 'usersbycluster' statname,  'usersbycluster' stathelp , user_owning_cluster ,how_many statvalue from cluster_users;
END;


CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.GetUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.GetAndLockUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.UpdateLockedUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.UpsertUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.DelUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.ReportQuotaUsage;  
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.AddCredit;  

DROP PROCEDURE DeleteStaleAllocations IF EXISTS;
  
CREATE PROCEDURE DIRECTED
   FROM CLASS chargingdemoprocs.DeleteStaleAllocations;  
   
DROP TASK PurgeWrangler IF EXISTS;

CREATE TASK PurgeWrangler  FROM CLASS chargingdemotasks.PurgeWrangler WITH (10,30000) ON ERROR LOG RUN ON PARTITIONS DISABLE;

END_OF_BATCH
