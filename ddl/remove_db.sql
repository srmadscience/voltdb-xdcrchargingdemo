DROP TASK PurgeWrangler IF EXISTS;

DROP procedure showTransactions if exists;
DROP procedure FindByLoyaltyCard; 
DROP PROCEDURE ShowCurrentAllocations__promBL IF EXISTS;
DROP PROCEDURE GetUser IF EXISTS;
DROP PROCEDURE GetAndLockUser IF EXISTS;
DROP PROCEDURE UpdateLockedUser IF EXISTS;
DROP PROCEDURE UpsertUser IF EXISTS;
DROP PROCEDURE DelUser IF EXISTS;
DROP PROCEDURE ReportQuotaUsage IF EXISTS;  
DROP PROCEDURE AddCredit IF EXISTS;  
DROP PROCEDURE DeleteStaleAllocations IF EXISTS;

DROP view cluster_activity_by_users if exists; 
DROP view cluster_activity if exists; 
DROP view last_cluster_activity if exists; 
DROP view cluster_users if exists; 
DROP view allocated_by_product if exists;

DROP TABLE cluster_table if exists;
DROP table product_table if exists;
DROP table user_table if exists;
DROP table user_recent_transactions if exists;
DROP table user_usage_table if exists;
DROP STREAM user_addcredit_events  if exists;

  


