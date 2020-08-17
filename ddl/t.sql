CREATE PROCEDURE DIRECTED
   FROM CLASS chargingdemoprocs.CreateUserClusterBalances;  
   
CREATE TASK CreateClusterBalancesTask
ON SCHEDULE DELAY 1 SECONDS
PROCEDURE CreateUserClusterBalances
WITH (15)
 ON ERROR LOG
RUN ON PARTITIONS;

