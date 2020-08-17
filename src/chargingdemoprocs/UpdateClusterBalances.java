package chargingdemoprocs;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class UpdateClusterBalances extends VoltProcedure {

  // @formatter:off

    public static final long TIMEOUT_MS = 300000;    
        
    public static final SQLStmt findMissingRecords = new SQLStmt("SELECT userid, balance_timestamp "
    		+ "FROM max_user_cluster_balances "
            + "WHERE clusterid = ? "
            + "AND balance_timestamp < TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW)) "
            + "ORDER BY balance_timestamp,userid, clusterid LIMIT 100;");
    
	public static final SQLStmt getClusters = new SQLStmt("SELECT * FROM cluster_table ORDER BY cluster_id;");

        
    public static final SQLStmt findRefData = new SQLStmt("SELECT userid, validated_balance_timestamp, validated_balance "
    		+ "FROM user_clusters "
            + "WHERE userid = ?  "
            + "AND   clusterid = ? ;");
 
    public static final SQLStmt getRawBalance = new SQLStmt(
    		"select ut.userid, uc.validated_balance + sum(ut.amount) validated_balance "
    		+ "from user_recent_transactions ut  "
    		+ "   , user_clusters uc "
    		+ "where ut.userid = ?  "
    	//	+ "and   ut.clusterid = ? "
    		+ "and   ut.userid = uc.userid "
    		+ "and   ut.clusterid = uc.clusterid "
    		+ "and ut.txn_time > uc.validated_balance_timestamp "
    		+ "and ut.txn_time <= TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW))"
    		+ "group by ut.userid, uc.validated_balance,ut.clusterid "
    		+ "order by ut.userid, uc.validated_balance,ut.clusterid; ");
    
//    public static final SQLStmt updateCluster = new SQLStmt("update user_clusters uc "
//    		+ "SET validated_balance = ? "
//    		+ "   , validated_balance_timestamp = TRUNCATE(HOUR, DATEADD( MINUTE, ?, NOW)) "
//    		+ "where uc.userid = ?  "
//    		+ "and   uc.clusterid = ?;");
//    
    public static final SQLStmt insertUserClusterBalance = new SQLStmt("INSERT INTO user_cluster_balances "
    		+ "(userid, clusterid,user_balance ,balance_timestamp) VALUES (?,?,?, TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW)));");
 
    
       
    // @formatter:on

    public VoltTable[] run(long graceMinutes) throws VoltAbortException {

        // Housekeeping: Delete allocations for this user that are older than
        // TIMEOUT_MS
        voltQueueSQL(findMissingRecords, this.getClusterId(), -1 * graceMinutes);
		voltQueueSQL(getClusters);

        VoltTable[] missingRecords = voltExecuteSQL();
        final byte clusterSize = (byte) missingRecords[1].getRowCount();
        
        
        while (missingRecords[0].advanceRow()) {

            long userid = missingRecords[0].getLong("userid");
            
            voltQueueSQL(findRefData, userid, this.getClusterId());
            voltQueueSQL(getRawBalance, userid, -1 * graceMinutes);
          
            VoltTable[] userStats = voltExecuteSQL();
            
            userStats[0].advanceRow();
            
            long validatedBalance = userStats[0].getLong("validated_balance");
            
            if (userStats[1].advanceRow()) {
            	validatedBalance = userStats[1].getLong("validated_balance");
            }
                     
            System.out.println("Creating UpdateClusterBalances " + userid + " " +  this.getClusterId()+ " " + validatedBalance+ " " + -1 * graceMinutes );
            voltQueueSQL(insertUserClusterBalance, userid, this.getClusterId(), validatedBalance, -1 * graceMinutes );    
            voltExecuteSQL();
            
            // See if we can move clock forward by seeing how many records exist for graceMinutes - 1...
            
            //clusterSize
        }

        
		VoltTable t = new VoltTable(new VoltTable.ColumnInfo("rows_processed", VoltType.BIGINT));

		t.addRow(missingRecords[0].getRowCount());

		VoltTable[] resultsAsArray = { t };

		return resultsAsArray;

    }
}
