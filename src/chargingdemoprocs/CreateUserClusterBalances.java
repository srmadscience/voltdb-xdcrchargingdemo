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
import org.voltdb.types.TimestampType;

public class CreateUserClusterBalances extends VoltProcedure {

	// @formatter:off

	public static final long TIMEOUT_MS = 300000;

	public static final SQLStmt findMissingRecords = new SQLStmt(
			"SELECT userid, balance_timestamp " + "FROM max_user_cluster_balances " + "WHERE clusterid = ? "
					+ "AND balance_timestamp < TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW)) "
					+ "ORDER BY balance_timestamp,userid, clusterid LIMIT 1000;");
	
	public static final SQLStmt findRefData = new SQLStmt(
			"SELECT userid, validated_balance_timestamp, validated_balance " + "FROM user_clusters "
					+ "WHERE userid = ?  " + "AND   clusterid = ? ;");

	public static final SQLStmt getRawBalance = new SQLStmt(
			"select ut.userid, sum(ut.amount) validated_balance "
					+ "from user_recent_transactions ut  " 
					+ "where ut.userid = ?  "
					+ "and ut.txn_time >  ? "
					+ "and ut.txn_time <= TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW))"
					+ "group by ut.userid "
					+ "order by ut.userid; ");

	public static final SQLStmt insertUserClusterBalance = new SQLStmt("INSERT INTO user_cluster_balances "
			+ "(userid, clusterid,user_balance ,balance_timestamp) VALUES (?,?,?, TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW)));");
	
	  public static final SQLStmt getPriorMinuteCount = new SQLStmt("SELECT user_balance FROM user_cluster_balances "
	    		+ " WHERE clusterid = ? AND userid = ? AND balance_timestamp = TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW))"
	    		+ " GROUP BY user_balance ORDER BY user_balance;");
	 
		public static final SQLStmt updateCluster = new SQLStmt("UPDATE user_clusters "
				+ "SET validated_balance = ?, validated_balance_timestamp = TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW)) WHERE clusterid = ? AND userid = ?;");
		
	    public static final SQLStmt migrateOldTxns = new SQLStmt("MIGRATE FROM user_recent_transactions "
	    		+ "WHERE userid = ? AND clusterid = ? AND txn_time < DATEADD(MINUTE,?,NOW)  AND NOT MIGRATING();");

		public static final SQLStmt deleteUserClusterBalance = new SQLStmt("DELETE FROM user_cluster_balances "
				+ " WHERE userid = ? AND clusterid = ? AND balance_timestamp < TRUNCATE(MINUTE, DATEADD( MINUTE, ?, NOW));");

//	public static final SQLStmt getClusters = new SQLStmt("SELECT * FROM cluster_table ORDER BY cluster_id;");
//
//	public static final SQLStmt findOtherClusterDates = new SQLStmt(
//			"select ucm.userid, ucm.balance_timestamp, ucm.user_balance, count(*) "
//					+ "from user_cluster_balances  ucm " + "   , user_clusters          uc "
//					+ "where ucm.userid = uc.userid " + "and   uc.clusterid = ? "
//					+ "and   uc.VALIDATED_BALANCE_TIMESTAMP < ucm.balance_timestamp "
//					+ "group by ucm.userid, ucm.balance_timestamp, ucm.user_balance " + "having count(*) = ? "
//					+ "order by ucm.balance_timestamp desc, ucm.userid " + "limit 1000 ; ");

//    public static final SQLStmt updateCluster = new SQLStmt("update user_clusters uc "
//    		+ "SET validated_balance = ? "
//    		+ "   , validated_balance_timestamp = TRUNCATE(HOUR, DATEADD( MINUTE, ?, NOW)) "
//    		+ "where uc.userid = ?  "
//    		+ "and   uc.clusterid = ?;");
//    

//	public static final SQLStmt updateCluster = new SQLStmt("UPDATE user_clusters "
//			+ "SET validated_balance = ?, validated_balance_timestamp = ? WHERE clusterid = ? AND userid = ?;");

	// @formatter:on

	public VoltTable[] run(long graceMinutes) throws VoltAbortException {

		// Housekeeping: Delete allocations for this user that are older than
		// TIMEOUT_MS
		voltQueueSQL(findMissingRecords, this.getClusterId(), -1 * graceMinutes);

 
		VoltTable[] missingRecords = voltExecuteSQL();
	       
		while (missingRecords[0].advanceRow()) {

			long userid = missingRecords[0].getLong("userid");

			voltQueueSQL(findRefData, userid, this.getClusterId());
			VoltTable[] refDataResult = voltExecuteSQL();
			refDataResult[0].advanceRow();
			
			long validatedBalance = refDataResult[0].getLong("validated_balance");
			
			TimestampType refDate = refDataResult[0].getTimestampAsTimestamp("validated_balance_timestamp");
		
			voltQueueSQL(getRawBalance, userid, refDate, -1 * graceMinutes);

			VoltTable[] userStats = voltExecuteSQL();

			if (userStats[0].advanceRow()) {
				validatedBalance += userStats[0].getLong("validated_balance");
			}

			voltQueueSQL(insertUserClusterBalance, userid, this.getClusterId(), validatedBalance, -1 * graceMinutes);
			voltQueueSQL(getPriorMinuteCount, this.getClusterId(), userid, -1 * ( 1 + graceMinutes));	
			
			VoltTable[] clusterCreateResult = voltExecuteSQL();
			

			if (clusterCreateResult[1].getRowCount() == 1) {
				// All rows for minute -2 agree on balance...
				clusterCreateResult[1].advanceRow();
				validatedBalance = clusterCreateResult[1].getLong("USER_BALANCE");
				voltQueueSQL(updateCluster,  validatedBalance, -1 * ( 1 + graceMinutes), this.getClusterId(), userid);	
				voltQueueSQL(migrateOldTxns,  userid , this.getClusterId(), -1 * ( 5 + graceMinutes));	
				voltQueueSQL(deleteUserClusterBalance,  userid , this.getClusterId(), -1 * ( 10 + graceMinutes));	
				
				
				
				voltExecuteSQL();
			}
		}

	
		VoltTable t = new VoltTable(new VoltTable.ColumnInfo("user_cluster_balances_created", VoltType.BIGINT));

		t.addRow(missingRecords[0].getRowCount());

		VoltTable[] resultsAsArray = { t };
		
		return resultsAsArray;

	}
}
