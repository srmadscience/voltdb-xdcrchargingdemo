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

public class ReportQuotaUsage extends AbstractChargingProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt("SELECT userid, user_validated_balance, user_validated_balance_timestamp, user_owning_cluster "
			+ "FROM user_table WHERE userid = ?;");

	public static final SQLStmt getTxn = new SQLStmt("SELECT txn_time FROM user_recent_transactions "
			+ "WHERE userid = ? AND user_txn_id = ? AND clusterid = ?;");
    
 	public static final SQLStmt addTxn = new SQLStmt("INSERT INTO user_recent_transactions "
			+ "(userid, user_txn_id, txn_time, productid, amount, clusterid,purpose) " + "VALUES (?,?,NOW,?,?,?,?);");

 	public static final SQLStmt updTxn = new SQLStmt("UPDATE user_recent_transactions "
			+ "SET purpose = ? WHERE userid = ? AND user_txn_id = ? AND clusterid = ?;");

	public static final SQLStmt getSpendingHistory = new SQLStmt(
			"select ut.userid, uc.user_validated_balance, sum(ut.amount) ut_amount, count(*) how_many " 
					+ "from user_recent_transactions ut  "
					+ "   , user_table uc " 
					+ "where ut.userid = ?  " 
					+ "and   ut.userid = uc.userid " 
					+ "and ut.txn_time > uc.user_validated_balance_timestamp "
					+ "group by ut.userid, uc.user_validated_balance "
					+ "order by  ut.userid, uc.user_validated_balance; ");

	public static final SQLStmt getAllocatedCredit = new SQLStmt(
			"select sum(uut.allocated_units * p.unit_cost )  allocated " + "from user_usage_table uut "
					+ "   , product_table p " + "where uut.userid = ? " + "and   p.productid = uut.productid;");

	public static final SQLStmt getProduct = new SQLStmt("SELECT unit_cost FROM product_table WHERE productid = ?;");

	public static final SQLStmt createAllocation = new SQLStmt("INSERT INTO user_usage_table "
			+ "(userid, productid, allocated_units,sessionid, lastdate, clusterid) VALUES (?,?,?,?,NOW,?);");

	public static final SQLStmt getCurrentAllocation = new SQLStmt(
			"SELECT allocated_units, sessionid, lastdate, userid, productid " + "FROM user_usage_table "
					+ "WHERE userid = ? AND productid = ? AND sessionid = ?" + "AND clusterid = ?;");

	public static final SQLStmt deleteAllocation = new SQLStmt(
			"DELETE FROM user_usage_table " + "WHERE userid = ? AND productid = ? AND sessionid = ? AND clusterid = ?");

	public static final SQLStmt deleteStaleAllocations = new SQLStmt(
			"DELETE FROM user_usage_table WHERE userid = ? AND lastdate <  DATEADD( MINUTE, ?, NOW)");


	// @formatter:on

	public VoltTable[] run(long userId, long productId, int unitsUsed, int unitsWanted, long inputSessionId,
			String txnId) throws VoltAbortException {

		// long currentBalance = 0;
		long unitCost = 0;
		long sessionId = inputSessionId;
		long userClusterId = -1;
		String decision = "Report prior usage";

		if (sessionId <= 0) {
			sessionId = this.getUniqueId();
		}

		voltQueueSQL(getUser, userId);
		voltQueueSQL(getProduct, productId);
		voltQueueSQL(getTxn, userId, txnId, this.getClusterId());
		voltQueueSQL(getCluster, this.getClusterId());
		voltQueueSQL(deleteStaleAllocations,userId, -1  * DeleteStaleAllocations.ALLOCATION_TIMEOUT_SECONDS );


		VoltTable[] results = voltExecuteSQL();

		// Sanity check: Does this user exist?
		if (!results[0].advanceRow()) {
			throw new VoltAbortException("User " + userId + " does not exist");
		}

		
		userClusterId = results[0].getLong("user_owning_cluster");
		
		// Sanity Check: Does this product exist?
		if (!results[1].advanceRow()) {
			throw new VoltAbortException("Product " + productId + " does not exist");
		} else {
			unitCost = results[1].getLong("UNIT_COST");
		}

	// Sanity Check: Is this a re-send of a transaction we've already done?
		if (results[2].advanceRow()) {
			this.setAppStatusCode(ReferenceData.TXN_ALREADY_HAPPENED);
			this.setAppStatusString(
					"Event already happened at " + results[2].getTimestampAsTimestamp("txn_time").toString());
			return voltExecuteSQL(true);
		}
		
		// Find out how long we wait before purging transactions
		results[3].advanceRow();
	
		
		byte clusterPurgeMinutes = (byte)results[3].getLong("cluster_purge_minutes");
	
		if (results[3].wasNull()) {
			clusterPurgeMinutes = DEFAULT_CLUSTER_PURGE_MINUTES;
		}

		long amountSpent = unitsUsed * unitCost * -1;
		String oldDecision = "Spent " + amountSpent;

		// Delete allocation record for current product/session
		voltQueueSQL(deleteAllocation, userId, productId, sessionId, this.getClusterId());

		// Note that transaction is now 'official'
		voltQueueSQL(addTxn, userId, txnId, productId, amountSpent, this.getClusterId(), oldDecision);
		
		// Get history so we know current balance
		voltQueueSQL(getSpendingHistory, userId);

		// get allocated credit so we can see what we can spend...
		voltQueueSQL(getAllocatedCredit, userId);
		
		final VoltTable[] interimResults = voltExecuteSQL();	
	
		// if unitsWanted is 0 or less then this transaction is finished...
		if (unitsWanted <= 0) {
			return interimResults;
		}

		// Check the balance...
		final long validatedBalance = results[0].getLong("user_validated_balance");
	
	    long recentChanges = 0;
		
		if (interimResults[2].advanceRow()) {
			recentChanges = interimResults[2].getLong("ut_amount");
			
			if (interimResults[2].wasNull()) {
				recentChanges=0;
			}
		}
		
		long currentlyAllocatedCredit = 0;

		// If we have any reservations take their cost into account
		if (interimResults[3].advanceRow()) {
			currentlyAllocatedCredit = interimResults[3].getLong("allocated");
			
			if (interimResults[3].wasNull()) {
				currentlyAllocatedCredit=0;
			}
			
		}

		final long availableCredit = validatedBalance + recentChanges - currentlyAllocatedCredit;


		long wantToSpend = unitCost * unitsWanted;

		// Calculate how much we can afford ..
		long qtyWeCanAfford = Long.MAX_VALUE;

		if (unitCost > 0) {
			qtyWeCanAfford = availableCredit / unitCost;
		}

		long unitsAllocated = 0;

		if (availableCredit < 0 ) {

			decision = "Negative balance: " + availableCredit;
			this.setAppStatusCode(ReferenceData.STATUS_NO_MONEY);

		} else if (qtyWeCanAfford == 0) {

			decision = "Not enough money for request. Unit cost is " + unitCost;
			this.setAppStatusCode(ReferenceData.STATUS_NO_MONEY);

		} else  if (wantToSpend > availableCredit) {

			unitsAllocated = qtyWeCanAfford;
			decision = "Allocated " + qtyWeCanAfford + " units of " + unitsWanted + " requested @" + unitCost;
			this.setAppStatusCode(ReferenceData.STATUS_SOME_UNITS_ALLOCATED);
			voltQueueSQL(createAllocation, userId, productId, qtyWeCanAfford, sessionId, this.getClusterId());

		} else {
			unitsAllocated = unitsWanted;
			decision = "Allocated " + unitsWanted + " units @" + unitCost;
			this.setAppStatusCode(ReferenceData.STATUS_ALL_UNITS_ALLOCATED);
			voltQueueSQL(createAllocation, userId, productId, unitsWanted, sessionId, this.getClusterId());

		}

		this.setAppStatusString(decision);
		voltQueueSQL(updTxn, oldDecision + ";bal=" + (validatedBalance + recentChanges)+ "/" + currentlyAllocatedCredit + ";" + decision, userId, txnId, this.getClusterId());
		voltExecuteSQL();

		
		int recordsMigrated = 0;
			
		if (userClusterId == this.getClusterId()) {
			recordsMigrated = cleanupTransactions(userId, clusterPurgeMinutes);
		}
		
		VoltTable t = new VoltTable(new VoltTable.ColumnInfo("userid", VoltType.BIGINT),
				new VoltTable.ColumnInfo("productId", VoltType.BIGINT),
				new VoltTable.ColumnInfo("sessionId", VoltType.BIGINT),
				new VoltTable.ColumnInfo("clusterid", VoltType.BIGINT)
				,new VoltTable.ColumnInfo("availableCredit", VoltType.BIGINT),
				new VoltTable.ColumnInfo("allocated_units", VoltType.BIGINT),
				new VoltTable.ColumnInfo("recordsMigrated", VoltType.BIGINT));

		t.addRow(userId, productId, sessionId, this.getClusterId(), availableCredit,unitsAllocated,recordsMigrated);

		VoltTable[] resultsAsArray = { t };

		return resultsAsArray;
	}
}
