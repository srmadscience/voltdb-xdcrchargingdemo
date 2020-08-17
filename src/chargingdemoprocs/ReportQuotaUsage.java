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

public class ReportQuotaUsage extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt("SELECT userid FROM user_table WHERE userid = ?;");

	public static final SQLStmt getTxn = new SQLStmt("SELECT txn_time FROM user_recent_transactions "
			+ "WHERE userid = ? AND user_txn_id = ? AND clusterid = ?;");
    
    public static final SQLStmt migrateOldZeroTxns = new SQLStmt("MIGRATE FROM user_recent_transactions "
    		+ "WHERE userid = ? AND txn_time < DATEADD(MINUTE,?,NOW) AND amount = 0 AND clusterid = ? AND NOT MIGRATING();");
    
	public static final SQLStmt addTxn = new SQLStmt("INSERT INTO user_recent_transactions "
			+ "(userid, user_txn_id, txn_time, productid, amount, clusterid) " + "VALUES (?,?,NOW,?,?,?);");

//    public static final SQLStmt getBalance = new SQLStmt("SELECT balance, CAST (? AS BIGINT) product_id,"
//        + " CAST (? AS BIGINT) session_id, userid FROM user_balances WHERE userid = ?;");

//    public static final SQLStmt getRemainingCredit
//    = new SQLStmt("select v.userid, v.balance - sum(uut.allocated_units * p.unit_cost )  balance "
//               + "from  user_balances v " 
//               + ", user_usage_table uut "
//               + ", product_table p "
//               + "where v.userid = ? "
//               + "and   v.userid = uut.userid "
//               + "and   p.productid = uut.productid "
//               + "group by v.userid, v.balance;");

	public static final SQLStmt getValidatedBalance = new SQLStmt(
			"select uc.validated_balance from  user_clusters uc " + "where uc.userid  = ? AND uc.clusterid = ?;");

	public static final SQLStmt getSpendingHistory = new SQLStmt(
			"select ut.userid, uc.validated_balance, sum(ut.amount) ut_amount " + "from user_recent_transactions ut  "
					+ "   , user_clusters uc " + "where ut.userid = ?  " + "and   ut.clusterid = ? "
					+ "and   ut.userid = uc.userid " + "and   ut.clusterid = uc.clusterid "
					+ "and ut.txn_time > uc.validated_balance_timestamp "
					+ "group by ut.userid, uc.validated_balance "
					+ "order by ut.userid, uc.validated_balance; ");

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


	public static final SQLStmt reportSpending = new SQLStmt(
			"INSERT INTO user_financial_events (userid   ,amount, purpose, clusterid)    VALUES (?,?,?,?);");

	// @formatter:on

	public VoltTable[] run(long userId, long productId, int unitsUsed, int unitsWanted, long inputSessionId,
			String txnId) throws VoltAbortException {

		// long currentBalance = 0;
		long unitCost = 0;
		long sessionId = inputSessionId;

		if (sessionId <= 0) {
			sessionId = this.getUniqueId();
		}

		voltQueueSQL(getUser, userId);
		voltQueueSQL(getProduct, productId);
		voltQueueSQL(getTxn, userId, txnId, this.getClusterId());

		VoltTable[] results = voltExecuteSQL();

		// Sanity check: Does this user exist?
		if (!results[0].advanceRow()) {
			throw new VoltAbortException("User " + userId + " does not exist");
		}

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

		long amountSpent = unitsUsed * unitCost * -1;

		if (unitsUsed > 0) {

			// Report spending...
			voltQueueSQL(reportSpending, userId, amountSpent, unitsUsed + " units of product " + productId,
					this.getClusterId());

			voltExecuteSQL();

		}

		// Delete allocation record for current product/session
		voltQueueSQL(deleteAllocation, userId, productId, sessionId, this.getClusterId());

		// Note that transaction is now 'official'
		voltQueueSQL(addTxn, userId, txnId, productId, amountSpent, this.getClusterId());
		
		// get balance
		voltQueueSQL(getValidatedBalance, userId, this.getClusterId());
		
		// Get history so we know current balance
		voltQueueSQL(getSpendingHistory, userId, this.getClusterId());

		// get allocated credit so we can see what we can spend...
		voltQueueSQL(getAllocatedCredit, userId);
		
		voltQueueSQL(migrateOldZeroTxns, userId, -2, this.getClusterId());
		
		

		final VoltTable[] interimResults = voltExecuteSQL();
		
		

		// Check the balance...
		interimResults[2].advanceRow();

		final long validatedBalance = interimResults[2].getLong("validated_balance");
	
	    long recentChanges = 0;
		
		if (interimResults[3].advanceRow()) {
			recentChanges = interimResults[3].getLong("ut_amount");
			
			if (interimResults[3].wasNull()) {
				recentChanges=0;
			}
		}
		
		long currentlyAllocatedCredit = 0;

		// If we have any reservations take their cost into account
		if (interimResults[4].advanceRow()) {
			currentlyAllocatedCredit = interimResults[4].getLong("allocated");
			
			if (interimResults[4].wasNull()) {
				currentlyAllocatedCredit=0;
			}
			
//			if (currentlyAllocatedCredit > 10000 || currentlyAllocatedCredit < 0) {
//				for (int i = 0; i < interimResults.length; i++) {
//					System.out.println(interimResults[i].toFormattedString());
//				}
//
//			}
		}

		
		final long availableCredit = validatedBalance + recentChanges - currentlyAllocatedCredit;

		// if unitsWanted is 0 or less then this transaction is finished...
		if (unitsWanted <= 0) {
			return interimResults;
		}

		long wantToSpend = unitCost * unitsWanted;

		// Calculate how much we can afford ..
		long whatWeCanAfford = Long.MAX_VALUE;

		if (unitCost > 0) {
			whatWeCanAfford = availableCredit / unitCost;
		}

		long unitsAllocated = 0;

		if (availableCredit <= 0 || whatWeCanAfford == 0) {

			this.setAppStatusString("Not enough money");
			this.setAppStatusCode(ReferenceData.STATUS_NO_MONEY);

		} else if (wantToSpend > availableCredit) {

			unitsAllocated = whatWeCanAfford;
			this.setAppStatusString("Allocated " + whatWeCanAfford + " units");
			this.setAppStatusCode(ReferenceData.STATUS_SOME_UNITS_ALLOCATED);
			voltQueueSQL(createAllocation, userId, productId, whatWeCanAfford, sessionId, this.getClusterId());

		} else {
			unitsAllocated = unitsWanted;
			this.setAppStatusString("Allocated " + unitsWanted + " units");
			this.setAppStatusCode(ReferenceData.STATUS_ALL_UNITS_ALLOCATED);
			voltQueueSQL(createAllocation, userId, productId, unitsWanted, sessionId, this.getClusterId());

		}

		voltExecuteSQL(true);

		VoltTable t = new VoltTable(new VoltTable.ColumnInfo("userid", VoltType.BIGINT),
				new VoltTable.ColumnInfo("productId", VoltType.BIGINT),
				new VoltTable.ColumnInfo("sessionId", VoltType.BIGINT),
				new VoltTable.ColumnInfo("clusterid", VoltType.BIGINT)
				,new VoltTable.ColumnInfo("availableCredit", VoltType.BIGINT),
				new VoltTable.ColumnInfo("allocated_units", VoltType.BIGINT));

		t.addRow(userId, productId, sessionId, this.getClusterId(), availableCredit,unitsAllocated);

		VoltTable[] resultsAsArray = { t };

		return resultsAsArray;
	}
}
