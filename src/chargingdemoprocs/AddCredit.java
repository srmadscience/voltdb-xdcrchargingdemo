package chargingdemoprocs;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

public class AddCredit extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt("SELECT userid, user_validated_balance, user_validated_balance_timestamp "
			+ "FROM user_table WHERE userid = ?;");
	
	public static final SQLStmt getTxn = new SQLStmt("SELECT txn_time FROM user_recent_transactions "
			+ "WHERE userid = ? " + "AND user_txn_id = ?" + "AND clusterid = ?;");

	public static final SQLStmt addTxn = new SQLStmt("INSERT INTO user_recent_transactions "
			+ "(userid, user_txn_id, txn_time,amount,clusterid,purpose) VALUES (?,?,NOW,?,?,?);");


	public static final SQLStmt getSpendingHistory = new SQLStmt(
			"select ut.userid, uc.user_validated_balance, sum(ut.amount) ut_amount " 
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

	// @formatter:on

	/**
	 * A VoltDB stored procedure to add credit to a user in the chargingdemo demo.
	 * It checks that the user exists and also makes sure that this transaction
	 * hasn't already happened.
	 * 
	 * @param userId
	 * @param extraCredit
	 * @param txnId
	 * @return Balance and Credit info
	 * @throws VoltAbortException
	 */
	public VoltTable[] run(long userId, long extraCredit, String txnId) throws VoltAbortException {

		// See if we know about this user and transaction...
		voltQueueSQL(getUser, userId);
		voltQueueSQL(getTxn, userId, txnId, this.getClusterId());

		VoltTable[] userAndTxn = voltExecuteSQL();

		// Sanity Check: Is this a real user?
		if (!userAndTxn[0].advanceRow()) {
			throw new VoltAbortException("User " + userId + " does not exist");
		}

		// Sanity Check: Has this transaction already happened?
		if (userAndTxn[1].advanceRow()) {

			this.setAppStatusCode(ReferenceData.TXN_ALREADY_HAPPENED);
			this.setAppStatusString(
					"Event already happened at " + userAndTxn[1].getTimestampAsTimestamp("txn_time").toString());

		} else {

			// Report credit add...

			this.setAppStatusCode(ReferenceData.CREDIT_ADDED);
			this.setAppStatusString(extraCredit + " added by Txn " + txnId);

			// Insert a row into the stream for each user's financial events.
			// The view user_balances can then calculate actual credit
			voltQueueSQL(addTxn, userId, txnId, extraCredit, this.getClusterId(),"Add Credit");


			// get user and validated balance
			voltQueueSQL(getUser, userId);

			// Get history so we know current balance
			voltQueueSQL(getSpendingHistory, userId);

			// get allocated credit so we can see what we can spend...
			voltQueueSQL(getAllocatedCredit, userId);

		}

		return voltExecuteSQL(true);
	}
}
