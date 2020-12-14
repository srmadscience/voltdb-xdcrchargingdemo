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
import org.voltdb.types.TimestampType;

/**
 * This is an abstract base class used by procedures that need to clean up old
 * transactions or move user ownership to a new cluster.
 *
 */
public class AbstractChargingProcedure extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getMigratableSpendingHistory = new SQLStmt(
			"select ut.userid, uc.user_validated_balance, uc.user_validated_balance_timestamp, max(ut.txn_time) txn_time, sum(ut.amount) ut_amount, count(*) how_many "
					+ "from user_recent_transactions ut  " + "   , user_table uc " + "where ut.userid = ?  "
					+ "and   ut.userid = uc.userid " + "and ut.txn_time > uc.user_validated_balance_timestamp "
					+ "and ut.txn_time <= DATEADD( MINUTE, ?, NOW) "
					+ "group by ut.userid, uc.user_validated_balance,uc.user_validated_balance_timestamp "
					+ "order by  ut.userid, uc.user_validated_balance,uc.user_validated_balance_timestamp; ");

//	public static final SQLStmt migrateOldTxns = new SQLStmt("MIGRATE FROM user_recent_transactions "
//			+ "WHERE userid = ? " + "AND txn_time >= ? " + "AND txn_time < ?  " + "AND NOT MIGRATING();"); // TODO

	public static final SQLStmt migrateOldTxns = new SQLStmt("DELETE FROM user_recent_transactions "
			+ "WHERE userid = ? " + "AND txn_time >= ? " + "AND txn_time < ?  " + "AND NOT MIGRATING();"); // TODO

	public static final SQLStmt updateUser = new SQLStmt("UPDATE user_table " + "SET user_validated_balance = ?"
			+ "  , user_validated_balance_timestamp = ? " + "WHERE userid = ? ;");

	public static final SQLStmt updateUserCluster = new SQLStmt(
			"UPDATE user_table " + "SET user_owning_cluster = ?" + "WHERE userid = ? ;");

	public static final SQLStmt getCluster = new SQLStmt("SELECT * FROM cluster_table WHERE cluster_id = ?;");

	public static final SQLStmt getClusterRankings = new SQLStmt(
			"select clusterid, how_many from cluster_activity_by_users where userid = ? order by how_many desc;");

	private static final int CLUSTER_MOVE_THRESHOLD = 10;

	public byte DEFAULT_CLUSTER_PURGE_MINUTES = 3;

	// @formatter:on

	/**
	 * 
	 * In this application a user's balance is the value in their user_table plus
	 * all the recent transactions. We do it this way to avoid directly updating a
	 * balance in an XDCR environment. Evey now and then we fold the older
	 * transactions for a user into their balance. We only do this to transactions
	 * that are older than 'minutesGracePeriod'. 
	 * 
	 * Note that the only cluster that can do this is the own that 'owns' the user.
	 * 
	 * @param userId
	 * @param minutesGracePeriod
	 * @return how many transactions we cleaned up.
	 */
	protected int cleanupTransactions(long userId, int minutesGracePeriod) {

		voltQueueSQL(getMigratableSpendingHistory, userId, -1 * minutesGracePeriod);
		VoltTable[] migrationCandidates = voltExecuteSQL();

		if (migrationCandidates[migrationCandidates.length - 1].getRowCount() == 0) {
			// no data for this user...
			return 0;
		}

		migrationCandidates[migrationCandidates.length - 1].resetRowPosition();
		migrationCandidates[migrationCandidates.length - 1].advanceRow();

		long validatedBalance = migrationCandidates[migrationCandidates.length - 1].getLong("user_validated_balance");
		long delta = migrationCandidates[migrationCandidates.length - 1].getLong("ut_amount");
		long howMany = migrationCandidates[migrationCandidates.length - 1].getLong("how_many");
		TimestampType validatedBalanceTime = migrationCandidates[migrationCandidates.length - 1]
				.getTimestampAsTimestamp("user_validated_balance_timestamp");
		TimestampType lastTxnTime = migrationCandidates[migrationCandidates.length - 1]
				.getTimestampAsTimestamp("txn_time");

		if (howMany < 5) {
			return 0;
		}

		voltQueueSQL(updateUser, (delta + validatedBalance), lastTxnTime, userId);
		voltQueueSQL(migrateOldTxns, userId, validatedBalanceTime, lastTxnTime);
		voltExecuteSQL();

		return (int) howMany;
	}

	/**
	 * In an XDCR deployment users can be updated by any cluster but are always 'owned' by a
	 * cluster for the purposes of cleaning up their transactions. This rountine hands ownership
	 * to another cluster if it's where all the transactions are happening.
	 * @param userId
	 * @return
	 */
	protected int changeClusterIfAppropriate(long userId) {

		voltQueueSQL(getClusterRankings, userId);
		VoltTable[] clusterRankingsResults = voltExecuteSQL();
		clusterRankingsResults[clusterRankingsResults.length - 1].advanceRow();

		final int busiestClusterId = (int) clusterRankingsResults[clusterRankingsResults.length - 1]
				.getLong("CLUSTERID");
		final int busiestClusterHowmany = (int) clusterRankingsResults[clusterRankingsResults.length - 1]
				.getLong("HOW_MANY");

		if (busiestClusterId != this.getClusterId() && busiestClusterHowmany > CLUSTER_MOVE_THRESHOLD) {
			voltQueueSQL(updateUserCluster, busiestClusterId, userId);
			voltExecuteSQL();
			return busiestClusterId;
		}

		return this.getClusterId();
	}

}
