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

public class UpdateUserCluster extends VoltProcedure {

	// @formatter:off



	public static final SQLStmt getClusters = new SQLStmt("SELECT * FROM cluster_table ORDER BY cluster_id;");

	public static final SQLStmt findOtherClusterDates = new SQLStmt(
			"select ucm.userid, ucm.balance_timestamp, ucm.user_balance, count(*) "
					+ "from user_cluster_balances  ucm " + "   , user_clusters          uc "
					+ "where ucm.userid = uc.userid " 
					+ "and   uc.clusterid = ? "
					+ "and   uc.clusterid = ucm.clusterid "
					+ "and   uc.VALIDATED_BALANCE_TIMESTAMP < ucm.balance_timestamp "
					+ "group by ucm.userid, ucm.balance_timestamp, ucm.user_balance " + "having count(*) = ? "
					+ "order by ucm.balance_timestamp desc, ucm.userid " + "limit 1000 ; ");

	public static final SQLStmt updateCluster = new SQLStmt("UPDATE user_clusters "
			+ "SET validated_balance = ?, validated_balance_timestamp = ? WHERE clusterid = ? AND userid = ?;");

	// @formatter:on

	public VoltTable[] run() throws VoltAbortException {

		voltQueueSQL(getClusters);
		VoltTable[] clusterList = voltExecuteSQL();
		byte clusterCount = (byte) clusterList[0].getRowCount();

		voltQueueSQL(findOtherClusterDates, this.getClusterId(), clusterCount);
		VoltTable[] candidatesForChange = voltExecuteSQL();

		TimestampType maxTime = null;

		
		System.out.println(candidatesForChange[0].getRowCount() + " candidates found for cluster " + this.getClusterId());
		
		while (candidatesForChange[0].advanceRow()) {

			if (maxTime == null) {
				maxTime = candidatesForChange[0].getTimestampAsTimestamp("balance_timestamp");
				System.out.println("set max date to " + maxTime.toString());
			}

			long userId = candidatesForChange[0].getLong("userid");

			if (candidatesForChange[0].getTimestampAsTimestamp("balance_timestamp").getTime() < maxTime.getTime()) {
				System.out.println("quit because I saw" + candidatesForChange[0].getTimestampAsTimestamp("balance_timestamp").toString());
				break;
			}

			long validatedBalance = candidatesForChange[0].getLong("user_balance");

			System.out.println("update " +userId );
			voltQueueSQL(updateCluster, validatedBalance, maxTime, this.getClusterId(), userId);
		}

		voltExecuteSQL(true);

		VoltTable t = new VoltTable(
				new VoltTable.ColumnInfo("user_clusters_updated", VoltType.BIGINT),
				new VoltTable.ColumnInfo("update_time", VoltType.TIMESTAMP));

		t.addRow( candidatesForChange[0].getRowCount(), maxTime);

		VoltTable[] resultsAsArray = { t };

		return resultsAsArray;

	}
}
