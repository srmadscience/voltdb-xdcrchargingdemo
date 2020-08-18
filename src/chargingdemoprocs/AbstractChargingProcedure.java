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

public  class AbstractChargingProcedure extends VoltProcedure {

    // @formatter:off

	public static final SQLStmt getMigratableSpendingHistory = new SQLStmt(
			"select ut.userid, uc.user_validated_balance, uc.user_validated_balance_timestamp, max(ut.txn_time) txn_time, sum(ut.amount) ut_amount, count(*) how_many " 
					+ "from user_recent_transactions ut  "
					+ "   , user_table uc " 
					+ "where ut.userid = ?  " 
					+ "and   ut.userid = uc.userid " 
					+ "and ut.txn_time > uc.user_validated_balance_timestamp "
					+ "and ut.txn_time <= DATEADD( MINUTE, ?, NOW) "
					+ "group by ut.userid, uc.user_validated_balance,uc.user_validated_balance_timestamp "
					+ "order by  ut.userid, uc.user_validated_balance,uc.user_validated_balance_timestamp; ");

	
	   public static final SQLStmt migrateOldTxns = new SQLStmt("MIGRATE FROM user_recent_transactions "
	    		+ "WHERE userid = ? "
	    		+ "AND txn_time >= ? "
	    		+ "AND txn_time < ?  "
	    		+ "AND NOT MIGRATING();");
	    
	   public static final SQLStmt updateUser = new SQLStmt("UPDATE user_table "
	   		+ "SET user_validated_balance = ?"
	   		+ "  , user_validated_balance_timestamp = ? "
	    		+ "WHERE userid = ? ;");
	    
	   public int MIGRATION_GRACE_MINUTES = 3;
   
  

    // @formatter:on

    
    protected int cleanupTransactions(long userId, int minutesGracePeriod) {
    	
    	voltQueueSQL(getMigratableSpendingHistory, userId, -1 * minutesGracePeriod);
    	VoltTable[] migrationCandidates = voltExecuteSQL();

    	if (migrationCandidates[0].getRowCount() == 0) {
    		return 0;
    	}

    	
    	migrationCandidates[0].advanceRow();
    	
    	
    	long validatedBalance = migrationCandidates[0].getLong("user_validated_balance");
    	long delta = migrationCandidates[0].getLong("ut_amount");
      	TimestampType validatedBalanceTime = migrationCandidates[0].getTimestampAsTimestamp("user_validated_balance_timestamp");
      	TimestampType lastTxnTime = migrationCandidates[0].getTimestampAsTimestamp("txn_time");
           	
    	if (userId == 2) {
    		System.out.println("bal = " + validatedBalance + "  delta = "  + delta + " " + lastTxnTime.toString());
    	}
    	
    	voltQueueSQL(migrateOldTxns, userId, validatedBalanceTime , lastTxnTime);
    	voltQueueSQL(updateUser,  (delta + validatedBalance), lastTxnTime, userId);
    	voltExecuteSQL();
    	
    	return (int) migrationCandidates[0].getLong("how_many");
    }

    public VoltTable[] run() throws VoltAbortException {

        
        return voltExecuteSQL(true);

    }
}
