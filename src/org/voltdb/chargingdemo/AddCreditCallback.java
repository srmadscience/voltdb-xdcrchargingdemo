package org.voltdb.chargingdemo;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

public class AddCreditCallback extends ReportLatencyCallback {

  UserState[] state = null;

  int userId = 0;
  int offset = 0;

  public AddCreditCallback(String statname, UserState[] state, int userId, int offset) {
    super(statname);
    this.state = state;
    this.userId = userId;
    this.offset = offset;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.voltdb.chargingdemo.ReportLatencyCallback#clientCallback(org.voltdb.
   * client.ClientResponse)
   */
  @Override
  public void clientCallback(ClientResponse arg0) throws Exception {
    super.clientCallback(arg0);

    VoltTable balanceTable = arg0.getResults()[arg0.getResults().length - 2];
    VoltTable reservationTable = arg0.getResults()[arg0.getResults().length - 1];

    if (balanceTable.advanceRow()) {
      
      int userid = (int) balanceTable.getLong("userid");
      
      long validatedBalance = balanceTable.getLong("validated_balance");
      long utAmount = balanceTable.getLong("ut_amount");
      
      long currentlyAllocated = 0;
      
      if (reservationTable.advanceRow()) {
    	  currentlyAllocated = reservationTable.getLong("allocated");
      }

      synchronized (state) {
        state[userid - offset].reportBalance(validatedBalance + utAmount - currentlyAllocated );
      }

    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return super.toString();
  }

}
