package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.strategy.StandardSMMessage;

public abstract class DynamicDivideSMMessage extends StandardSMMessage {
    public DynamicDivideSMMessage(int sender, int cid, int type, int replica, ApplicationState state, View view, int regency, int leader) {
        super(sender, cid, type, replica, state, view, regency, leader);
    }

    public DynamicDivideSMMessage() {
        super();
    }
}
