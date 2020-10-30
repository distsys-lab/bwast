package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;

public class DynamicDivideSMRequestMessage extends DynamicDivideSMMessage {
    private BitSet chunkIds;
    private BitSet hashIds;

    public DynamicDivideSMRequestMessage(int sender, int cid, int type, int replica, BitSet chunkIds, BitSet hashIds, ApplicationState state, View view, int regency, int leader) {
        super(sender, cid, type, replica, state, view, regency, leader);
        this.chunkIds = chunkIds;
        this.hashIds = hashIds;
    }

    public DynamicDivideSMRequestMessage() {
        super();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(chunkIds);
        out.writeObject(hashIds);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        chunkIds = (BitSet) in.readObject();
        hashIds = (BitSet) in.readObject();
    }

    public BitSet getChunkIds() { return chunkIds; }

    public BitSet getHashIds() { return hashIds; }
}
