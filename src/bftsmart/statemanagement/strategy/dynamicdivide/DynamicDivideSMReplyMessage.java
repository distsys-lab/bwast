package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;

public class DynamicDivideSMReplyMessage extends DynamicDivideSMMessage {
    private int chunkId;
    private BitSet hashIds;
    private byte[] hashes;
    private boolean hasHashes;


    public DynamicDivideSMReplyMessage(int sender, int cid, int type, int replica, int chunkId, BitSet hashIds, byte[] hashes, ApplicationState state, View view, int regency, int leader) {
        super(sender, cid, type, replica, state, view, regency, leader);
        this.chunkId = chunkId;
        this.hashIds = hashIds;
        this.hashes = hashes;
        hasHashes = hashIds != null;
    }

    public DynamicDivideSMReplyMessage(DynamicDivideSMReplyMessage msg, ApplicationState state, BitSet hashIds, byte[] hashes) {
        super(msg.getSender(), msg.getCID(), msg.getType(), msg.getReplica(), state, msg.getView(), msg.getRegency(), msg.getLeader());
        this.chunkId = msg.getChunkId();
        this.hashIds = hashIds;
        this.hashes = hashes;
        hasHashes = hashIds != null;
    }

    public DynamicDivideSMReplyMessage() {
        super();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(chunkId);
        out.writeObject(hashIds);
        if (hashIds == null) {
            out.writeInt(0);
        } else {
            out.writeInt(hashes.length);
            out.write(hashes);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        chunkId = in.readInt();
        hashIds = (BitSet) in.readObject();
        int hashLendth = in.readInt();
        hasHashes = hashLendth > 0;
        if (hasHashes) {
            hashes = new byte[hashLendth];
            in.readFully(hashes);
        }
    }

    public void setHashes(BitSet hashIds, byte[] hashes) {
        this.hashIds = hashIds;
        this.hashes = hashes;
        this.hasHashes = hashIds != null;
    }

    public int getChunkId() {
        return chunkId;
    }

    public BitSet getHashIds() {
        return hashIds;
    }

    public byte[] getHashes() {
        return hashes;
    }

    public boolean hasHashes() {
        return hasHashes;
    }
}
