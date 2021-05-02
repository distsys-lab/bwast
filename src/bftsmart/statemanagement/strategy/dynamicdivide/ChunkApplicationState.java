package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.leaderchange.CertifiedDecision;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ChunkApplicationState implements ApplicationState {
    private static final long serialVersionUID = 2801568594413719860L;
    private byte[] serializedState;
    private ByteBuffer stateBuffer;

    ChunkApplicationState(byte[] serializedState) {
        this.serializedState = serializedState;
        this.stateBuffer = null;
    }

    ChunkApplicationState(ByteBuffer stateBuffer) {
        this.stateBuffer = stateBuffer;
        serializedState = null;
    }

    @Override
    public int getLastCID() {
        return 0;
    }

    @Override
    public CertifiedDecision getCertifiedDecision(ServerViewController controller) {
        return null;
    }

    @Override
    public boolean hasState() {
        return serializedState != null || stateBuffer != null;
    }

    @Override
    public byte[] getSerializedState() {
        if (serializedState != null) {
            return serializedState;
        }
        serializedState = Arrays.copyOfRange(stateBuffer.array(), stateBuffer.arrayOffset(), stateBuffer.arrayOffset() + stateBuffer.limit());
        stateBuffer = null;
        return serializedState;
    }

    @Override
    public void setSerializedState(byte[] state) {
        this.serializedState = state;
        stateBuffer = null;
    }

    @Override
    public byte[] getStateHash() {
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        if (serializedState != null) {
            out.writeInt(serializedState.length);
            out.write(serializedState);
        } else {
            out.writeInt(stateBuffer.limit());
            out.write(stateBuffer.array(), stateBuffer.arrayOffset(), stateBuffer.limit());
        }
        out.flush();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.stateBuffer = null;
        int length = in.readInt();
        serializedState = new byte[length];
        in.readFully(serializedState);
    }

    private void readObjectNoData() throws ObjectStreamException {
        this.stateBuffer = null;
        serializedState = null;
    }
}
