package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.leaderchange.CertifiedDecision;

public class ChunkApplicationState implements ApplicationState {
    private byte[] serializedState;

    ChunkApplicationState(byte[] serializedState) {
        this.serializedState = serializedState;
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
        return serializedState != null;
    }

    @Override
    public byte[] getSerializedState() {
        return serializedState;
    }

    @Override
    public void setSerializedState(byte[] state) {
        this.serializedState = state;
    }

    @Override
    public byte[] getStateHash() {
        return null;
    }
}
