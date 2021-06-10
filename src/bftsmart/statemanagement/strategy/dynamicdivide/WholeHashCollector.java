package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.ServerViewController;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static bftsmart.statemanagement.strategy.dynamicdivide.hashtree.HashTree.getMessageDigest;

public class WholeHashCollector extends HashCollector {
    private final ServerViewController SVController;
    private final Map<Integer, byte[]> senderWholeHashes = new HashMap<>();
    private boolean isWholeHashCollected = false;
    private byte[] wholeHash;

    public WholeHashCollector(ServerViewController SVController, int totalChunkNum) {
        super(SVController, totalChunkNum);
        this.SVController = SVController;
    }

    @Override
    public boolean addHashes(int sender, BitSet hashIds, byte[] hashes) {
        // if running WholeHashCollector, whole hash == hashes
        senderWholeHashes.put(sender, hashes);
        if (!enoughWholeHashes(hashes)) {
            return false;
        }
        if (!isWholeHashCollected) {
            isWholeHashCollected = true;
            wholeHash = hashes;
        }
        return true;
    }

    @Override
    public boolean verify(int chunkId, byte[] chunk) {
        if (!isWholeHashCollected) {
            throw new IllegalStateException("don't have whole hash");
        }
        return true;
    }

    public boolean verifyWhole(byte[] state) {
        if (!isWholeHashCollected) {
            throw new IllegalStateException("don't have whole hash");
        }
        MessageDigest md = getMessageDigest();
        return Arrays.equals(wholeHash, md.digest(state));
    }

    @Override
    public boolean existsHash(int chunkId) {
        if (!isWholeHashCollected) {
            throw new IllegalStateException("don't have whole hash");
        }
        return true;
    }

    @Override
    public boolean existsAllHashes() {
        return isWholeHashCollected;
    }

    @Override
    public BitSet getRequiredHashIds(BitSet chunkIds) {
        if (!isWholeHashCollected) {
            return new BitSet(); // only root hash required
        } else {
            return null;
        }
    }

    @Override
    public boolean existsTrustedRootHash() {
        return isWholeHashCollected;
    }

    @Override
    public void reset() {
        isWholeHashCollected = false;
        senderWholeHashes.clear();
    }

    private boolean enoughWholeHashes(byte[] wholeHash) {
        Collection<byte[]> rootHashes = senderWholeHashes.values();
        int count = (int) rootHashes.stream().filter(x -> Arrays.equals(x, wholeHash)).count();
        return count > SVController.getQuorum();
    }
}
