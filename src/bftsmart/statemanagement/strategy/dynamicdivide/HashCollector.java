package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.strategy.dynamicdivide.hashtree.HashTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HashCollector {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private HashTree hashTree;
    private final int totalChunkNum;
    private final ServerViewController SVController;
    private final Map<Integer, byte[]> senderRootHashes = new HashMap<>();
    private final Map<BitSet, byte[]> pendingSubTrees = Collections.synchronizedMap(new HashMap<>());

    public HashCollector(ServerViewController SVController, int totalChunkNum) {
        this.SVController = SVController;
        this.totalChunkNum = totalChunkNum;
    }

    public boolean addHashes(int sender, BitSet hashIds, byte[] hashes) {
        byte[] rootHash = HashTree.extractRootHash(hashes);
        senderRootHashes.put(sender, rootHash);

        if (!enoughRootHashes(rootHash)) {
            if (!existsTrustedRootHash()) {
                pendingSubTrees.put(hashIds, hashes);
            }
            return false;
        }
        if (hashTree == null) {
            hashTree = new HashTree(rootHash, totalChunkNum);
            synchronized (pendingSubTrees) {
                pendingSubTrees.forEach(hashTree::addTree);
                pendingSubTrees.clear();
            }
        }

        hashTree.addTree(hashIds, hashes);
        return true;
    }

    public boolean verify(int chunkId, byte[] chunk) {
        if (hashTree == null) {
            throw new IllegalStateException("has no trustedRootHash");
        }
        return hashTree.verify(chunkId, chunk);
    }

    public boolean existsHash(int chunkId) {
        if (hashTree == null) {
            throw new IllegalStateException("has no trustedRootHash");
        }
        return hashTree.existsHash(chunkId);
    }

    public boolean existsAllHashes() {
        return existsTrustedRootHash() && hashTree.existsAllHashes();
    }

    public BitSet getRequiredHashIds() {
        if (!existsTrustedRootHash()) {
            return new BitSet(); // only root hash required
        } else {
            return hashTree.getRequiredHashIds();
        }
    }

    public boolean existsTrustedRootHash() {
        return hashTree != null;
    }

    public void reset() {
        hashTree = null;
        senderRootHashes.clear();
        pendingSubTrees.clear();
    }

    private boolean enoughRootHashes(byte[] rootHash) {
        Collection<byte[]> rootHashes = senderRootHashes.values();
        int count = (int) rootHashes.stream().filter(x -> Arrays.equals(x, rootHash)).count();

        return count > SVController.getQuorum();
    }
}
