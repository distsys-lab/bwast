package bftsmart.statemanagement.strategy.dynamicdivide.hashtree;

import bftsmart.statemanagement.strategy.dynamicdivide.StateSender;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class HashTree {
    private final byte[] trustedRootHash;
    private final int chunkNum;
    private final MessageDigest md;
    private final Map<Integer, byte[]> hashes = new HashMap<>();
    private boolean existsAllHashes = false;
    private HashTreeNode rootNode;
    private static final Logger logger = LoggerFactory.getLogger(HashTree.class);

    public HashTree(byte[] trustedRootHash, int chunkNum) {
        this.trustedRootHash = trustedRootHash;
        this.chunkNum = chunkNum;
        this.md = getMessageDigest();
    }

    public static List<byte[]> calcStateHashes(byte[] state, int chunkNum) {
        MessageDigest md = getMessageDigest();
        int chunkSize = state.length / (chunkNum - 1);
        List<byte[]> stateHashes = new ArrayList<>(chunkNum);
        IntStream.range(0, chunkNum).forEachOrdered(i -> {
            md.update(StateSender.buildStateChunkBuffer(i, chunkNum, chunkSize, state));
            stateHashes.add(md.digest());
        });
        return stateHashes;
    }

    public static byte[] generatePrunedTree(List<byte[]> stateHashes, BitSet hashIds) {
        List<HashTreeNode> nodeList = new LinkedList<>();
        int i = 0;
        for (byte[] hash : stateHashes) {
            nodeList.add(new HashTreeNode(hash, hashIds.get(i), null, null));
            i++;
        }
        HashTreeNode root = HashTreeNode.generateHashTree(nodeList);
        root.prune();
        return root.toByteArray();
    }

    public static MessageDigest getMessageDigest() {
        MessageDigest md;
        try {
            md = TOMUtil.getHashEngine();
        } catch (NoSuchAlgorithmException e) {
            Logger logger = LoggerFactory.getLogger(HashTree.class);
            logger.error("Failed to get message digest object", e);
            throw new RuntimeException(e); // can't resolve exception without rewrite config file
        }
        return md;
    }

    public static byte[] extractRootHash(byte[] tree) {
        return Arrays.copyOfRange(tree, 0, getMessageDigest().getDigestLength());
    }

    public BitSet getRequiredHashIds() {
        return rootNode.getRequiredHashIds();
    }

    public boolean existsAllHashes() {
        return existsAllHashes;
    }

    public boolean addTree(BitSet hashIds, byte[] tree) {
        byte[] rootHash = extractRootHash(tree);
        if (!Arrays.equals(rootHash, trustedRootHash)) {
            return false;
        }
        if (tree.length == rootHash.length) {
            return true;
        }
        HashTreeNode newRootNode = HashTreeNode.fromByteArray(tree, hashIds, chunkNum);
        if (!newRootNode.verifyTree()) {
            return false;
        }
        hashes.putAll(newRootNode.getLeaves());
        if (rootNode == null) {
            rootNode = newRootNode;
        } else {
            rootNode.merge(newRootNode);
        }
        if (rootNode.existsAllHash()) {
            existsAllHashes = true;
        }
        return true;
    }

    public boolean verify(int chunkId, byte[] chunk) {
        byte[] hash = hashes.getOrDefault(chunkId, null);
        if (hash == null) {
            return false;
        }
        return Arrays.equals(hash, md.digest(chunk));
    }

    public boolean existsHash(int chunkId) {
        return hashes.containsKey(chunkId);
    }
}
