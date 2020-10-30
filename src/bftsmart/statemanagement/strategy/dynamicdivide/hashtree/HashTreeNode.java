package bftsmart.statemanagement.strategy.dynamicdivide.hashtree;

import com.google.common.primitives.Bytes;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashTreeNode {
    private byte[] hash;
    private final HashTreeNode left;
    private final HashTreeNode right;
    private boolean needsSend;
    private boolean isChildrenSent = false;
    private final Integer hashId;

    public HashTreeNode(byte[] hash, boolean needsSend, HashTreeNode left, HashTreeNode right) {
        this.hash = hash;
        this.needsSend = needsSend;
        this.left = left;
        this.right = right;
        this.hashId = null;
    }

    public HashTreeNode(byte[] hash, boolean needsSend, int hashId) {
        this.hash = hash;
        this.needsSend = needsSend;
        this.left = null;
        this.right = null;
        this.hashId = hashId;
    }

    public static HashTreeNode fromByteArray(byte[] tree, BitSet hashIds, int chunkNum) {
        List<HashTreeNode> nodeList = new LinkedList<>();
        for (int i = 0; i < chunkNum; i++) {
            nodeList.add(new HashTreeNode(null, hashIds.get(i), i));
        }
        HashTreeNode root = generateHashTree(nodeList);
        root.prune();
        Deque<byte[]> hashQueue = expandHashTree(tree);
        root.setHash(hashQueue);
        return root;
    }

    public void print() {
        print("");
    }

    private void print(String serialNum) {
        if (serialNum.equals("")) {
            System.out.println("root");
        } else {
            System.out.println(serialNum);
        }
        System.out.println("--- " + ((needsSend || serialNum.equals("")) ? "send" : "    ") + " " + (isChildrenSent ? "children" : "        ") + " " + (left == null ? "leaf" : "node"));
        System.out.println();
        if (left != null) {
            left.print(serialNum + "0");
        }
        if (right != null) {
            right.print(serialNum + "1");
        }
    }

    private static Deque<byte[]> expandHashTree(byte[] tree) {
        int hashLength = HashTree.getMessageDigest().getDigestLength();
        LinkedList<byte[]> hashes = new LinkedList<>();
        int hashNum = tree.length / hashLength;
        int start = 0;
        for (int i = 0; i < hashNum; i++) {
            hashes.add(Arrays.copyOfRange(tree, start, start + hashLength));
            start += hashLength;
        }
        return hashes;
    }

    public static HashTreeNode generateHashTree(List<HashTreeNode> nodeList) {
        HashTreeNode rootNode = generateHashTree(nodeList, HashTree.getMessageDigest());
        rootNode.needsSend = true;
        return rootNode;
    }

    private static HashTreeNode generateHashTree(List<HashTreeNode> nodeList, MessageDigest md) {
        List<HashTreeNode> parentNodeList = new LinkedList<>();
        for (Iterator<HashTreeNode> iter = nodeList.iterator(); iter.hasNext(); ) {
            HashTreeNode left = iter.next();
            HashTreeNode right = iter.next();
            byte[] hash = (left.getHash() != null) ? md.digest(Bytes.concat(left.getHash(), right.getHash())) : null;
            parentNodeList.add(new HashTreeNode(hash, false, left, right));
        }
        if (nodeList.size() % 2 == 1) {
            parentNodeList.add(nodeList.get(nodeList.size() - 1));
        }
        if (parentNodeList.size() == 1) {
            return parentNodeList.get(0);
        } else {
            return generateHashTree(parentNodeList, md);
        }
    }

    public void prune() {
        if (left != null) {
            left.prune();
        }
        if (right != null) {
            right.prune();
        }
        if (left == null || right == null) {
            return;
        }
        boolean existsLeft = left.needsSend || left.isChildrenSent;
        boolean existsRight = right.needsSend || right.isChildrenSent;
        if (existsLeft || existsRight) {
            isChildrenSent = true;
        }
        if (existsLeft != existsRight) {
            if (!left.isChildrenSent) {
                left.needsSend = true;
            }
            if (!right.isChildrenSent) {
                right.needsSend = true;
            }
        }
    }

    public byte[] getHash() {
        return hash;
    }

    private void setHash(Deque<byte[]> hashQueue) {
        if (needsSend) {
            hash = hashQueue.removeFirst();
        }
        if (left != null) {
            left.setHash(hashQueue);
        }
        if (right != null) {
            right.setHash(hashQueue);
        }
    }

    public byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (needsSend) {
            baos.write(hash, 0, hash.length); // write(byte[]) throws IOException, this method doesn't throws one.
        }
        if (left != null) {
            byte[] leftByteArray = left.toByteArray();
            baos.write(leftByteArray, 0, leftByteArray.length);
        }
        if (right != null) {
            byte[] rightByteArray = right.toByteArray();
            baos.write(rightByteArray, 0, rightByteArray.length);
        }
        return baos.toByteArray();
    }

    private byte[] calcHash(MessageDigest md) {
        if (needsSend || left == null || right == null) {
            return hash;
        }
        byte[] leftHash = left.calcHash(md);
        byte[] rightHash = right.calcHash(md);
        return md.digest(Bytes.concat(leftHash, rightHash));
    }

    public BitSet getRequiredHashIds() {
        BitSet hashIds = new BitSet();
        if (hashId != null && hash == null) {
            hashIds.set(hashId);
        }
        if (left != null) {
            hashIds.or(left.getRequiredHashIds());
        }
        if (right != null) {
            hashIds.or(right.getRequiredHashIds());
        }
        return hashIds;
    }

    public Map<Integer, byte[]> getLeaves() {
        Map<Integer, byte[]> leaves = new HashMap<>();
        putLeavesToMap(leaves);
        return leaves;
    }

    public boolean verifyTree() {
        MessageDigest md = HashTree.getMessageDigest();
        return Arrays.equals(hash, calcHash(md));
    }

    public void merge(HashTreeNode newNode) {
        if (hash == null) {
            hash = newNode.hash;
        }
        if (left != null && newNode.left != null) {
            left.merge(newNode.left);
        }
        if (right != null && newNode.right != null) {
            right.merge(newNode.right);
        }
    }

    public boolean existsAllHash() {
        if (left == null || right == null) {
            return hash != null;
        }
        return left.existsAllHash() && right.existsAllHash();
    }

    private void putLeavesToMap(Map<Integer, byte[]> leaves) {
        if (left == null || right == null) {
            if (hash != null) {
                leaves.put(hashId, hash);
            }
            return;
        }
        left.putLeavesToMap(leaves);
        right.putLeavesToMap(leaves);
    }
}