package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.statemanagement.strategy.dynamicdivide.hashtree.HashTree;
import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

class HashTreeTest {
    @BeforeEach
    void setup() {
    }

    @Test
    void getRequiredHashIds_4OutOf8_requiredRest4() {
        byte[] state = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        List<byte[]> stateHashes = HashTree.calcStateHashes(state, 8);

        BitSet hashIds = new BitSet();
        hashIds.flip(0, 4);
        byte[] tree = HashTree.generatePrunedTree(stateHashes, hashIds);
        byte[] rootHash = HashTree.extractRootHash(tree);
        HashTree hashTree = new HashTree(rootHash, 8);
        hashTree.addTree(hashIds, tree);
        BitSet expected = new BitSet();
        expected.flip(4, 8);
        Assertions.assertEquals(expected, hashTree.getRequiredHashIds());
    }

    @Test
    void addTree_twice8to4Tree_existsAllHashesWhen2TreeAdded() {
        byte[] state = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        List<byte[]> stateHashes = HashTree.calcStateHashes(state, 8);

        BitSet hashIds1 = new BitSet();
        hashIds1.set(0);
        hashIds1.set(1);
        hashIds1.set(2);
        hashIds1.set(3);
        byte[] tree1 = HashTree.generatePrunedTree(stateHashes, hashIds1);


        BitSet hashIds2 = new BitSet();
        hashIds2.set(4);
        hashIds2.set(5);
        hashIds2.set(6);
        hashIds2.set(7);
        byte[] tree2 = HashTree.generatePrunedTree(stateHashes, hashIds2);

        byte[] rootHash = HashTree.extractRootHash(tree1);
        HashTree hashTree = new HashTree(rootHash, 8);
        Assertions.assertTrue(hashTree.addTree(hashIds1, tree1));
        Assertions.assertFalse(hashTree.existsAllHashes());
        Assertions.assertTrue(hashTree.addTree(hashIds2, tree2));
        Assertions.assertTrue(hashTree.existsAllHashes());
    }

    @Test
    void verify_aLeafHash_verificationIsSuccessful() {
        byte[] state = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        List<byte[]> stateHashes = HashTree.calcStateHashes(state, 8);

        BitSet hashIds = new BitSet();
        hashIds.flip(0, 8);
        byte[] tree = HashTree.generatePrunedTree(stateHashes, hashIds);

        byte[] rootHash = HashTree.extractRootHash(tree);
        HashTree hashTree = new HashTree(rootHash, 8);
        hashTree.addTree(hashIds, tree);
        Assertions.assertTrue(hashTree.existsAllHashes());

        Assertions.assertTrue(hashTree.verify(7, "h".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void generatePrunedTree_request0Leaves_getRootHash() {
        byte[] state = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        List<byte[]> stateHashes = HashTree.calcStateHashes(state, 8);
        BitSet hashIds = new BitSet();
        byte[] tree = HashTree.generatePrunedTree(stateHashes, hashIds);
        String hexRootHash = "99464eeecd051185ca8795cd86e251337bd12df16d6b2b34d75c003cdd81eed32f2855d2b729f8116de0be1ed8a271e8025315b88761857b15a591f6f078ba92";
        byte[] expected = BaseEncoding.base16().decode(hexRootHash.toUpperCase());
        Assertions.assertArrayEquals(expected, tree);
    }
}