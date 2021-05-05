package bftsmart.statemanagement.strategy.dynamicdivide;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class ChunkCollector {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final int totalChunkNum;

    private final SortedMap<Integer, byte[]> collectedChunks = Collections.synchronizedSortedMap(new TreeMap<>());
    private final Map<Integer, byte[]> pendingChunks = Collections.synchronizedMap(new HashMap<>());
    private final HashCollector hashCollector;

    public ChunkCollector(int totalChunkNum, HashCollector hashCollector) {
        this.totalChunkNum = totalChunkNum;
        this.hashCollector = hashCollector;
    }

    public void addPendingChunks() {
        if (!hashCollector.existsTrustedRootHash() || pendingChunks.isEmpty()) {
            return;
        }

        if (hashCollector.existsAllHashes()) {
            pendingChunks.forEach(this::addChunk);
            pendingChunks.clear();
            return;
        }

        List<Integer> addedChunkIds = new LinkedList<>();
        pendingChunks.forEach((id, chunk) -> {
            boolean successful = addChunk(id, chunk);
            if (successful) {
                addedChunkIds.add(id);
            }
        });
        addedChunkIds.forEach(pendingChunks::remove);
    }

    public boolean addChunk(int chunkId, byte[] chunk) {
        logger.info("[Time] verifyHash start: " + System.currentTimeMillis());
        if (!hashCollector.existsTrustedRootHash()) {
            pendingChunks.put(chunkId, chunk);
            return false;
        }
        if (!hashCollector.existsHash(chunkId)) {
            pendingChunks.put(chunkId, chunk);
            return false;
        }
        if (!hashCollector.verify(chunkId, chunk)) {
            logger.info("verify failed");
            return false;
        }
        logger.info("[Time] verifyHash end: " + System.currentTimeMillis());
        collectedChunks.put(chunkId, chunk);
        return true;
    }

    public BitSet getRestChunkIds() {
        BitSet chunkIds = new BitSet();
        synchronized (collectedChunks) {
            collectedChunks.keySet().forEach(chunkIds::set);
        }
        synchronized (pendingChunks) {
            pendingChunks.keySet().forEach(chunkIds::set);
        }
        chunkIds.flip(0, totalChunkNum);
        return chunkIds;
    }

    public int getRestChunkNum() {
        return totalChunkNum - collectedChunks.size() - pendingChunks.size();
    }

    public byte[] getState() {
        if (getRestChunkNum() > 0) {
            throw new IllegalStateException("This method must call after collected all chunks.");
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        collectedChunks.values().forEach(chunk -> baos.write(chunk, 0, chunk.length));
        return baos.toByteArray();
    }

    public void reset() {
        collectedChunks.clear();
        pendingChunks.clear();
    }
}
