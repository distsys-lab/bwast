package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.strategy.dynamicdivide.hashtree.HashTree;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

class StateSenderTest {
    LinkedBlockingQueue<SystemMessage> outQueue = new LinkedBlockingQueue<>();
    int totalChunksNum = 256;
    int processId = 1;
    int replicaId = 1;
    int reg = 1;
    int leader = 1;
    int cid = 1;
    int stateNumber = 123;
    int stateSize = 100 * 1024 * 1024;
    DefaultApplicationState state = new DefaultApplicationState();
    BitSet hashIds = null;
    List<Integer> oldQueueNodeIds = Arrays.asList(2, 4, 5, 6, 7, 8, 11, 13, 15, 16, 18, 20, 21, 27, 30, 31, 32, 34, 35, 37, 42, 47, 51, 57, 60, 62, 63, 66, 68, 70, 72, 74, 78, 80, 87, 89, 91, 92, 93, 94, 98, 100, 101, 105, 107, 109, 110, 111, 113, 115, 119, 123, 124, 125, 130, 131, 132, 137, 138, 139, 146, 147, 148, 153, 158, 160, 164, 168, 173, 175, 176, 177, 180, 181, 182, 186, 190, 194, 196, 198, 199, 201, 205, 207, 215, 217, 218, 220, 221, 224, 228, 230, 232, 235, 236, 237, 243, 246, 248, 253);
    List<Integer> newQueueNodeIds = Arrays.asList(0, 1, 2, 3, 4, 7, 10, 15, 16, 17, 18, 20, 21, 26, 27, 28, 31, 33, 38, 40, 41, 45, 48, 53, 54, 55, 56, 62, 63, 64, 66, 73, 77, 79, 85, 87, 90, 94, 95, 98, 101, 103, 104, 105, 106, 114, 117, 118, 123, 125, 127, 130, 135, 141, 142, 153, 157, 158, 164, 165, 166, 167, 174, 175, 177, 178, 182, 183, 185, 186, 187, 193, 194, 196, 201, 202, 203, 204, 205, 208, 211, 212, 214, 215, 216, 217, 220, 223, 227, 230, 242, 243, 244, 247, 249, 250, 251, 252, 253, 254);

    public static ByteBuffer buildStateChunkBuffer(int chunkId, int totalChunkNum, int normalChunkSize, byte[] state) {
        int lastChunk = totalChunkNum - 1;
        int lastChunkSize = state.length - ((totalChunkNum - 1) * normalChunkSize);
        int chunkSize;
        if (chunkId != lastChunk) {
            chunkSize = normalChunkSize;
        } else {
            chunkSize = lastChunkSize;
        }
        int id = 0;
        return ByteBuffer.wrap(state, id * normalChunkSize, chunkSize).slice();
    }

    @Test
    void update_measureProcessingTime_shouldLessThan100ms() {

        // set serialized state
        byte[] serializedState = createHugeSnapshot(stateNumber, stateSize);
        state.setSerializedState(serializedState);

        // set out queue's contents
        oldQueueNodeIds.forEach(x -> outQueue.add(buildChunkMessage(x, cid)));

        // set send request's order
        BitSet chunkIds = new BitSet();
        newQueueNodeIds.forEach(chunkIds::set);
        System.out.println(System.currentTimeMillis());
        Assertions.assertTimeout(Duration.ofMillis(10), () -> modifyOutQueue(buildSendQueueUpdater(chunkIds, hashIds, cid, state)));
        System.out.println(System.currentTimeMillis());
    }

    public void modifyOutQueue(UnaryOperator<List<SystemMessage>> operator) {
        List<SystemMessage> list = new LinkedList<>();
        outQueue.drainTo(list);
        List<SystemMessage> newList = operator.apply(list);
        outQueue.addAll(newList);
    }

    private UnaryOperator<List<SystemMessage>> buildSendQueueUpdater(BitSet chunkIds, BitSet hashIds, int cid, ApplicationState state) {
        return (List<SystemMessage> messages) -> {
            List<DynamicDivideSMReplyMessage> dynamicDivideMessages = messages.stream().filter(x -> x instanceof DynamicDivideSMReplyMessage).map(x -> (DynamicDivideSMReplyMessage) x).collect(Collectors.toList());
            BitSet sendingChunkIds = new BitSet();
            dynamicDivideMessages.forEach(reply -> sendingChunkIds.set(reply.getChunkId()));
            BitSet intersection = (BitSet) chunkIds.clone();
            intersection.and(sendingChunkIds);
            BitSet addedChunkIds = (BitSet) chunkIds.clone();
            addedChunkIds.xor(intersection);
            BitSet removedChunkIds = (BitSet) sendingChunkIds.clone();
            removedChunkIds.xor(intersection);
            System.out.println("size of replies (before update): " + messages.stream().filter(x -> x instanceof DynamicDivideSMReplyMessage).count());
            messages.removeAll(dynamicDivideMessages.stream().filter(reply -> removedChunkIds.get(reply.getChunkId())).collect(Collectors.toList()));
            messages.addAll(addedChunkIds.stream().mapToObj(chunkId -> buildChunkMessage(chunkId, cid)).collect(Collectors.toList()));
            List<DynamicDivideSMReplyMessage> updatedDynamicDivideMessages = messages.stream().filter(x -> x instanceof DynamicDivideSMReplyMessage).map(x -> (DynamicDivideSMReplyMessage) x).collect(Collectors.toList());
            if (hashIds != null) {
                int totalChunkNum = getTotalNumberOfChunks();
                byte[] hashTree = HashTree.generatePrunedTree(state.getSerializedState(), hashIds, totalChunkNum);
                DynamicDivideSMReplyMessage head;
                if (updatedDynamicDivideMessages.isEmpty()) {
                    head = buildChunkMessage(0, cid);
                    messages.add(head);
                } else {
                    head = updatedDynamicDivideMessages.get(0);
                }
                ApplicationState stateWithMessageBatches = new DefaultApplicationState((DefaultApplicationState) state);
                stateWithMessageBatches.setSerializedState(head.getState().getSerializedState());
                DynamicDivideSMReplyMessage messageWithHash = new DynamicDivideSMReplyMessage(head, stateWithMessageBatches, hashIds, hashTree);
                messages.set(messages.indexOf(head), messageWithHash);
            }
            System.out.println("size of replies (after update): " + updatedDynamicDivideMessages.size());
            return messages;
        };
    }

    private DynamicDivideSMReplyMessage buildChunkMessage(int chunkId, int cid) {
        int stateSize = state.getSerializedState().length;
        int totalChunksNum = getTotalNumberOfChunks();
        int chunkSize = stateSize / (totalChunksNum - 1);
        ByteBuffer chunkBuffer = buildStateChunkBuffer(chunkId, totalChunksNum, chunkSize, state.getSerializedState());
        ApplicationState newState = new ChunkApplicationState(chunkBuffer);
        return new DynamicDivideSMReplyMessage(getProcessId(), cid, TOMUtil.SM_REPLY,
                replicaId, chunkId, null, null, newState, getCurrentView(),
                getLastReg(), getCurrentLeader());
    }

    private int getTotalNumberOfChunks() {
        return totalChunksNum;
    }

    private int getProcessId() {
        return processId;
    }

    private View getCurrentView() {
        return null;
    }

    private int getLastReg() {
        return reg;
    }

    private int getCurrentLeader() {
        return leader;
    }

    private byte[] createHugeSnapshot(int count, int size) {
        String countString = String.format("%020d", count);
        byte[] snapshot = new byte[size];
        byte[] countBytes = countString.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(countBytes, 0, snapshot, 0, countBytes.length);
        return snapshot;
    }
}