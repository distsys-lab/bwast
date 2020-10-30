package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.server.ServerConnection;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.statemanagement.strategy.dynamicdivide.hashtree.HashTree;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class StateSender {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final TOMLayer tomLayer;
    private final ServerViewController SVController;
    private final DefaultApplicationState state;
    private final int replicaId;

    public StateSender(TOMLayer tomLayer,
                       ServerViewController SVController,
                       DefaultApplicationState state,
                       int replicaId) {
        this.tomLayer = tomLayer;
        this.SVController = SVController;
        this.state = new DefaultApplicationState(state);
        this.replicaId = replicaId;
    }

    public static byte[] buildStateChunk(int chunkId, int totalChunkNum, int normalChunkSize, byte[] state) {
        int lastChunk = totalChunkNum - 1;
        int lastChunkSize = state.length - ((totalChunkNum - 1) * normalChunkSize);
        int chunkSize;
        if (chunkId != lastChunk) {
            chunkSize = normalChunkSize;
        } else {
            chunkSize = lastChunkSize;
        }
        int id = 0;
        byte[] chunk = new byte[chunkSize];
        System.arraycopy(state, id * normalChunkSize, chunk, 0, chunkSize);
        return chunk;
    }

    private static int sizeof(Object obj) throws IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        objectOutputStream.close();

        return byteOutputStream.toByteArray().length;
    }

    public void update(int serverId, BitSet chunkIds, BitSet hashIds, int cid) {
        ServerConnection connection = tomLayer.getCommunication().getServersConn().getConnection(serverId);
        logStateSize(state);
        connection.modifyOutQueue(buildSendQueueUpdater(chunkIds, hashIds, cid, state));
        logger.info("[Time] Update Replies End: " + System.currentTimeMillis());
        logger.info("Updated");
    }

    public int getCID() {
        return state.getLastCID();
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

            messages.removeAll(dynamicDivideMessages.stream().filter(reply -> removedChunkIds.get(reply.getChunkId())).collect(Collectors.toList()));
            messages.addAll(addedChunkIds.stream().mapToObj(chunkId -> buildChunkMessage(chunkId, cid)).collect(Collectors.toList()));
            List<DynamicDivideSMReplyMessage> updatedDynamicDivideMessages = messages.stream().filter(x -> x instanceof DynamicDivideSMReplyMessage).map(x -> (DynamicDivideSMReplyMessage) x).collect(Collectors.toList());
            if (hashIds != null) {
                int totalChunkNum = SVController.getStaticConf().getTotalNumberOfChunks();
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
            logger.info("size of messages: " + messages.size());
            logger.info("size of replies: " + updatedDynamicDivideMessages.size());
            return messages;
        };
    }

    private void logStateSize(ApplicationState state) {
        logger.info("state size is " + (state.getSerializedState() != null ? state.getSerializedState().length : "null"));
        if (state instanceof DefaultApplicationState) {
            DefaultApplicationState das = (DefaultApplicationState) state;
            logger.info("batch size is " + (das.getMessageBatches() != null ? das.getMessageBatches().length : "null"));
        }
    }

    public void sendVoidState(int serverId, Recoverable recoverer, int cid) {
        ApplicationState voidState = recoverer.getState(-1, true);
        SMMessage message = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                cid, TOMUtil.SM_REPLY, -1, voidState, SVController.getCurrentView(),
                tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());
        sendMessage(serverId, message);
    }

    private void sendMessage(int serverId, SMMessage message) {
        tomLayer.getCommunication().send(new int[]{serverId}, message);
    }

    private DynamicDivideSMReplyMessage buildChunkMessage(int chunkId, int cid) {
        int stateSize = state.getSerializedState().length;
        int totalChunksNum = SVController.getStaticConf().getTotalNumberOfChunks();
        int chunkSize = stateSize / (totalChunksNum - 1);
        byte[] chunk = buildStateChunk(chunkId, totalChunksNum, chunkSize, state.getSerializedState());
        ApplicationState newState = new ChunkApplicationState(chunk);
        return new DynamicDivideSMReplyMessage(SVController.getStaticConf().getProcessId(), cid, TOMUtil.SM_REPLY,
                replicaId, chunkId, null, null, newState, SVController.getCurrentView(),
                tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());
    }
}
