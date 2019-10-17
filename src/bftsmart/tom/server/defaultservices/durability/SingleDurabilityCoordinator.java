package bftsmart.tom.server.defaultservices.durability;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.durability.CSTRequest;
import bftsmart.statemanagement.strategy.durability.CSTState;
import bftsmart.statemanagement.strategy.durability.DurableStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SingleDurabilityCoordinator implements Recoverable, SingleExecutable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected ReplicaContext replicaContext;
    private List<byte[]> commands = new ArrayList<>();
    private List<MessageContext> msgContexts = new ArrayList<>();

    private ReentrantLock logLock = new ReentrantLock();
    private ReentrantLock hashLock = new ReentrantLock();
    private ReentrantLock stateLock = new ReentrantLock();

    private TOMConfiguration config;

    private MessageDigest md;

    private DurableStateLog log;

    private StateManager stateManager;

    private int globalCheckpointPeriod;
    private int replicaCkpIndex;
    private int checkpointPortion;
    private int lastCkpCID;

    public SingleDurabilityCoordinator() {
        try {
            md = TOMUtil.getHashEngine();
        } catch (NoSuchAlgorithmException ex) {
            logger.error("Failed to get message digest object", ex);
        }
    }

    @Override
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {

        return executeOrdered(command, msgCtx, false);

    }

    private byte[] executeOrdered(byte[] command, MessageContext msgCtx, boolean noop) {

        int cid = msgCtx.getConsensusId();

        byte[] reply = null;

        if (!noop) {
            stateLock.lock();
            reply = appExecuteOrdered(command, msgCtx);
            stateLock.unlock();
        }

        commands.add(command);
        msgContexts.add(msgCtx);

        if(msgCtx.isLastInBatch()) {
            if (cid % globalCheckpointPeriod == replicaCkpIndex && lastCkpCID < cid) {
                logger.debug("Performing checkpoint for consensus " + cid);
                stateLock.lock();
                byte[] snapshot = getSnapshot();
                stateLock.unlock();
                saveState(snapshot, cid);
                lastCkpCID = cid;
            } else {
                logger.debug("Storing message batch in the state log for consensus " + cid);
                saveCommands(commands.toArray(new byte[0][]), msgContexts.toArray(new MessageContext[0]));
            }
            getStateManager().setLastCID(cid);
            commands = new ArrayList<>();
            msgContexts = new ArrayList<>();
        }
        return reply;
    }

    /**
     * Iterates over the commands to find if any replica took a checkpoint.
     * When a replica take a checkpoint, it is necessary to save in an auxiliary table
     * the position in the log in which that replica took the checkpoint.
     * It is used during state transfer to find lower or upper log portions to be
     * restored in the recovering replica.
     * This iteration over commands is needed due to the batch execution strategy
     * introduced with the durable techniques to improve state management. As several
     * consensus instances can be executed in the same batch of commands, it is necessary
     * to identify if the batch contains checkpoint indexes.

     * @param msgCtxs the contexts of the consensus where the messages where executed.
     * There is one msgCtx message for each command to be executed

     * @return the index in which a replica is supposed to take a checkpoint. If there is
     * no replica taking a checkpoint during the period comprised by this command batch, it
     * is returned -1
     */
    private int findCheckpointPosition(int[] cids) {
        if(config.getGlobalCheckpointPeriod() < 1)
            return -1;
        if(cids.length == 0)
            throw new IllegalArgumentException();
        int firstCID = cids[0];
        if((firstCID + 1) % checkpointPortion == 0) {
            return cidPosition(cids, firstCID);
        } else {
            int nextCkpIndex = (((firstCID / checkpointPortion) + 1) * checkpointPortion) - 1;
            if(nextCkpIndex <= cids[cids.length -1]) {
                return cidPosition(cids, nextCkpIndex);
            }
        }
        return -1;
    }

    /**
     * Iterates over the message contexts to retrieve the index of the last
     * command executed prior to the checkpoint. That index is used by the
     * state transfer protocol to find the position of the log commands in
     * the log file.
     *
     * @param msgCtx the message context of the commands executed by the replica.
     * There is one message context for each command
     * @param cid the CID of the consensus where a replica took a checkpoint
     * @return the higher position where the CID appears
     */
    private int cidPosition(int[] cids, int cid) {
        int index = -1;
        if(cids[cids.length-1] == cid)
            return cids.length-1;
        for(int i = 0; i < cids.length; i++) {
            if(cids[i] > cid)
                break;
            index++;
        }
        logger.info("Checkpoint is in position " + index);
        return index;
    }


    @Override
    public ApplicationState getState(int cid, boolean sendState) {
        logLock.lock();
        ApplicationState ret = null;
        logLock.unlock();
        return ret;
    }

    @Override
    public int setState(ApplicationState recvState) {
        int lastCID = -1;
        if (recvState instanceof CSTState) {
            CSTState state = (CSTState) recvState;

            int lastCheckpointCID = state.getCheckpointCID();
            lastCID = state.getLastCID();

            logger.debug("(DurabilityCoordinator.setState) I'm going to update myself from CID "
                    + lastCheckpointCID + " to CID " + lastCID);

            stateLock.lock();
            if(state.getSerializedState() != null) {
                logger.info("The state is not null. Will install it");
                log.update(state);
                installSnapshot(state.getSerializedState());
            }

            logger.info("Installing log from " + (lastCheckpointCID+1) + " to " + lastCID);

            for (int cid = lastCheckpointCID + 1; cid <= lastCID; cid++) {
                try {
                    logger.debug("Processing  and verifying batched requests for CID " + cid);
                    CommandsInfo cmdInfo = state.getMessageBatch(cid);
                    byte[][] commands = cmdInfo.commands;
                    MessageContext[] msgCtx = cmdInfo.msgCtx;

                    if (commands == null || msgCtx == null || msgCtx[0].isNoOp()) {
                        continue;
                    }

                    for(int i = 0; i < commands.length; i++) {
                        appExecuteOrdered(commands[i], msgCtx[i]);
                    }
                } catch (Exception e) {
                    logger.error("Failed to process and verify batched requests",e);
                }

            }
            logger.info("Installed");
            stateLock.unlock();

        }

        return lastCID;
    }

    private final byte[] computeHash(byte[] data) {
        byte[] ret = null;
        hashLock.lock();
        ret = md.digest(data);
        hashLock.unlock();
        return ret;
    }

    private void saveState(byte[] snapshot, int lastCID) {
        logLock.lock();

        logger.debug("(TOMLayer.saveState) Saving state of CID " + lastCID);

        log.newCheckpoint(snapshot, computeHash(snapshot), lastCID);
        log.setLastCID(-1);
        log.setLastCheckpointCID(lastCID);

        logLock.unlock();
        logger.debug("(TOMLayer.saveState) Finished saving state of CID " + lastCID);
    }

    /**
     * Write commands to log file
     * @param commands array of commands. Each command is an array of bytes
     * @param msgCtx
     */
    private void saveCommands(byte[][] commands, MessageContext[] msgCtx) {
        if(!config.isToLog())
            return;

        if (commands.length != msgCtx.length) {
            logger.info("SIZE OF COMMANDS AND MESSAGE CONTEXTS IS DIFFERENT----");
            logger.info("COMMANDS: " + commands.length + ", CONTEXTS: " + msgCtx.length + " ----");
        }

        logLock.lock();

        int cid = msgCtx[0].getConsensusId();
        int batchStart = 0;
        for(int i = 0; i <= msgCtx.length; i++) {
            if(i == msgCtx.length) { // the batch command contains only one command or it is the last position of the array
                byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                log.addMessageBatch(batch, batchMsgCtx, cid);
                log.setLastCID(cid, globalCheckpointPeriod, checkpointPortion);
                //				if(batchStart > 0)
                //					System.out.println("Last batch: " + commands.length + "," + batchStart + "-" + i + "," + batch.length);
            } else {
                if(msgCtx[i].getConsensusId() > cid) { // saves commands when the CID changes or when it is the last batch
                    byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                    MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                    //					System.out.println("THERE IS MORE THAN ONE CID in this batch." + commands.length + "," + batchStart + "-" + i + "," + batch.length);
                    log.addMessageBatch(batch, batchMsgCtx, cid);
                    log.setLastCID(cid, globalCheckpointPeriod, checkpointPortion);
                    cid = msgCtx[i].getConsensusId();
                    batchStart = i;
                }
            }
        }
        logLock.unlock();
    }


    public CSTState getState(CSTRequest cstRequest) {
        CSTState ret = log.getState(cstRequest);
        return ret;
    }

    @Override
    public void setReplicaContext(ReplicaContext replicaContext) {
        this.config = replicaContext.getStaticConfiguration();
        if(log == null) {
            globalCheckpointPeriod = config.getGlobalCheckpointPeriod();
            replicaCkpIndex = getCheckpointPortionIndex();
            checkpointPortion = globalCheckpointPeriod / config.getN();

//			byte[] state = getSnapshot();
            if(config.isToLog()) {
                int replicaId = config.getProcessId();
                boolean isToLog = config.isToLog();
                boolean syncLog = config.isToWriteSyncLog();
                boolean syncCkp = config.isToWriteSyncCkp();
//				log = new DurableStateLog(replicaId, state, computeHash(state), isToLog, syncLog, syncCkp);
                log = new DurableStateLog(replicaId, null, null, isToLog, syncLog, syncCkp);
                CSTState storedState = log.loadDurableState();
                if(storedState.getLastCID() > -1) {
                    logger.info("LAST CID RECOVERED FROM LOG: " + storedState.getLastCID());
                    setState(storedState);
                    getStateManager().setLastCID(storedState.getLastCID());
                } else {
                    logger.info("REPLICA IS IN INITIAL STATE");
                }
            }
            getStateManager().askCurrentConsensusId();
        }
    }

    private int getCheckpointPortionIndex() {
        int numberOfReplicas = config.getN();
        int ckpIndex = ((globalCheckpointPeriod / numberOfReplicas) * (config.getProcessId() + 1)) -1;
        return ckpIndex;
    }

    /**
     * Iterates over the message context array and get the consensus id of each command
     * being executed. As several times during the execution of commands and logging the
     * only information important in MessageContext is the consensus id, it saves time to
     * have it already in an array of ids
     * @param ctxs the message context, one for each command to be executed
     * @return the id of the consensus decision for each command
     */
    private int[] consensusIds(MessageContext[] ctxs) {
        int[] cids = new int[ctxs.length];
        for(int i = 0; i < ctxs.length; i++)
            cids[i] = ctxs[i].getConsensusId();
        return cids;
    }

    @Override
    public StateManager getStateManager() {
        if(stateManager == null)
            stateManager = new DurableStateManager();
        return stateManager;
    }

    public byte[] getCurrentStateHash() {
        byte[] currentState = getSnapshot();
        byte[] currentStateHash = TOMUtil.computeHash(currentState);
        logger.info("State size: " + currentState.length + " Current state Hash: " + Arrays.toString(currentStateHash));
        return currentStateHash;
    }



    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return appExecuteUnordered(command, msgCtx);
    }

    @Override
    public void Op(int CID, byte[] requests, MessageContext msgCtx) {
        //Requests are logged within 'executeBatch(...)' instead of in this method.
    }

    @Override
    public void noOp(int CID, byte[][] operations, MessageContext[] msgCtxs) {

        for (int i = 0; i < msgCtxs.length; i++) {
            executeOrdered(operations[i], msgCtxs[i], true);
        }

    }

    /**
     * Given a snapshot received from the state transfer protocol, install it
     * @param state The serialized snapshot
     */
    public abstract void installSnapshot(byte[] state);

    /**
     * Returns a serialized snapshot of the application state
     * @return A serialized snapshot of the application state
     */
    public abstract byte[] getSnapshot();

    /**
     * Execute a batch of ordered requests
     *
     * @param command The ordered request
     * @param msgCtx The context associated to each request
     *
     * @return the reply for the request issued by the client
     */
    public abstract byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx);

    /**
     * Execute an unordered request
     *
     * @param command The unordered request
     * @param msgCtx The context associated to the request
     *
     * @return the reply for the request issued by the client
     */
    public abstract byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx);
}
