/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.statemanagement.strategy.staticdivide;

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.statemanagement.strategy.sort.SortStateManager;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

public class StaticDivideStateManager extends SortStateManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    protected void requestState() {
        if (SVController.getStaticConf().getSourceOrder() == null || SVController.getStaticConf().getNumbersOfStateChunks() == null) {
            logger.debug("Required settings do not exist. Fallback to SortStateManager.");
            fallbackRequestState();
            return;
        }

        logger.info("[Time] Request State Transfer Start: " + System.currentTimeMillis());

        if (tomLayer.requestsTimer != null) {
            tomLayer.requestsTimer.clearAll();
        }

        int chunksCount = 0;
        for (int id : SVController.getCurrentViewOtherAcceptors()) {
            int chunksNum = getStateChunksNum(id);
            StaticDivideSMMessage msg = new StaticDivideSMMessage(SVController.getStaticConf().getProcessId(), waitingCID, TOMUtil.SM_REQUEST,
                    0, chunksCount, chunksCount + chunksNum - 1, null, null, -1, -1);
            chunksCount += chunksNum;
            tomLayer.getCommunication().send(new int[]{id}, msg);
        }

        logger.info("I just sent a request to the other replicas for the state up to CID " + waitingCID);
        logger.info("[Time] Send Request End: " + System.currentTimeMillis());

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                int[] myself = new int[1];
                myself[0] = SVController.getStaticConf().getProcessId();
                tomLayer.getCommunication().send(myself, new StandardSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, -1, null, null, -1, -1));
            }
        };

        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask, timeout);
    }

    private int getStateChunksNum(int replicaId) {
        int[] stateChunksNums = SVController.getStaticConf().getNumbersOfStateChunks();
        int[] sourceOrder = SVController.getStaticConf().getSourceOrder();
        for (int i = 0; i < sourceOrder.length; i++) {
            if (sourceOrder[i] == replicaId) {
                return stateChunksNums[i];
            }
        }
        logger.error("Failed to find number of state chunks");
        return -1;
    }

    private void fallbackRequestState() {
        super.requestState();
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        logger.debug("Timeout for the replica that was supposed to send the complete state. Fallback to SortStateManager.");
        if (stateTimer != null)
            stateTimer.cancel();
        reset();
        fallbackRequestState();
        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (!(msg instanceof StaticDivideSMMessage)) {
            super.SMRequestDeliver(msg, isBFT);
            return;
        }

        if (!SVController.getStaticConf().isStateTransferEnabled() || dt.getRecoverer() == null) {
            return;
        }

        logger.info("[Time] Send Reply Start: " + System.currentTimeMillis());

        StaticDivideSMMessage alMsg = (StaticDivideSMMessage) msg;
        int[] targets = {msg.getSender()};
        SMMessage replyMsg = buildReplySMMessage(alMsg);
        ApplicationState replyState = replyMsg.getState();
        logger.info("state size is " + (replyState.getSerializedState() != null ? replyState.getSerializedState().length : "null"));
        if (replyState instanceof DefaultApplicationState) {
            DefaultApplicationState das = (DefaultApplicationState) replyState;
            logger.info("batch size is " + (das.getMessageBatches() != null ? das.getMessageBatches().length : "null"));
        }
        logger.info("Sending state...");
        tomLayer.getCommunication().send(targets, replyMsg);
        logger.info("[Time] Send Reply End: " + System.currentTimeMillis());
        logger.info("Sent");
    }

    private SMMessage buildReplySMMessage(StaticDivideSMMessage reqMsg) {
        ApplicationState thisState = dt.getRecoverer().getState(reqMsg.getCID(), true);
        if (thisState == null || thisState.getSerializedState() == null) {
            logger.warn("For some reason, I am sending a void state");
            ApplicationState voidState = dt.getRecoverer().getState(-1, true);
            return new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    reqMsg.getCID(), TOMUtil.SM_REPLY, -1, voidState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());
        }

        int stateSize = thisState.getSerializedState().length;
        int totalChunksNum = SVController.getStaticConf().getTotalNumberOfChunks();
        if (stateSize < totalChunksNum) {
            logger.debug("State's length is less total chunk num. Fallback to SortStateManager.");
            ApplicationState replyState;
            if (reqMsg.getStartStateChunk() == 0) {
                replyState = thisState;
            } else {
                replyState = dt.getRecoverer().getState(reqMsg.getCID(), false);
            }
            return new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    reqMsg.getCID(), TOMUtil.SM_REPLY, -1, replyState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());
        }

        int chunkSize = stateSize / (totalChunksNum - 1);
        int startChunk = reqMsg.getStartStateChunk();
        int endChunk = reqMsg.getEndStateChunk();

        logger.info("start: " + startChunk + ", end: " + endChunk);

        int startByte = startChunk * chunkSize;
        int endByte;
        // Arrays.copyOfRange's third argument means "less than"
        if (endChunk != (totalChunksNum - 1)) {
            endByte = (endChunk + 1) * chunkSize;
        } else {
            endByte = stateSize;
        }
        thisState.setSerializedState(Arrays.copyOfRange(thisState.getSerializedState(), startByte, endByte));
        return new StaticDivideSMMessage(SVController.getStaticConf().getProcessId(), reqMsg.getCID(), TOMUtil.SM_REPLY,
                stateSize, startChunk, endChunk, thisState, SVController.getCurrentView(),
                tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        if (!(msg instanceof StaticDivideSMMessage)) {
            super.SMReplyDeliver(msg, isBFT);
            return;
        }
        StaticDivideSMMessage alMsg = (StaticDivideSMMessage) msg;

        logger.info("[Time] Receive State (" + (senderStates.size() + 1) + "/" + (SVController.getCurrentViewN() - 1) + "): " + System.currentTimeMillis());

        logger.info("state size: " + alMsg.getStateSize() + " start: " + alMsg.getStartStateChunk() + " end: " + alMsg.getEndStateChunk());

        lockTimer.lock();
        if (!SVController.getStaticConf().isStateTransferEnabled()) {
            lockTimer.unlock();
            return;
        }
        if (waitingCID == -1 || msg.getCID() != waitingCID) {
            lockTimer.unlock();
            return;
        }

        int currentRegency;
        int currentLeader;
        View currentView;
        CertifiedDecision currentProof;
        if (!appStateOnly) {
            int msgSender = msg.getSender();
            int msgRegency = msg.getRegency();
            int msgLeader = msg.getLeader();
            View msgView = msg.getView();
            CertifiedDecision msgCD = msg.getState().getCertifiedDecision(SVController);

            senderRegencies.put(msgSender, msgRegency);
            senderLeaders.put(msgSender, msgLeader);
            senderViews.put(msgSender, msgView);
            senderProofs.put(msgSender, msgCD);

            currentRegency = enoughRegencies(msgRegency) ? msgRegency : -1;
            currentLeader = enoughLeaders(msgLeader) ? msgLeader : -1;
            currentView = enoughViews(msgView) ? msgView : null;
            currentProof = enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()) ? msgCD : null;
        } else {
            currentLeader = tomLayer.execManager.getCurrentLeader();
            currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
            currentView = SVController.getCurrentView();
            currentProof = null;
        }

        byte[] serializedState;
        if (state == null) {
            state = alMsg.getState();
            serializedState = new byte[alMsg.getStateSize()];
        } else {
            serializedState = state.getSerializedState();
        }
        int totalChunksNum = SVController.getStaticConf().getTotalNumberOfChunks();
        int chunkSize = alMsg.getStateSize() / (totalChunksNum - 1);
        System.arraycopy(alMsg.getState().getSerializedState(), 0,
                serializedState, alMsg.getStartStateChunk() * chunkSize,
                alMsg.getState().getSerializedState().length);
        state.setSerializedState(serializedState);

        senderStates.put(msg.getSender(), msg.getState());

        logger.debug("Verifying came all replies");
        if (senderStates.size() < SVController.getCurrentViewN() - 1) {
            lockTimer.unlock();
            return;
        }

        logger.debug("came all replies confirmed");

        logger.info("[Time] Get State End: " + System.currentTimeMillis());

        ApplicationState otherReplicaState = getOtherReplicaState();
        if (otherReplicaState == null) {
            waitingCID = -1;
            reset();
            if (stateTimer != null) {
                stateTimer.cancel();
            }
            if (appStateOnly) {
                fallbackRequestState();
            }
            return;
        }

        if (!Arrays.equals(tomLayer.computeHash(state.getSerializedState()), otherReplicaState.getStateHash())) {
            logger.debug("The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");
            reset();
            fallbackRequestState();
            if (stateTimer != null) {
                stateTimer.cancel();
            }
            lockTimer.unlock();
            return;
        }

        if (currentRegency == -1 || currentLeader == -1 || currentView == null) {
            logger.debug("State transfer not yet finished");
            lockTimer.unlock();
            return;
        }

        if (isBFT && currentProof == null && !appStateOnly) {
            logger.debug("State transfer not yet finished");
            lockTimer.unlock();
            return;
        }

        logger.info("[Time] Hash Check End: " + System.currentTimeMillis());

        logger.info("Received state. Will install it");

        tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
        tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
        tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);
        tomLayer.execManager.setNewLeader(currentLeader);

        if (currentProof != null && !appStateOnly) {
            logger.debug("Installing proof for consensus " + waitingCID);
            Consensus cons = execManager.getConsensus(waitingCID);
            Epoch e = null;
            for (ConsensusMessage cm : currentProof.getConsMessages()) {
                e = cons.getEpoch(cm.getEpoch(), true, SVController);
                if (e.getTimestamp() != cm.getEpoch()) {
                    logger.warn("Strange... proof contains messages from more than just one epoch");
                    e = cons.getEpoch(cm.getEpoch(), true, SVController);
                }
                e.addToProof(cm);
                if (cm.getType() == MessageFactory.ACCEPT) {
                    e.setAccept(cm.getSender(), cm.getValue());
                } else if (cm.getType() == MessageFactory.WRITE) {
                    e.setWrite(cm.getSender(), cm.getValue());
                }
            }

            if (e != null) {
                e.propValueHash = tomLayer.computeHash(currentProof.getDecision());
                e.propValue = currentProof.getDecision();
                e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(), false);
                cons.decided(e, false);
                logger.info("Successfully installed proof for consensus " + waitingCID);
            } else {
                logger.error("Failed to install proof for consensus " + waitingCID);
            }
        }

        // I might have timed out before invoking the state transfer, so
        // stop my re-transmission of STOP messages for all regencies up to the current one
        if (currentRegency > 0) {
            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
        }
        //if (currentRegency > 0)
        //    tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() * (currentRegency * 2));

        dt.deliverLock();
        waitingCID = -1;
        dt.update(state);

        if (!appStateOnly && execManager.stopped()) {
            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
            for (ConsensusMessage stopped : stoppedMsgs) {
                if (stopped.getNumber() > state.getLastCID() /*msg.getCID()*/) {
                    execManager.addOutOfContextMessage(stopped);
                }
            }
            execManager.clearStopped();
            execManager.restart();
        }

        tomLayer.processOutOfContext();

        if (SVController.getCurrentViewId() != currentView.getId()) {
            logger.info("Installing current view!");
            SVController.reconfigureTo(currentView);
        }

        isInitializing = false;

        dt.canDeliver();
        dt.deliverUnlock();

        reset();

        logger.info("[Time] Install End: " + System.currentTimeMillis());

        logger.info("I updated the state!");

        tomLayer.requestsTimer.Enabled(true);
        tomLayer.requestsTimer.startTimer();
        if (stateTimer != null) stateTimer.cancel();

        if (appStateOnly) {
            appStateOnly = false;
            tomLayer.getSynchronizer().resumeLC();
        }

        lockTimer.unlock();
    }
}
