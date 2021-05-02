/*
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
package bftsmart.statemanagement.strategy.dynamicdivide;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

public class DynamicDivideStateManager extends SortStateManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private StateReceiver stateReceiver = null;
    private StateSender stateSender = null;
    private final List<Long> timeLog = new ArrayList<>(Collections.nCopies(5, 0L));
    private long oldA = 0;

    @Override
    protected void requestState() {
        if (SVController.getStaticConf().getSourceOrder() == null || SVController.getStaticConf().getNumbersOfStateChunks() == null) {
            logger.warn("Required settings do not exist. Fallback to SortStateManager.");
            fallbackRequestState();
            return;
        }

        if (tomLayer.requestsTimer != null) {
            tomLayer.requestsTimer.clearAll();
        }


        if (stateReceiver != null) {
            logger.info("state receiver is still running.");
            return;
        }

        logger.info("[Time] Request State Transfer Start: " + System.currentTimeMillis());
        stateReceiver = new StateReceiver(tomLayer, SVController, replicaId, waitingCID);

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                sendTimeoutMessage();
            }
        };
        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask, timeout);

        stateReceiver.startSendingRequest();
    }

    private void fallbackRequestState() {
        super.requestState();
    }

    private void sendTimeoutMessage() {
        int[] myself = new int[1];
        myself[0] = SVController.getStaticConf().getProcessId();
        tomLayer.getCommunication().send(myself, new StandardSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, -1, null, null, -1, -1));
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
        if (!(msg instanceof DynamicDivideSMRequestMessage)) {
            super.SMRequestDeliver(msg, isBFT);
            return;
        }
        if (!SVController.getStaticConf().isStateTransferEnabled() || dt.getRecoverer() == null) {
            return;
        }

        if (stateSender == null || stateSender.getCID() != msg.getCID()) {
            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), true);
            if (thisState == null || thisState.getSerializedState() == null) {
                logger.warn("For some reason, I am sending a void state");
                stateSender.sendVoidState(msg.getSender(), dt.getRecoverer(), msg.getCID());
                return;
            }
            if (thisState.getSerializedState().length < SVController.getStaticConf().getTotalNumberOfChunks()) {
                logger.warn("State's length is less total chunk num. Fallback to SortStateManager.");
                super.SMRequestDeliver(msg, isBFT);
                return;
            }
            stateSender = new StateSender(tomLayer, SVController, (DefaultApplicationState) thisState, replicaId);
        }

        DynamicDivideSMRequestMessage alReq = (DynamicDivideSMRequestMessage) msg;
        stateSender.update(alReq.getSender(), alReq.getChunkIds(), alReq.getHashIds(), alReq.getCID());
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        long a = System.nanoTime();
        if (!(msg instanceof DynamicDivideSMReplyMessage)) {
            logger.debug("not dynamic divide reply " + msg);
            super.SMReplyDeliver(msg, isBFT);
            return;
        }
        DynamicDivideSMReplyMessage alMsg = (DynamicDivideSMReplyMessage) msg;
        long b = System.nanoTime();

        logger.debug("chiba: receive chunk id = " + alMsg.getChunkId());
        lockTimer.lock();
        if (!SVController.getStaticConf().isStateTransferEnabled()) {
            logger.debug("guarded due to isStateTransferEnabled() is false");
            lockTimer.unlock();
            return;
        }
        if (waitingCID == -1 || msg.getCID() != waitingCID) {
            logger.debug("guarded due to waitingCID = " + waitingCID + " and msg.getCID = " + msg.getCID());
            lockTimer.unlock();
            return;
        }

        long c = System.nanoTime();

        stateReceiver.receiveStateChunk(alMsg);

        long d = System.nanoTime();
        int currentRegency;
        int currentLeader;
        View currentView;
        CertifiedDecision currentProof;

        if (!appStateOnly && alMsg.hasHashes()) {
            int msgSender = msg.getSender();
            CertifiedDecision msgCD = msg.getState().getCertifiedDecision(SVController);

            senderRegencies.put(msgSender, msg.getRegency());
            senderLeaders.put(msgSender, msg.getLeader());
            senderViews.put(msgSender, msg.getView());
            senderProofs.put(msgSender, msgCD);
        }

        long e = System.nanoTime();

        timeLog.set(0, timeLog.get(0) + b - a);
        timeLog.set(1, timeLog.get(1) + c - b);
        timeLog.set(2, timeLog.get(2) + d - c);
        timeLog.set(3, timeLog.get(3) + e - d);
        timeLog.set(4, timeLog.get(4) + (oldA == 0L ? 0 : oldA - e));
        oldA = a;

        if (!stateReceiver.isReceivedAllChunks()) {
            return;
        }

        if (state == null) {
            state = stateReceiver.getState();
        }
        senderStates.put(msg.getSender(), msg.getState());

        logger.info("[Time] Get State End: " + System.currentTimeMillis());

        if (!appStateOnly) {
            CertifiedDecision msgCD = state.getCertifiedDecision(SVController);
            currentRegency = enoughRegencies(msg.getRegency()) ? msg.getRegency() : -1;
            currentLeader = enoughLeaders(msg.getLeader()) ? msg.getLeader() : -1;
            currentView = enoughViews(msg.getView()) ? msg.getView() : null;
            currentProof = enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()) ? msgCD : null;
        } else {
            currentLeader = tomLayer.execManager.getCurrentLeader();
            currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
            currentView = SVController.getCurrentView();
            currentProof = null;
        }

        logger.debug("senderRegencies: " + senderRegencies);
        logger.debug("senderLeaders: " + senderLeaders);
        logger.debug("senderViews: " + senderViews);
        logger.debug("senderProofs: " + senderProofs);
        logger.debug("cr: " + currentRegency + ", cl: " + currentLeader + ", cv: " + currentView);

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

        installState(currentRegency, currentLeader, currentView, currentProof);

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

    protected void installState(int currentRegency, int currentLeader, View currentView, CertifiedDecision currentProof) {
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
    }

    @Override
    public void requestAppState(int cid) {
        if (stateReceiver != null) {
            logger.debug("state receiver is still running.");
            return;
        }
        lastCID = cid + 1;
        waitingCID = cid;
        logger.debug("Updated waitingcid to " + cid);
        appStateOnly = true;
        requestState();
    }

    @Override
    protected void reset() {
        super.reset();
        stateReceiver.reset();
        stateReceiver = null;
    }
}
