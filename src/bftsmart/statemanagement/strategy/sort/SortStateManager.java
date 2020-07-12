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
package bftsmart.statemanagement.strategy.sort;

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.BaseStateManager;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.leaderchange.LCManager;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class SortStateManager extends BaseStateManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int replicaId = -1;
    private int replicaIndex = -1;
    protected ReentrantLock lockTimer = new ReentrantLock();
    protected Timer stateTimer = null;
    protected final static long INIT_TIMEOUT = 180000;
    protected long timeout = INIT_TIMEOUT;

    //private LCManager lcManager;
    protected ExecutionManager execManager;

    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {
        SVController = tomLayer.controller;

        this.tomLayer = tomLayer;
        this.dt = dt;
        //this.lcManager = tomLayer.getSyncher().getLCManager();
        this.execManager = tomLayer.execManager;

        // chooseReplicaFromConfig(); // initialize replica from which to ask the complete state

        //this.replica = 0;
        //if (SVController.getCurrentViewN() > 1 && replica == SVController.getStaticConf().getProcessId())
        //	changeReplica();

        state = null;
        lastCID = -1;
        waitingCID = -1;

        appStateOnly = false;
    }

    private void changeReplica() {

        int[] processes = this.SVController.getCurrentViewOtherAcceptors();
        Random r = new Random();

        int pos;
        do {
            //pos = this.SVController.getCurrentViewPos(replica);
            //replica = this.SVController.getCurrentViewProcesses()[(pos + 1) % SVController.getCurrentViewN()];

            if (processes != null && processes.length > 1) {
                pos = r.nextInt(processes.length);
                replicaId = processes[pos];
            } else {
                replicaId = 0;
                break;
            }
        } while (replicaId == SVController.getStaticConf().getProcessId());
    }

    private void chooseReplicaFromConfig() {
        int[] sourceOrder = SVController.getStaticConf().getSourceOrder();
        int[] processes = this.SVController.getCurrentViewOtherAcceptors();
        int nextIndex = replicaIndex + 1;

        if (sourceOrder == null || nextIndex > sourceOrder.length || !containsInArray(processes, sourceOrder[nextIndex])) {
            changeReplica();
            return;
        }

        replicaIndex++;
        replicaId = sourceOrder[replicaIndex];
    }

    private boolean containsInArray(int[] array, int num) {
        for (int item : array) {
            if (item == num) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void requestState() {
        if (tomLayer.requestsTimer != null)
            tomLayer.requestsTimer.clearAll();

        chooseReplicaFromConfig();

        SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                waitingCID, TOMUtil.SM_REQUEST, replicaId, null, null, -1, -1);
        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), smsg);

        logger.info("I just sent a request to the other replicas for the state up to CID " + waitingCID);

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

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        logger.debug("Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
        if (stateTimer != null)
            stateTimer.cancel();
        reset();
        requestState();
        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (SVController.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
            StandardSMMessage stdMsg = (StandardSMMessage) msg;
            boolean sendState = stdMsg.getReplica() == SVController.getStaticConf().getProcessId();

            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);
            if (thisState == null) {

                logger.warn("For some reason, I am sending a void state");
                thisState = dt.getRecoverer().getState(-1, sendState);
            }

            int[] targets = {msg.getSender()};
            SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    msg.getCID(), TOMUtil.SM_REPLY, -1, thisState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());

            logger.info("state size is " + (thisState.getSerializedState() != null ? thisState.getSerializedState().length : "null"));
            logger.info("Sending state...");
            tomLayer.getCommunication().send(targets, smsg);
            logger.info("Sent");
        }
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
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
            LCManager lcm = this.tomLayer.getSynchronizer().getLCManager();
            currentProof = enoughProofs(waitingCID, lcm) ? msgCD : null;
        } else {
            currentLeader = tomLayer.execManager.getCurrentLeader();
            currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
            currentView = SVController.getCurrentView();
            currentProof = null;
        }

        if (msg.getSender() == replicaId && msg.getState().getSerializedState() != null) {
            logger.debug("Expected replica sent state. Setting it to state");
            state = msg.getState();
            if (stateTimer != null) {
                stateTimer.cancel();
            }
        }

        senderStates.put(msg.getSender(), msg.getState());

        logger.debug("Verifying more than F replies");
        if (!enoughReplies()) {
            lockTimer.unlock();
            return;
        }

        logger.debug("More than F confirmed");
        if (state == null) {
            int replyNum = getReplies();
            if (replyNum >= (SVController.getCurrentViewN() - SVController.getCurrentViewF())) {
                logger.debug("Could not obtain the state, retrying");
                reset();
                if (stateTimer != null) stateTimer.cancel();
                waitingCID = -1;
            }
            lockTimer.unlock();
            return;
        }

        ApplicationState otherReplicaState = getOtherReplicaState();
        if (otherReplicaState == null) {
            int replyNum = getReplies();
            if (replyNum > (SVController.getCurrentViewN() / 2)) {
                waitingCID = -1;
                reset();
                if (stateTimer != null) {
                    stateTimer.cancel();
                }
                if (appStateOnly) {
                    requestState();
                }
            } else if (replyNum >= (SVController.getCurrentViewN() - SVController.getCurrentViewF())) {
                logger.debug("Could not obtain the state, retrying");
                reset();
                if (stateTimer != null) stateTimer.cancel();
                waitingCID = -1;
            } else {
                logger.debug("State transfer not yet finished");
            }
            lockTimer.unlock();
            return;
        }

        if (!Arrays.equals(tomLayer.computeHash(state.getSerializedState()), otherReplicaState.getStateHash())) {
            int replyNum = getReplies();
            if (getNumEqualStates() > SVController.getCurrentViewF()) {
                logger.debug("The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");
                reset();
                requestState();
                if (stateTimer != null) stateTimer.cancel();
            } else if (replyNum >= (SVController.getCurrentViewN() - SVController.getCurrentViewF())) {
                logger.debug("Could not obtain the state, retrying");
                reset();
                if (stateTimer != null) stateTimer.cancel();
                waitingCID = -1;
            } else {
                logger.debug("State transfer not yet finished");
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
        replicaIndex = -1;

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

    /**
     * Search in the received states table for a state that was not sent by the expected
     * replica. This is used to compare both states after received the state from expected
     * and other replicas.
     *
     * @return The state sent from other replica
     */
    protected ApplicationState getOtherReplicaState() {
        int[] processes = SVController.getCurrentViewProcesses();
        for (int process : processes) {
            if (process == replicaId)
                continue;
            else {
                ApplicationState otherState = senderStates.get(process);
                if (otherState != null)
                    return otherState;
            }
        }
        return null;
    }

    private int getNumEqualStates() {
        List<ApplicationState> states = new ArrayList<ApplicationState>(receivedStates());
        int match = 0;
        for (ApplicationState st1 : states) {
            int count = 0;
            for (ApplicationState st2 : states) {
                if (st1 != null && st1.equals(st2))
                    count++;
            }
            if (count > match)
                match = count;
        }
        return match;
    }

    @Override
    public void currentConsensusIdAsked(int sender) {
        int me = SVController.getStaticConf().getProcessId();
        int lastConsensusId = tomLayer.getLastExec();
        SMMessage currentCID = new StandardSMMessage(me, lastConsensusId, TOMUtil.SM_REPLY_INITIAL, 0, null, null, 0, 0);
        tomLayer.getCommunication().send(new int[]{sender}, currentCID);
    }

}
