package bftsmart.statemanagement.strategy.dynamicdivide;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StateReceiver {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final TOMLayer tomLayer;
    private final ServerViewController SVController;
    private final int requestInterval;
    private final int waitingCID;
    private final int processId;
    private final int replicaId;
    private final Map<Integer, Integer> initialStateChunksNums;
    private Timer trafficTimer = null;
    private final ChunkCollector chunkCollector;
    private final HashCollector hashCollector;
    private Map<Integer, BitSet> lastDividedChunks = null;
    private ApplicationState state = null;
    private int lowTrafficCount = 0;

    public StateReceiver(TOMLayer tomLayer,
                         ServerViewController SVController,
                         int replicaId,
                         int waitingCID) {
        this.tomLayer = tomLayer;
        this.SVController = SVController;
        this.replicaId = replicaId;
        this.waitingCID = waitingCID;
        requestInterval = SVController.getStaticConf().getStateRequestInterval();
        processId = SVController.getStaticConf().getProcessId();
        int[] sourceOrder = SVController.getStaticConf().getSourceOrder();
        int[] numbersOfStateChunks = SVController.getStaticConf().getNumbersOfStateChunks();
        initialStateChunksNums = new HashMap<>();
        for (int i = 0; i < sourceOrder.length; i++) {
            initialStateChunksNums.put(sourceOrder[i], numbersOfStateChunks[i]);
        }
        int totalChunkNum = SVController.getStaticConf().getTotalNumberOfChunks();
        if (!SVController.getStaticConf().getUsesWholeHash()) {
            hashCollector = new HashCollector(SVController, totalChunkNum);
        } else {
            hashCollector = new WholeHashCollector(SVController, totalChunkNum);
        }
        chunkCollector = new ChunkCollector(totalChunkNum, hashCollector);
    }

    public void startSendingRequest() {
        logger.info("[Time] Request State Transfer Start: " + System.currentTimeMillis());

        if (tomLayer.requestsTimer != null) {
            tomLayer.requestsTimer.clearAll();
        }

        logger.info("I just sent a request to the other replicas for the state up to CID " + waitingCID);

        if (trafficTimer != null) {
            trafficTimer.cancel();
        }
        trafficTimer = new Timer("traffic timer");

        hashCollector.reset();
        chunkCollector.reset();

        Map<Integer, BitSet> chunkIdsMap = initialDivideRequestChunkIds();
        logger.info("[updateSendRequest] sendRequestMessage start: " + System.currentTimeMillis());
        for (Map.Entry<Integer, BitSet> chunkIdsEntry : chunkIdsMap.entrySet()) {
            logger.info("[updateSendRequest] RequestChunkIds(" + chunkIdsEntry.getKey() + "): " + chunkIdsEntry.getValue());
            sendRequestMessage(chunkIdsEntry.getKey(), chunkIdsEntry.getValue(), true);
        }
        logger.info("[updateSendRequest] sendRequestMessage end: " + System.currentTimeMillis());
        trafficTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                logger.info("[updateSendRequest] sendRequestMessage start: " + System.currentTimeMillis());
                Map<Integer, Long> trafficOfConnections = tomLayer.getCommunication().getServersConn().getTrafficOfConnections();
                logger.info("[TrafficTimer] traffic: " + trafficOfConnections);
                alertIfTrafficIsTooLow(trafficOfConnections.values().stream().mapToLong(x -> x).sum());
                Map<Integer, BitSet> chunkIdsMap = divideRequestChunkIds(trafficOfConnections);
                for (Map.Entry<Integer, BitSet> chunkIdsEntry : chunkIdsMap.entrySet()) {
                    logger.info("[updateSendRequest] RequestChunkIds(" + chunkIdsEntry.getKey() + "): " + chunkIdsEntry.getValue());
                    sendRequestMessage(chunkIdsEntry.getKey(), chunkIdsEntry.getValue(), false);
                }
                logger.info("[updateSendRequest] sendRequestMessage end: " + System.currentTimeMillis());
            }
        }, requestInterval, requestInterval);
    }

    public void receiveStateChunk(DynamicDivideSMReplyMessage replyMessage) {
        if (replyMessage.hasHashes()) {
            boolean successful = hashCollector.addHashes(replyMessage.getSender(), replyMessage.getHashIds(), replyMessage.getHashes());
            if (successful) {
                chunkCollector.addPendingChunks();
                if (state == null) {
                    state = new DefaultApplicationState((DefaultApplicationState) replyMessage.getState());
                }
            }
        }
        logger.info("[receiveStateChunk] received from " + replyMessage.getSender() + " , chunkId: " + replyMessage.getChunkId());
        chunkCollector.addChunk(replyMessage.getChunkId(), replyMessage.getState().getSerializedState());
    }

    public boolean isReceivedAllChunks() {
        return chunkCollector.getRestChunkNum() == 0;
    }

    public ApplicationState getState() {
        if (state == null) {
            return null;
        }

        byte[] serializedState = chunkCollector.getState();

        if (SVController.getStaticConf().getUsesWholeHash()) {
            logger.info("[Time] verify whole hash start: " + System.currentTimeMillis());
            assert hashCollector instanceof WholeHashCollector;
            boolean isCorrect = ((WholeHashCollector) hashCollector).verifyWhole(serializedState);
            logger.info("[Time] verify whole hash end: " + System.currentTimeMillis());
            if (!isCorrect) {
                return null;
            }
        }

        state.setSerializedState(serializedState);
        return state;
    }

    public void reset() {
        trafficTimer.cancel();
    }

    private void alertIfTrafficIsTooLow(Long traffic) {
        if (traffic / (double) requestInterval * 1000 > 1000 * 1000) {
            lowTrafficCount = 0;
            return;
        }
        lowTrafficCount += 1;
        if (lowTrafficCount > 10 * (1000 / (double) requestInterval)) {
            logger.warn("[TrafficAlert] traffic is too low for " + (lowTrafficCount * (1000 / (double) requestInterval)) + " seconds");
        }
    }

    private void sendRequestMessage(int serverId, BitSet chunkIds, boolean isFirstRequest) {
        BitSet hashIds = hashCollector.getRequiredHashIds(isFirstRequest, chunkIds);
        DynamicDivideSMRequestMessage msg = new DynamicDivideSMRequestMessage(processId, waitingCID, TOMUtil.SM_REQUEST, replicaId, chunkIds, hashIds, null, null, -1, -1);
        tomLayer.getCommunication().send(new int[]{serverId}, msg);
    }

    private Map<Integer, BitSet> divideRequestChunkIds(Map<Integer, Long> trafficOfConnections) {
        if (lastDividedChunks == null || trafficOfConnections.isEmpty()) {
            return initialDivideRequestChunkIds();
        }
        Map<Integer, BitSet> dividedChunks = new HashMap<>(lastDividedChunks);
        BitSet restChunkIds = chunkCollector.getRestChunkIds();
        dividedChunks.forEach((k, v) -> v.and(restChunkIds));
        Map<Integer, Integer> oldStateChunksNums = dividedChunks.entrySet().stream()
                .map(e -> Maps.immutableEntry(e.getKey(), e.getValue().cardinality()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Integer, Integer> stateChunksNums = calcStateChunksNumbers(trafficOfConnections);
        Map<Integer, Integer> stateChunksNumsDiff = stateChunksNums.keySet().stream()
                .map(key -> Maps.immutableEntry(key, oldStateChunksNums.get(key) - stateChunksNums.get(key)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int diffNum = stateChunksNumsDiff.values().stream().mapToInt(Integer::intValue).map(Math::abs).sum() / 2;

        IntStream.range(0, diffNum).forEach(i -> {
            int maxKey = stateChunksNumsDiff.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getValue)).orElseThrow(() -> new RuntimeException("unexpected null")).getKey();
            int minKey = stateChunksNumsDiff.entrySet().stream().min(Comparator.comparingInt(Map.Entry::getValue)).get().getKey();
            BitSet maxChunkIds = dividedChunks.get(maxKey);
            int flipIndex = maxChunkIds.previousSetBit(maxChunkIds.length() - 1);
            dividedChunks.get(maxKey).flip(flipIndex);
            dividedChunks.get(minKey).flip(flipIndex);
            stateChunksNumsDiff.put(maxKey, stateChunksNumsDiff.get(maxKey) - 1);
            stateChunksNumsDiff.put(minKey, stateChunksNumsDiff.get(minKey) + 1);
        });
        // add a chunkId if has no chunkIds
        if (dividedChunks.values().stream().anyMatch(x -> x.cardinality() == 0)) {
            BitSet maxReq = dividedChunks.values().stream().max(Comparator.comparingInt(BitSet::cardinality)).orElseThrow(() -> new RuntimeException("unexpected null"));
            int minChunkIdOrError = maxReq.nextSetBit(0);
            int minChunkId = minChunkIdOrError != -1 ? minChunkIdOrError : 0;
            dividedChunks.values().stream().filter(x -> x.cardinality() == 0).forEach(x -> x.set(minChunkId));
        }
        lastDividedChunks = dividedChunks;
        return dividedChunks;
    }

    private Map<Integer, BitSet> initialDivideRequestChunkIds() {
        Map<Integer, Integer> stateChunksNumbers = calcStateChunksNumbers();
        Map<Integer, BitSet> dividedChunks = new HashMap<>(stateChunksNumbers.keySet().stream()
                .map(k -> Maps.immutableEntry(k, new BitSet()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        if (stateChunksNumbers.isEmpty()) {
            return new HashMap<>();
        }
        int minNumber = stateChunksNumbers.values().stream().mapToInt(Integer::intValue).min().getAsInt();
        Map<Integer, Integer> chunkDivideRatio = stateChunksNumbers.entrySet().stream()
                .map(e -> Maps.immutableEntry(e.getKey(), e.getValue() / minNumber))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int count = 0;
        while (stateChunksNumbers.values().stream().anyMatch(x -> x > 0)) {
            for (Map.Entry<Integer, Integer> ratio : chunkDivideRatio.entrySet()) {
                int remainingNum = stateChunksNumbers.get(ratio.getKey());
                if (remainingNum <= 0) {
                    continue;
                }
                BitSet chunkIds = dividedChunks.get(ratio.getKey());
                if (remainingNum < ratio.getValue()) {
                    chunkIds.set(count, count + remainingNum);
                    count += remainingNum;
                    stateChunksNumbers.put(ratio.getKey(), 0);
                } else {
                    chunkIds.set(count, count + ratio.getValue());
                    count += ratio.getValue();
                    stateChunksNumbers.put(ratio.getKey(), remainingNum - ratio.getValue());
                }
                dividedChunks.put(ratio.getKey(), chunkIds);
            }
        }
        lastDividedChunks = dividedChunks;
        return dividedChunks;
    }

    private Map<Integer, Integer> calcStateChunksNumbers(Map<Integer, Long> trafficOfConnections) {
        Map<Integer, Integer> baseStateChunksNumbers;
        boolean trafficExists = trafficOfConnections.values().stream().allMatch(x -> x > 0);
        if (trafficExists) {
            baseStateChunksNumbers = getBaseStateChunksNumbersFromTraffic(trafficOfConnections);
        } else {
            baseStateChunksNumbers = getBaseStateChunksNumbersFromConstants();
        }
        return calcStateChunksNumbersFromBase(baseStateChunksNumbers);
    }

    private Map<Integer, Integer> calcStateChunksNumbers() {
        Map<Integer, Integer> baseStateChunksNumbers = getBaseStateChunksNumbersFromConstants();
        return calcStateChunksNumbersFromBase(baseStateChunksNumbers);
    }

    private Map<Integer, Integer> calcStateChunksNumbersFromBase(Map<Integer, Integer> stateChunksNumbers) {
        int chunkNum = chunkCollector.getRestChunkNum();
        int remainder = chunkNum - stateChunksNumbers.values().stream().reduce(0, Integer::sum);
        for (int key : stateChunksNumbers.keySet()) {
            if (remainder <= 0) {
                break;
            }
            stateChunksNumbers.put(key, stateChunksNumbers.get(key) + 1);
            remainder -= 1;
        }
        return stateChunksNumbers;
    }

    private Map<Integer, Integer> getBaseStateChunksNumbersFromTraffic(Map<Integer, Long> trafficOfConnections) {
        int traffic = Math.toIntExact(trafficOfConnections.values().stream().reduce(0L, Long::sum));
        int chunkNum = chunkCollector.getRestChunkNum();
        double unitTraffic = traffic / (double) chunkNum;
        return trafficOfConnections.entrySet().stream()
                .map(e -> Maps.immutableEntry(e.getKey(), (int) (e.getValue() / unitTraffic)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<Integer, Integer> getBaseStateChunksNumbersFromConstants() {
        int chunkNum = chunkCollector.getRestChunkNum();
        double totalChunkNumRate = chunkNum / (double) initialStateChunksNums.values().stream().mapToInt(Integer::intValue).sum();
        List<Integer> connectionIds = Ints.asList(SVController.getCurrentViewOtherAcceptors());
        return initialStateChunksNums.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), (int) (entry.getValue() * totalChunkNumRate)))
                .filter(entry -> connectionIds.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
