/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerEntryPusher {

    private static Logger logger = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private DLedgerConfig dLedgerConfig; //多副本相关配置。
    private DLedgerStore dLedgerStore; //存储实现类。

    private final MemberState memberState; //节点状态机

    private DLedgerRpcService dLedgerRpcService; //RPC 服务实现类，用于集群内的其他节点进行网络通讯。

    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>(); //每个节点基于投票轮次的当前水位线标记。键值为投票轮次，值为 ConcurrentMap<String/** 节点id*/, Long/** 节点对应的日志序号*/>
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>(); //用于存放追加请求的响应结果(Future模式)。

    private EntryHandler entryHandler; //从节点上开启的线程，用于接收主节点的 push 请求（append、commit、append）

    private QuorumAckChecker quorumAckChecker; //主节点上的追加请求投票器。

    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>(); //主节点日志请求转发器，向从节点复制消息等。

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger)); //
            }
        }
        this.entryHandler = new EntryHandler(logger);
        this.quorumAckChecker = new QuorumAckChecker(logger);
    }

    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    private void updatePeerWaterMark(long term, String peerId, long index) { //当前的投票轮次。  当前节点的ID。  当前追加数据的日志序号。
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");    //初始化 peerWaterMarksByTerm 数据结构，其结果为 < Long /** term */, Map< String /** peerId */, Long /** entry index*/>。
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) { //如果 peerWaterMarksByTerm 存储的日志index 小于当前数据的日志index，则更新
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    public boolean isPendingFull(long currTerm) {
        //检查当前投票轮次是否在 PendingMap 中，如果不在，则初始化，其结构为：Map< Long/* 投票轮次*/, ConcurrentMap<Long, TimeoutFuture< AppendEntryResponse>>>
        checkTermForPendingMap(currTerm, "isPendingFull");
        //检测当前等待从节点返回结果的个数是否超过其最大请求数量，可通过maxPendingRequestsNum 配置，该值默认为：10000
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry, boolean isBatchWait) {
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex()); //更新主节点的 push 水位线。
        if (memberState.getPeerMap().size() == 1) { //如果集群的节点个数为1，无需转发，直接返回成功结果
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            if (isBatchWait) {
                return BatchAppendFuture.newCompletedFuture(entry.getPos(), response);
            }
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        } else {
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            //构建 append 响应 Future 并设置超时时间，默认值为：2500 ms，可以通过 maxWaitAckTimeMs 配置改变其默认值
            AppendFuture<AppendEntryResponse> future;
            if (isBatchWait) {
                future = new BatchAppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            } else {
                future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            }
            future.setPos(entry.getPos());
            //将构建的 Future 放入等待结果集合中。
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }
            return future;
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis(); //上次打印水位线的时间戳，单位为毫秒。
        private long lastCheckLeakTimeMs = System.currentTimeMillis();  //上次检测泄漏的时间戳
        private long lastQuorumIndex = -1;  //已投票仲裁的日志序号。

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        @Override
        public void doWork() {
            try {
                //如果离上一次打印 watermak 的时间超过3s，则打印一下当前的 term、ledgerBegin、ledgerEnd、committed、peerWaterMarksByTerm 这些数据日志。
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                //如果当前节点不是主节点，直接返回，不作为
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                //清理pendingAppendResponsesByTerm、peerWaterMarksByTerm 中不是本次投票轮次的数据，避免一些不必要的内存使用
                if (pendingAppendResponsesByTerm.size() > 1) {
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().complete(response);
                        }
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }
                //根据各个从节点反馈的进度，进行仲裁，确定已提交序号。
                // 为了加深对这段代码的理解，再来啰嗦一下 peerWaterMarks 的作用，存储的是各个从节点当前已成功追加的日志序号。
                // 例如一个三节点的 DLedger 集群，peerWaterMarks 数据存储大概如下
                //{
                //“dledger_group_01_0” : 100,
                //"dledger_group_01_1" : 101,
                //}
                //其中 dledger_group_01_0 为从节点1的ID，当前已复制的序号为 100，而 dledger_group_01_1 为节点2的ID，当前已复制的序号为 101。再加上主节点，如何确定可提交序号呢
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                //首先遍历 peerWaterMarks 的 value 集合，即上述示例中的 {100, 101}，用临时变量 index 来表示待投票的日志序号，
                //需要集群内超过半数的节点的已复制序号超过该值，则该日志能被确认提交。
                //{100,101,101} 提交101、{99 100 101} 提交100 、{95 96 97 98 99 100 101} 提交98 因为集群有4个节点的序号都超过98了（超过一半） 但只有3个超过99（没到一半） 所以提交98
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                        .stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList());
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2); //排序后取中间数 中间数为此次提交日志序号
                dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                //处理 quorumIndex 之前的挂起请求，需要发送响应到客户端,
                ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                boolean needCheck = false;
                int ackNum = 0;
                for (Long i = quorumIndex; i > lastQuorumIndex; i--) {  //从 quorumIndex 开始处理，没处理一条，该序号减一，直到大于0或主动退出，请看后面的退出逻辑
                    try {
                        CompletableFuture<AppendEntryResponse> future = responses.remove(i); //responses 中移除该日志条目的挂起请求
                        if (future == null) { //如果未找到挂起请求，说明前面挂起的请求已经全部处理完毕)
                            needCheck = true;
                            break;
                        } else if (!future.isDone()) {   //向客户端返回结果。
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) future).getPos());
                            future.complete(response);
                        }
                        ackNum++;  //ackNum，表示本次确认的数量。
                    } catch (Throwable t) {
                        logger.error("Error in ack to index={} term={}", i, currTerm, t);
                    }
                }
                //如果本次确认的个数为0，则尝试去判断超过该仲裁序号的请求，是否已经超时，如果已超时，则返回超时响应结果。
                if (ackNum == 0) {
                    for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                        TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                        if (future == null) {
                            break;
                        } else if (future.isTimeOut()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            future.complete(response);
                        } else {
                            break;
                        }
                    }
                    waitForRunning(1);
                }
                //检查是否发送泄漏。其判断泄漏的依据是如果挂起的请求的日志序号小于已提交的序号，则移除
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                    updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                    for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
                        if (futureEntry.getKey() < quorumIndex) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(futureEntry.getKey());
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                            futureEntry.getValue().complete(response);
                            responses.remove(futureEntry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                //一次日志仲裁就结束了，最后更新 lastQuorumIndex 为本次仲裁的的新的提交值。
                lastQuorumIndex = quorumIndex;
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     *   APPEND : append the entries to the follower
     *   COMPARE : if the leader changes, the new leader should compare its entries to follower's
     *   TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     *   COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     *           the leader will send a pure request to inform the follower of committed index.
     *
     *   The common transferring between these types are as following:
     *
     *   COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     *   ^                             |
     *   |---<-----<------<-------<----|
     *
     */
    private class EntryDispatcher extends ShutdownAbleThread {
        //向从节点发送命令的类型，可选值：PushEntryRequest.Type.COMPARE、TRUNCATE、APPEND、COMMIT，下面详细说明。
        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        private long lastPushCommitTimeMs = -1; //上一次发送提交类型的时间戳。
        private String peerId; //目标节点ID。
        private long compareIndex = -1; //已完成比较的日志序号。
        private long writeIndex = -1;  //已写入的日志序号 ||  已发送的同步日志序号
        private int maxPendingSize = 1000;  //允许的最大挂起日志数量。
        private long term = -1;  //Leader 节点当前的投票轮次。
        private String leaderId = null;  //Leader 节点ID。
        private long lastCheckLeakTimeMs = System.currentTimeMillis();  //上次检测泄漏的时间，所谓的泄漏，就是看挂起的日志请求数量是否查过了 maxPendingSize
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();  //记录日志的挂起时间，key：日志的序列(entryIndex)，value：挂起时间戳。
        private ConcurrentMap<Long, Pair<Long, Integer>> batchPendingMap = new ConcurrentHashMap<>();
        private PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();
        private Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());  //配额

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        private boolean checkAndFreshState() {
            if (!memberState.isLeader()) {  //如果节点的状态不是主节点，则直接返回 false。则结束 本次 doWork 方法。因为只有主节点才需要向从节点转发日志。
                return false;
            }
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                //如果当前节点状态是主节点，但当前的投票轮次与状态机轮次或 leaderId 还未设置，或 leaderId 与状态机的 leaderId 不相等，这种情况通常是集群触发了重新选举，设置其term、leaderId与状态机同步，即将发送COMPARE 请求。
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup()); //集群群组
            request.setRemoteId(peerId);  //从节点
            request.setLeaderId(leaderId); //主节点
            request.setTerm(term);  //投票轮次
            request.setEntry(entry);  //日志内容
            request.setType(target); //请求类型与
            request.setCommitIndex(dLedgerStore.getCommittedIndex());  //主节点已提交日志序号
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            //首先触发条件：append 挂起请求数已超过最大允许挂起数；基于文件存储并主从差异超过300m，可通过 peerPushThrottlePoint 配置。
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {  //每秒追加的日志超过 20m(可通过 peerPushQuota 配置)，则会 sleep 1s中后再追加。
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }
        private void doAppendInner(long index) throws Exception { //追加请求
            DLedgerEntry entry = getDLedgerEntryForAppend(index); //首先根据序号查询出日志
            if (null == entry) {
                return;
            }
            checkQuotaAndWait(entry); //检测配额，如果超过配额，会进行一定的限流，其关键实现点
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND); //构建 PUSH APPEND请求日志。
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request); //通过 Netty 发送网络请求到从节点，从节点收到请求会进行处理
            pendingMap.put(index, System.currentTimeMillis()); //用 pendingMap 记录待追加的日志的发送时间，用于发送端判断是否超时的一个依据。
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            pendingMap.remove(x.getIndex()); //移除 pendingMap 中的关于该日志的发送超时时间。
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());  //更新已成功追加的日志序号(按投票轮次组织，并且每个从服务器一个键值对)。
                            quorumAckChecker.wakeup();  //唤醒 quorumAckChecker 线程(主要用于仲裁 append 结果)，后续会详细介绍。
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {  //如果上一次单独发送 commit 的请求时间与当前时间相隔低于 1s，放弃本次提交请求。
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);  //构建提交请求
                //Ignore the results
                dLedgerRpcService.push(request);  //通过网络向从节点发送 commit 请求。
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        private void doCheckAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);  //获取已成功 append 的序号。
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                //从挂起的请求队列中获取下一条的发送时间，如果不为空并去超过了 append 的超时时间，则再重新发送 append 请求，最大超时时间默认为 1s，可以通过 maxPushTimeOutMs 来改变默认值。
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1); //追加请求
            }
        }

        private void doAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {  //检查状态
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {  //如果请求类型不为 APPEND，则退出，结束本轮 doWork 方法执行。
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    // writeIndex 表示当前追加到从该节点的序号，通常情况下主节点向从节点发送 append 请求时，会附带主节点的已提交指针
                    // 但如何 append 请求发不那么频繁 writeIndex 大于 leaderEndIndex 时
                    // （由于pending请求超过其 pending 请求的队列长度（默认为1w)，时，会阻止数据的追加，此时有可能出现 writeIndex 大于 leaderEndIndex 的情况，此时单独发送 COMMIT 请求。
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                if (pendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    //检测 pendingMap(挂起的请求数量)是否发送泄漏，即挂起队列中容量是否超过允许的最大挂起阀值。
                    // 获取当前节点关于本轮次的当前水位线(已成功 append 请求的日志序号)，如果发现正在挂起请求的日志序号小于水位线，则丢弃。
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    //如果挂起的请求（等待从节点追加结果）大于 maxPendingSize 时，检查并追加一次 append 请求。
                    doCheckAppendResponse();
                    break;
                }
                //具体的追加请求。
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        private void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(dLedgerStore.getCommittedIndex());
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            batchPendingMap.remove(x.getIndex());
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }

        private void doBatchAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            batchAppendEntryRequest.addEntry(entry);
            if (batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchPushSize()) {
                sendBatchAppendEntryRequest();
            }
        }

        private void doCheckBatchAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Pair pair = batchPendingMap.get(peerWaterMark + 1);
            if (pair != null && System.currentTimeMillis() - (long) pair.getKey() > dLedgerConfig.getMaxPushTimeOutMs()) {
                long firstIndex = peerWaterMark + 1;
                long lastIndex = firstIndex + (int) pair.getValue() - 1;
                logger.warn("[Push-{}]Retry to push entry from {} to {}", peerId, firstIndex, lastIndex);
                batchAppendEntryRequest.clear();
                for (long i = firstIndex; i <= lastIndex; i++) {
                    DLedgerEntry entry = dLedgerStore.get(i);
                    batchAppendEntryRequest.addEntry(entry);
                }
                sendBatchAppendEntryRequest();
            }
        }

        private void doBatchAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (batchAppendEntryRequest.getCount() > 0) {
                        sendBatchAppendEntryRequest();
                    }
                    doCommit();
                    doCheckBatchAppendResponse();
                    break;
                }
                if (batchPendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : batchPendingMap.entrySet()) {
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            batchPendingMap.remove(entry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (batchPendingMap.size() >= maxPendingSize) {
                    doCheckBatchAppendResponse();
                    break;
                }
                doBatchAppendInner(writeIndex);
                writeIndex++;
            }
        }

        private void doTruncate(long truncateIndex) throws Exception {  //该方法主要就是构建 truncate 请求到从节点。
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:   //如果将目标类型设置为 append，则重置 compareIndex ，并设置 writeIndex 为当前 index 加1。
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    quorumAckChecker.wakeup();
                    writeIndex = index + 1;
                    if (dLedgerConfig.isEnableBatchPush()) {
                        resetBatchAppendEntryRequest();
                    }
                    break;
                case COMPARE:   //如果将目标类型设置为 COMPARE，则重置 compareIndex 为负一，接下将向各个从节点发送 COMPARE 请求类似，并清除已挂起的请求
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        if (dLedgerConfig.isEnableBatchPush()) {
                            batchPendingMap.clear();
                        } else {
                            pendingMap.clear();
                        }
                    }
                    break;
                case TRUNCATE:   //如果将目标类型设置为 TRUNCATE，则重置 compareIndex 为负一。
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        private void doCompare() throws Exception {
            while (true) { //测试
                if (!checkAndFreshState()) {  //检查状态
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {  //如果是请求类型不是 COMPARE 或 TRUNCATE 请求，则直接跳出。
                    break;
                }
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) { //如果已比较索引 和 ledgerEndIndex 都为 -1 ，表示一个新的 DLedger 集群，则直接跳出。
                    break;
                }
                //revise the compareIndex
                //如果 compareIndex 为 -1 或compareIndex 不在有效范围内，则重置待比较序列号为当前已已存储的最大日志序号：ledgerEndIndex
                if (compareIndex == -1) {
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }
                //根据序号查询到日志，并向从节点发起 COMPARE 请求，其超时时间为 3s。
                DLedgerEntry entry = dLedgerStore.get(compareIndex);
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                long truncateIndex = -1;

                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    //如果两者的日志序号相同，则无需截断，下次将直接先从节点发送 append 请求；否则将 truncateIndex 设置为响应结果中的 endIndex
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex()
                    || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {
                    //如果从节点存储的最大日志序号小于主节点的最小序号，或者从节点的最小日志序号大于主节点的最大日志序号，即两者不相交，
                    // 这通常发生在从节点崩溃很长一段时间，而主节点删除了过期的条目时。truncateIndex 设置为主节点的 ledgerBeginIndex，即主节点目前最小的偏移量。
                    /*
                     The follower's entries does not intersect with the leader.
                     This usually happened when the follower has crashed for a long time while the leader has deleted the expired entries.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    //如果已比较的日志序号小于从节点的开始日志序号，很可能是从节点磁盘发送损耗，从主节点最小日志序号开始同步。
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex > response.getEndIndex()) {
                    //如果已比较的日志序号大于从节点的最大日志序号，则已比较索引设置为从节点最大的日志序号，触发数据的继续同步。
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    compareIndex = response.getEndIndex();
                } else {
                    //如果已比较的日志序号大于从节点的开始日志序号，但小于从节点的最大日志序号，则待比较索引减一。
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    compareIndex--;
                }
                /*
                 The compared index is smaller than the leader's begin index, truncate the follower.
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    //如果比较出来的日志序号小于主节点的最小日志需要，则设置为主节点的最小序号。
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /*
                 If get value for truncateIndex, do it right now.
                 */
                if (truncateIndex != -1) {  //如果比较出来的日志序号不等于 -1 ，则向从节点发送 TRUNCATE 请求。
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        @Override
        public void doWork() {
            try {
                if (!checkAndFreshState()) { //检查状态，是否可以继续发送 append 或 compare
                    waitForRunning(1);
                    return;
                }

                if (type.get() == PushEntryRequest.Type.APPEND) {  //如果推送类型为APPEND，主节点向从节点传播消息请求。
                    if (dLedgerConfig.isEnableBatchPush()) {
                        doBatchAppend();
                    } else {
                        doAppend();
                    }
                } else {
                    doCompare();                  //主节点向从节点发送对比数据差异请求（当一个新节点被选举成为主节点时，往往这是第一步）。
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                DLedgerUtils.sleep(500);
            }
        }
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     *
     */
    private class EntryHandler extends ShutdownAbleThread {
        //上一次检查主服务器是否有 push 消息的时间戳。
        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();
        // 请求处理队列。
        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        //COMMIT、COMPARE、TRUNCATE 相关请求
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            //The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);  //首先构建一个响应结果Future，默认超时时间 1s。
            switch (request.getType()) {
                case APPEND: //如果是 APPEND 请求，放入到 writeRequestMap 集合中，如果已存在该数据结构，说明主节点重复推送，构建返回结果，其状态码为 REPEATED_PUSH。放入到 writeRequestMap 中，由 doWork 方法定时去处理待写入的请求。
                    if (dLedgerConfig.isEnableBatchPush()) {
                        PreConditions.check(request.getBatchEntry() != null && request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                        long firstIndex = request.getFirstEntryIndex();
                        writeRequestMap.put(firstIndex, new Pair<>(request, future));
                    } else {
                        PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                        long index = request.getEntry().getIndex();
                        Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                        if (old != null) { //重复请求
                            logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                            future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                        }
                    }
                    break;
                case COMMIT:  //如果是提交请求， 将请求存入 compareOrTruncateRequests 请求处理中，由 doWork 方法异步处理。
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:  //如果是 COMPARE 或 TRUNCATE 请求，将待写入队列 writeRequestMap 清空，并将请求放入 compareOrTruncateRequests 请求队列中，由 doWork 方法异步处理。
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getEntry().getIndex());
            }
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        private PushEntryResponse buildBatchAppendResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            response.setIndex(request.getLastEntryIndex());
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            //调用DLedgerStore 的 appendAsFollower 方法进行日志的追加，与appendAsLeader 在日志存储部分相同，只是从节点无需再转发日志
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));  //主要也是返回当前从几点的 ledgerBeginIndex、ledgerEndIndex 以及投票轮次，供主节点进行判断比较。
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                dLedgerStore.updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            //handleDoTruncate 方法实现比较简单，删除从节点上 truncateIndex 日志序号之后的所有日志，具体调用dLedgerStore 的 truncate 方法
            // 根据日志序号，去定位到日志文件，如果命中具体的文件，则修改相应的读写指针、刷盘指针等，并将所在在物理文件之后的所有文件删除
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoBatchAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getBatchEntry()) {
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildBatchAppendResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoBatchAppend]", t);
            }

        }

        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long index = pair.getKey().getEntry().getIndex(); //首先获取待写入日志的序号
                //Fall behind
                if (index <= endIndex) {  //如果待写入的日志序号小于从节点已追加的日志(endIndex)，并且日志的确已存储在从节点，则返回成功，并输出警告日志【PushFallBehind】，继续监测下一条待写入日志
                    try {
                        DLedgerEntry local = dLedgerStore.get(index);
                        PreConditions.check(pair.getKey().getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(index);
                    continue;
                }
                //Just OK
                if (index == endIndex + 1) {  //如果待写入 index 等于 endIndex + 1，则结束循环，因为下一条日志消息已经在待写入队列中，即将写入
                    //The next entry is coming, just return
                    return;
                }
                //Fast forward 如果待写入 index 大于 endIndex + 1，并且未超时，则直接检查下一条待写入日志
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (index < minFastForwardIndex) {  //果待写入 index 大于 endIndex + 1，并且已经超时，则记录该索引，使用 minFastForwardIndex 存储
                    minFastForwardIndex = index;
                }
            }
            //如果未找到需要快速失败的日志序号或 writeRequestMap 中未找到其请求，则直接结束检测。
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            //则向主节点报告从节点已经与主节点发生了数据不一致，从节点并没有写入序号 minFastForwardIndex 的日志。如果主节点收到此种响应，将会停止日志转发，转而向各个从节点发送 COMPARE 请求，从而使数据恢复一致
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        private void checkBatchAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                //Fall behind
                if (lastEntryIndex <= endIndex) {
                    try {
                        for (DLedgerEntry dLedgerEntry : pair.getKey().getBatchEntry()) {
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (firstEntryIndex < minFastForwardIndex) {
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }
        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * @param endIndex
         */
        //该方法是重点，doWork 的从服务器存储的最大有效日志序号(ledgerEndIndex) + 1 序号，尝试从待写请求中获取不到对应的请求时调用，
        // 这种情况也很常见，例如主节点并么有将最新的数据 PUSH 给从节点
        private void checkAbnormalFuture(long endIndex) {
            //如果上一次检查的时间距现在不到1s，则跳出；如果当前没有积压的append请求，同样跳出，因为可以同样明确的判断出主节点还未推送日志
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }
            if (dLedgerConfig.isEnableBatchPush()) {
                checkBatchAppendFuture(endIndex);
            } else {
                checkAppendFuture(endIndex); //遍历当前待写入的日志追加请求(主服务器推送过来的日志复制请求)，找到需要快速快进的的索引
            }
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {  //如果当前节点的状态不是从节点，则跳出。
                    waitForRunning(1);
                    return;
                }
                if (compareOrTruncateRequests.peek() != null) {
                    //如果 compareOrTruncateRequests 队列不为空，说明有COMMIT、COMPARE、TRUNCATE 等请求，这类请求优先处理。
                    // 值得注意的是这里使用是 peek、poll 等非阻塞方法，然后根据请求的类型，调用对应的方法。稍后详细介绍
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    //如果只有 append 类请求，则根据当前节点最大的消息序号，尝试从 writeRequestMap 容器中，获取下一个消息复制请求(ledgerEndIndex + 1) 为 key 去查找。
                    // 如果不为空，则执行 doAppend 请求，如果为空，则调用 checkAbnormalFuture 来处理异常情况。
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    if (dLedgerConfig.isEnableBatchPush()) {
                        handleDoBatchAppend(nextIndex, request, pair.getValue());
                    } else {
                        handleDoAppend(nextIndex, request, pair.getValue());
                    }
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
