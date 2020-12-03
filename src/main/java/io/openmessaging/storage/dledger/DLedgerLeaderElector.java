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
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    private Random random = new Random();
    private DLedgerConfig dLedgerConfig;
    private final MemberState memberState;
    private DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    private volatile long lastLeaderHeartBeatTime = -1;
    private volatile long lastSendHeartBeatTime = -1;
    private volatile long lastSuccHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 2000;
    private int maxHeartBeatLeak = 3;
    //as a client
    private long nextTimeToRequestVote = -1;
    private volatile boolean needIncreaseTermImmediately = false;
    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;

    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    private long lastVoteCost = 0L;

    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    private final TakeLeadershipTask takeLeadershipTask = new TakeLeadershipTask();

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        stateMaintainer.start();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.currTerm()) {
            //如果主节点的 term 小于 从节点的term，发送反馈给主节点，告知主节点的 term 已过时；
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {  //如果投票轮次相同，并且发送心跳包的节点是该节点的主节点，则返回成功。
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) { //如果主节的投票轮次小于当前投票轮次，则返回主节点投票轮次过期。
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) { //如果投票轮次相同
                if (memberState.getLeaderId() == null) { //如果当前节点的主节点字段为空，则使用主节点的ID，并返回成功。
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) { //如果当前节点的主节点就是发送心跳包的节点，则更新上一次收到心跳包的时间戳，并返回成功
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //如果从节点的主节点与发送心跳包的节点ID不同，说明有另外一个Leaer，按道理来说是不会发送的，如果发生，则返回已存在- 主节点，标记该心跳包处理结束
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                //如果主节点的投票轮次大于从节点的投票轮次，则认为从节点并未准备好，则从节点进入Candidate 状态，并立即发起一次投票。
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }

            //判断请求节点的 ledgerEndTerm 与当前节点的 ledgerEndTerm，这里主要是判断日志的复制进度。
            //如果请求节点的 ledgerEndTerm 小于当前节点的 ledgerEndTerm 则拒绝，其原因是请求节点的日志复制进度比当前节点低，这种情况是不能成为主节点的。
            //如果 ledgerEndTerm 相等，但是 ledgerEndIndex 比当前节点小，则拒绝，原因与上一条相同。
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.currTerm()) {
                //如果发起投票节点的 term 小于当前节点的 term
                //此种情况下投拒绝票，也就是说在 raft 协议的世界中，谁的 term 越大，越有话语权。
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) { //如果两者的 term 相等，说明两者都处在同一个投票轮次中，地位平等，接下来看该节点是否已经投过票
                if (memberState.currVoteFor() == null) { //如果未投票
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) { // 或已投票给请求节点
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) { //
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {  //如果该节点还未有Leader节点，但已经投了其他节点的票，则拒绝请求节点，并告知已投票。
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                //如果发起投票节点的 term 大于当前节点的 term
                //拒绝请求节点的投票请求，并告知自身还未准备投票，自身会使用请求节点的投票轮次立即进入到Candidate状态。
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            //如果请求的 term 小于 ledgerEndTerm 以同样的理由拒绝。
            if (request.getTerm() < memberState.getLedgerEndTerm()) {  //term 一般会比 ledgerEndTerm 要大 或者相等                                                                                                                                                                                                                   每次选举时基本会自增term  每次append消息会把term赋值给ledgerEndTerm
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            if (!self && isTakingLeadership() && request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TAKING_LEADERSHIP));
            }
            //经过层层条件帅选，将宝贵的赞成票投给请求节点
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    private void sendHeartbeats(long term, String leaderId) throws Exception {
        final AtomicInteger allNum = new AtomicInteger(1);
        final AtomicInteger succNum = new AtomicInteger(1);
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        final AtomicLong maxTerm = new AtomicLong(-1);
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        final CountDownLatch beatLatch = new CountDownLatch(1);
        long startHeartbeatTimeMs = System.currentTimeMillis();
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:    //心跳包成功响应。
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:    //主节点的投票 term 小于从节点的投票轮次。
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:   //从节点已经有了新的主节点。
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:  //TERM_NOT_READY
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }

                    if (x.getCode() == DLedgerResponseCode.NETWORK_ERROR.getCode())
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                    else
                        memberState.getPeersLiveTable().put(id, Boolean.TRUE);

                    if (memberState.isQuorum(succNum.get())
                        || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
        }
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {  //如果成功的票数大于进群内的半数，则表示集群状态正常，正常按照心跳包间隔发送心跳包(
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {   //如果成功的票数加上未准备的投票的节点数量超过集群内的半数，则立即发送心跳包
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {   //如果从节点的投票轮次比主节点的大，则使用从节点的投票轮次，或从节点已经有了另外的主节点，节点状态从 Leader 转换为 Candidate
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {  //从节点已经有了新的主节点。
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                changeRoleToCandidate(term);
            }
        }
    }

    private void maintainAsLeader() throws Exception {
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) { //如果maxHeartBeatLeak (默认为3)个心跳包周期内未收到心跳，则将状态变更为Candidate
            synchronized (memberState) {
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
        long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                voteResponse = handleVote(voteRequest, true); //如果是自己 调用这里
            } else {
                //async
                voteResponse = dLedgerRpcService.vote(voteRequest); //调用其他节点的handleVote  所以重点关注handleVote
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    private boolean isTakingLeadership() {
        return memberState.getSelfId().equals(dLedgerConfig.getPreferredLeaderId())
            || memberState.getTermToTakeLeadership() == memberState.currTerm();
    }

    private long getNextTimeToRequestVote() {
        if (isTakingLeadership()) {
            //下一次倒计时：当前时间戳 + 上次投票的开销 + 最小投票间隔(300ms) + （1000- 300 ）之间的随机值。
            return System.currentTimeMillis() + dLedgerConfig.getMinTakeLeadershipVoteIntervalMs() +
                random.nextInt(dLedgerConfig.getMaxTakeLeadershipVoteIntervalMs() - dLedgerConfig.getMinTakeLeadershipVoteIntervalMs());
        }
        return System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    private void maintainAsCandidate() throws Exception {
        //for candidate
        //下一次发起的投票的时间，如果当前时间小于该值，说明计时器未过期，此时无需发起投票 && 是否应该立即发起投票
        //该值默认为false，当收到从主节点的心跳包并且当前状态机的轮次大于主节点的轮次，说明集群中Leader的投票轮次小于从节点的轮次，应该立即发起新的投票
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        long term;
        long ledgerEndTerm;
        long ledgerEndIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) { //不是候选者不能参与投票
                return;
            }
            /*
            * 如果上一次的投票结果为待下一次投票或应该立即开启投票，并且根据当前状态机获取下一轮的投票轮次。
            * 如果上一次的投票结果不是WAIT_TO_VOTE_NEXT(等待下一轮投票)，则投票轮次依然为状态机内部维护的轮次。
            * */
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) { // 如果投票结果等于WAIT_TO_VOTE_NEXT 或 应该立即开启投票
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex(); //发起投票节点维护的已知的最大日志条目索引。
            ledgerEndTerm = memberState.getLedgerEndTerm(); // 发起投票节点维护的已知的最大投票轮次。
        }
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote(); //重新设置下一次投票超时时间 并重置needIncreaseTermImmediately
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();
        //向集群内的其他节点发起投票请，并返回投票结果列表
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        final AtomicLong knownMaxTermInGroup = new AtomicLong(term); //已知的最大投票轮次。
        final AtomicInteger allNum = new AtomicInteger(0); //所有投票票数。
        final AtomicInteger validNum = new AtomicInteger(0); //有效投票数
        final AtomicInteger acceptedNum = new AtomicInteger(0); //获得的投票数
        final AtomicInteger notReadyTermNum = new AtomicInteger(0); //未准备投票的节点数量，如果对端节点的投票轮次小于发起投票的轮次，则认为对端未准备好，对端节点使用本次的轮次进入 - Candidate 状态。
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0); //发起投票的节点的ledgerEndTerm小于对端节点的个数。
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false); //是否已经存在Leader

        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) { //多个节点返回的投票结果
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:     //赞成票，acceptedNum加一，只有得到的赞成票超过集群节点数量的一半才能成为Leader。
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED: //拒绝票，原因是已经投了其他节点的票。
                            case REJECT_TAKING_LEADERSHIP:
                                break;
                            case REJECT_ALREADY_HAS_LEADER: //拒绝票，原因是因为集群中已经存在Leaer了。alreadyHasLeader设置为true，无需在判断其他投票结果了，结束本轮投票。
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER: //拒绝票，如果自己维护的term小于远端维护的ledgerEndTerm，则返回该结果，如果对端的team大于自己的team，需要记录对端最大的投票轮次，以便更新自己的投票轮次
                            case REJECT_EXPIRED_VOTE_TERM: //拒绝票，如果自己维护的term小于远端维护的term，更新自己维护的投票轮次。
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM: //拒绝票，如果自己维护的 ledgerTerm小于对端维护的ledgerTerm，则返回该结果。如果是此种情况，增加计数器- biggerLedgerNum的值
                            case REJECT_SMALL_LEDGER_END_INDEX: //拒绝票，如果对端的ledgerTeam与自己维护的ledgerTeam相等，但是自己维护的dedgerEndIndex小于对端维护的值，返回该值，增加biggerLedgerNum计数器的值
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY: //拒绝票，对端的投票轮次小于自己的team，则认为对端还未准备好投票，对端使用自己的投票轮次，是自己进入到Candidate状态
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) { //如果leader已经产生 不用等待投票结果
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("vote response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }

        try {
            voteLatch.await(2000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }

        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        if (knownMaxTermInGroup.get() > term) { //如果远端的投票轮次大于发起投票的节点，则该节点使用远端的轮次，重新进入到Candidate状态，并且重置投票计时器，其值为“1个常规计时器”
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT; //等待下一次投票
            nextTimeToRequestVote = getNextTimeToRequestVote(); //重置计时器
            changeRoleToCandidate(knownMaxTermInGroup.get()); //角色更新为候选者
        } else if (alreadyHasLeader.get()) {
            //如果已经存在Leader，该节点重新进入到Candidate,并重置定时器，该定时器的时间： “1个常规计时器” + heartBeatTimeIntervalMs * maxHeartBeatLeak ，其中 heartBeatTimeIntervalMs 为一次心跳间隔时间，
            //maxHeartBeatLeak 为 允许最大丢失的心跳包，即如果Flower节点在多少个心跳周期内未收到心跳包，则认为Leader已下线
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE; //等待重新投票
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) {
            //如果收到的有效票数未超过半数，则重置计时器为“ 1个常规计时器”，然后等待重新投票，注意状态为WAIT_TO_REVOTE，该状态下的特征是下次投票时不增加投票轮次。
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (!memberState.isQuorum(validNum.get() - biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + maxVoteIntervalMs;
        } else if (memberState.isQuorum(acceptedNum.get())) { //如果得到的赞同票超过半数，则成为Leader。
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) { //如果得到的赞成票加上未准备投票的节点数超过半数，则应该立即发起投票，故其结果为REVOTE_IMMEDIATELY
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else { //其他情况，开启下一轮投票。
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
            memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }
    }

    /**
     * The core method of maintainer. Run the specified logic according to the current role: candidate => propose a
     * vote. leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     * follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     *
     * @throws Exception
     */
    /**
     * 从节点会在主节点成为leader后会调用maintainAsLeader方法 让自己角色变成Follower 从节点在未被通知成为Follower期间 会一直调用maintainAsCandidate
     * 所以主节点流程   maintainAsCandidate -> 成为leader -> maintainAsLeader -> 通知其他节点成为Follower
     * 从节点流程 maintainAsCandidate -> 自己不是leader -> maintainAsCandidate ->  maintainAsCandidate ->  被通知成为Follower -> maintainAsFollower
     * @throws Exception
     */
    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader(); //主要通知其他节点变成Follower
        } else if (memberState.isFollower()) {
            maintainAsFollower(); //主要做以下事情 如果maxHeartBeatLeak (默认为3)个心跳包周期内未收到心跳，则将状态变更为Candidate（主节点可能挂了,状态变成更然后触发重新选举）
        } else {
            maintainAsCandidate(); //主要是发起选举和接收选票
        }
    }

    private void handleRoleChange(long term, MemberState.Role role) {
        try {
            takeLeadershipTask.check(term, role);
        } catch (Throwable t) {
            logger.error("takeLeadershipTask.check failed. ter={}, role={}", term, role, t);
        }

        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        logger.info("handleLeadershipTransfer: {}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [HandleLeaderTransfer] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            if (!memberState.isLeader()) {
                logger.warn("[BUG] [HandleLeaderTransfer] selfId={} is not leader", request.getLeaderId());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.NOT_LEADER.getCode()));
            }

            if (memberState.getTransferee() != null) {
                logger.warn("[BUG] [HandleLeaderTransfer] transferee={} is already set", memberState.getTransferee());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.LEADER_TRANSFERRING.getCode()));
            }

            memberState.setTransferee(request.getTransfereeId());
        }
        LeadershipTransferRequest takeLeadershipRequest = new LeadershipTransferRequest();
        takeLeadershipRequest.setGroup(memberState.getGroup());
        takeLeadershipRequest.setLeaderId(memberState.getLeaderId());
        takeLeadershipRequest.setLocalId(memberState.getSelfId());
        takeLeadershipRequest.setRemoteId(request.getTransfereeId());
        takeLeadershipRequest.setTerm(request.getTerm());
        takeLeadershipRequest.setTakeLeadershipLedgerIndex(memberState.getLedgerEndIndex());
        takeLeadershipRequest.setTransferId(memberState.getSelfId());
        takeLeadershipRequest.setTransfereeId(request.getTransfereeId());
        if (memberState.currTerm() != request.getTerm()) {
            logger.warn("[HandleLeaderTransfer] term changed, cur={} , request={}", memberState.currTerm(), request.getTerm());
            return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }

        return dLedgerRpcService.leadershipTransfer(takeLeadershipRequest).thenApply(response -> {
            synchronized (memberState) {
                if (response.getCode() != DLedgerResponseCode.SUCCESS.getCode() ||
                    (memberState.currTerm() == request.getTerm() && memberState.getTransferee() != null)) {
                    logger.warn("leadershipTransfer failed, set transferee to null");
                    memberState.setTransferee(null);
                }
            }
            return response;
        });
    }

    public CompletableFuture<LeadershipTransferResponse> handleTakeLeadership(
        LeadershipTransferRequest request) throws Exception {
        logger.debug("handleTakeLeadership.request={}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [handleTakeLeadership] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            long targetTerm = request.getTerm() + 1;
            memberState.setTermToTakeLeadership(targetTerm);
            CompletableFuture<LeadershipTransferResponse> response = new CompletableFuture<>();
            takeLeadershipTask.update(request, response);
            changeRoleToCandidate(targetTerm);
            needIncreaseTermImmediately = true;
            return response;
        }
    }

    private class TakeLeadershipTask {
        private LeadershipTransferRequest request;
        private CompletableFuture<LeadershipTransferResponse> responseFuture;

        public synchronized void update(LeadershipTransferRequest request,
            CompletableFuture<LeadershipTransferResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        public synchronized void check(long term, MemberState.Role role) {
            logger.trace("TakeLeadershipTask called, term={}, role={}", term, role);
            if (memberState.getTermToTakeLeadership() == -1 || responseFuture == null) {
                return;
            }
            LeadershipTransferResponse response = null;
            if (term > memberState.getTermToTakeLeadership()) {
                response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.EXPIRED_TERM.getCode());
            } else if (term == memberState.getTermToTakeLeadership()) {
                switch (role) {
                    case LEADER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.SUCCESS.getCode());
                        break;
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        return;
                }
            } else {
                switch (role) {
                    /*
                     * The node may receive heartbeat before term increase as a candidate,
                     * then it will be follower and term < TermToTakeLeadership
                     */
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
                }
            }

            responseFuture.complete(response);
            logger.info("TakeLeadershipTask finished. request={}, response={}, term={}, role={}", request, response, term, role);
            memberState.setTermToTakeLeadership(-1);
            responseFuture = null;
            request = null;
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) { //是否开启驱动设置
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);//重置定时器
                    DLedgerLeaderElector.this.maintainState();//驱动状态机
                }
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }
}
