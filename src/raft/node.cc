#include "node.hpp"
#include <functional>

#include <random>


namespace craft {


static auto g_logger = M_SYLAR_LOG_NAME("craft");

static inline m_sylar::ConfigVar<size_t>::ptr g_followerElectionTimeMin =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.followerElectionTimeMin", 400, kRpcConfId,
                                           "最短选举超时定时器时间ms");
static inline m_sylar::ConfigVar<size_t>::ptr g_followerElectionTimeMax =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.followerElectionTimeMax", 650, kRpcConfId,
                                           "最长选举超时定时器时间ms");
static inline m_sylar::ConfigVar<size_t>::ptr g_leaderHeartBeatTime =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.leaderHeartBeatTime", 150, kRpcConfId,
                                           "leader心跳循环时间ms");

static uint64_t getRandFollowerElectionTimeMs() {
    static std::mt19937 m_rng(std::random_device{}());
    static std::uniform_int_distribution<uint64_t> m_election_dist(g_followerElectionTimeMin->getValue(),
                                                                   g_followerElectionTimeMax->getValue());
    return m_election_dist(m_rng);
}



// 节点状态 ----------------------------------------
void NodeState::init(int id, size_t node_count) {
    m_id = id;
    m_votes.resize(node_count, 0);
}

bool NodeState::becomeFollower(uint64_t term) {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为follower";
    if (term < m_term) {
        M_SYLAR_LOG_ERROR(g_logger) << "变为follower时term减小";
    }

    m_term = term;
    m_node_state = State::Follower;
    return true;
}


bool NodeState::becomeCandidate() {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为candidate";



    m_node_state = State::Candidate;
    m_voted_for = m_id;

    m_term++;
    // 重置投票列表
    for (unsigned long & m_vote : m_votes) {
        m_vote = 0;
    }
    m_votes[m_id] = 1;
    m_vote_count = 1;
    return true;
}


bool NodeState::becomeLeader() {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为leader";
    if (m_node_state != State::Candidate) {
        M_SYLAR_LOG_WARN(g_logger) << "无效状态：无法从非candidate状态转换到leader状态，当前状态：" << static_cast<int>(m_node_state);
        return false;
    }

    m_node_state = State::Leader;
    m_voted_for = m_id;

    return true;
}


// 从id节点获得了一票
NodeState::VoteState NodeState::gainVote(int id, size_t term) {
    if (term != m_term || m_node_state != State::Candidate) {
        return VoteState::FAILED;
    }

    if (id < 0 || static_cast<size_t>(id) >= m_votes.size()) {
        return VoteState::FAILED;
    }

    if (m_votes[id] != 0) {
        return VoteState::FAILED;
    }

    m_votes[id] = 1;
    ++m_vote_count;
    return VoteState::SUCCESS;
}


NodeState::VoteState NodeState::castVote(int id, size_t term, const NodeLog& log, size_t last_log_index, size_t last_log_term) {
    if (term < m_term) {
        return VoteState::FAILED;
    }

    if (term > m_term) {
        m_term = term;
        m_node_state = State::Follower;
        m_voted_for = -1;
        return VoteState::RETRY;
    }

    if (m_voted_for != -1 && m_voted_for != id) {
        return VoteState::FAILED;
    }

    if (last_log_term < log.getLastLogTerm()) {
        return VoteState::FAILED;
    }

    if (last_log_term == log.getLastLogTerm() &&
        last_log_index < log.getLastLogIdx()) {
        return VoteState::FAILED;
        }

    m_voted_for = id;
    return VoteState::SUCCESS;
}


// 节点总控 ----------------------------------------
RaftNode::RaftNode() {
}


RaftNode::~RaftNode() {

}


// 流程控制
m_sylar::Task<void> RaftNode::init(craft::RPC::RpcDefine& define, int node_id) {
    if (m_rpc_state != 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(uninit) to start";
    }

    // 初始化节点状态信息
    m_node_count = define.nodes.size();
    m_state.init(node_id, m_node_count);
    m_peers_state.resize(m_node_count);

    // 初始化manager
    m_id = node_id;
    m_mgr = craft::RPCManager::GetInstanceWithOutInit();
    m_mgr->create();

    // 节点数据初始化
    co_await m_log.init(m_id);

    // 初始化client,server
    const int peers = co_await m_mgr->getClient()->init(define, m_id);
    int rt = co_await m_mgr->getServer()->init(define, m_id);
    if (rt == -1) {
        M_SYLAR_LOG_ERROR(g_logger) << " failed to init server";
        m_rpc_state = -1;
        co_return;
    }

    // 初始化定时器
    // 配置节点定时器时间
    m_recent_leader_heart = m_sylar::GetCurrentTime_ms();
    m_election_interval_time = getRandFollowerElectionTimeMs();
    // 选举定时器配置
    if (m_election_timer != nullptr) { m_election_timer->cancel(); m_election_timer = nullptr; }
    m_election_timer = m_sylar::TimeTask::create(m_election_interval_time, true,
        [node = shared_from_this()](m_sylar::TimeTask::ptr task) -> m_sylar::Task<void> {
            // 检查leader心跳
            uint64_t curr_time = m_sylar::GetCurrentTime_ms();
            if (node->getState() == NodeState::State::Leader) {
                M_SYLAR_LOG_DEBUG(g_logger) << "当前节点是leader却在尝试发送选举请求";
                co_return;
            }
            if (curr_time - node->m_recent_leader_heart > node->m_election_interval_time) {
                // 选举定时器超时， 调度选举处理函数
                m_sylar::IOManager::getInstance()->schedule(m_sylar::TaskCoro20::create_coro(
                    std::bind(&RaftNode::startElection, node)));
            }
            co_return;
        }
    );
    m_sylar::TimeManager::getInstance()->addTimer(m_election_timer);

    // leader心跳定时器配置
    if (m_leader_timer != nullptr) { m_leader_timer->cancel(); m_leader_timer = nullptr; }
    m_leader_timer = m_sylar::TimeTask::create(g_leaderHeartBeatTime->getValue(), true,
        [node = shared_from_this()](m_sylar::TimeTask::ptr task) -> m_sylar::Task<void> {
            // TODO 调度心跳处理函数
            M_SYLAR_LOG_DEBUG(g_logger) << "leader 调度了心跳函数";
            m_sylar::IOManager::getInstance()->schedule(
                m_sylar::TaskCoro20::create_coro(
                    std::bind(&RaftNode::heartBeat, node)));
            co_return;
        }
    );


    // 路由注册
    // 投票请求
    RPCManager::GetInstanceWithOutInit()->getServer()->
        registeRoute("craft", "requestVote",std::bind(
            &RaftNode::handleRequestVote,
            this,
            std::placeholders::_1,
            std::placeholders::_2
    ));

    // appendEntries
    RPCManager::GetInstanceWithOutInit()->getServer()->
        registeRoute("craft", "appendEntries",std::bind(
            &RaftNode::handleAppendEntries,
            this,
            std::placeholders::_1,
            std::placeholders::_2
    ));

    // sync
    RPCManager::GetInstanceWithOutInit()->getServer()->
        registeRoute("craft", "syncMessage",std::bind(
            &RaftNode::handleSyncNodeMessage,
            this,
            std::placeholders::_1,
            std::placeholders::_2
    ));

    // 其他变量设置
    m_rpc_state = 1;
    co_return;
}


void RaftNode::start() {
    if (m_rpc_state != 1) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(unstart) to start";
    }
    m_rpc_state = 2;
}


m_sylar::Task<void> RaftNode::stop() {
    m_rpc_state = 3;
    co_return;
}


// 选举函数,选举超时后使用
m_sylar::Task<void, m_sylar::TaskBeginExecuter> RaftNode::startElection() {
    if (m_rpc_state == 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: not started";
        co_return;
    }


    // 加锁
    m_sylar::CoUniqueLock w_lock(m_mutex);
    co_await w_lock.lock();
    // 成为candidatte
    co_await becomeCandidate();
    // 装配请求
    auto req_bytes = std::make_shared<std::string>();
    craft::RequestVoteArgs req_vote;
    req_vote.set_term(m_state.getTerm());                      // 任期
    req_vote.set_candidate_id(m_state.getId());                 // 节点id
    req_vote.set_last_log_index(m_log.getLastLogIdx());
    req_vote.set_last_log_term(m_log.getLastLogTerm());
    *req_bytes = req_vote.SerializeAsString();



    w_lock.unlock();

    // 轮训所有节点, 发送成为leader的请求.
    auto resp_bytes = std::make_shared<std::string>();
    // TODO 改为并行发送请求
    for (int i = 0; i < m_node_count; i++) {
        if (i == m_id) {continue; }

        RPC::CallState st = co_await m_mgr->getClient()->call(i, "craft", "requestVote", req_bytes, resp_bytes);
        if (st != RPC::CallState::SUCCESS) {
            M_SYLAR_LOG_DEBUG(g_logger) << "failed to request vote to node {" << i << "}";
            continue;
        }

        // resp 解析
        co_await w_lock.lock();
        RequestVoteReply reply;
        reply.ParseFromString(*resp_bytes);
        if (reply.vote_granted() == true) {
            if (m_state.gainVote(i, reply.term()) == NodeState::VoteState::SUCCESS) {
                // 从i中获得了一个投票
                M_SYLAR_LOG_DEBUG(g_logger) << "从节点 " << i << " 中获得一个投票，当前状态：term "
                    << m_state.getTerm() << " votes: " << m_state.getVoteCount() << "/" << m_node_count;
            }
            else {
                M_SYLAR_LOG_INFO(g_logger) << "未能从节点 " << i << " 中获得投票";
                if (m_state.getTerm() < reply.term()) {
                    M_SYLAR_LOG_DEBUG(g_logger) << "对方此时term较大，终止选举，回退为follower";
                    co_await becomeFollower(reply.term());
                    co_return;
                }
            }
        }

        // 停止检查
        if (m_state.getVoteCount() * 2 > m_state.getNodeCount()) {
            co_await becomeLeader();
            break;
        }
        w_lock.unlock();
    }
    w_lock.unlock();
    // 同步状态信息
    co_await SyncNodeMessage();
    co_return;
}


m_sylar::Task<void> RaftNode::handleRequestVote
        (std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    // 解析请求
    RequestVoteArgs req_vote;
    req_vote.ParseFromString(req->data());

    RequestVoteReply reply;
    // 节点状态设置与响应构建
    {
        m_sylar::CoUniqueLock w_lock(m_mutex);
        co_await w_lock.lock();
        // 更新状态并设置响应内容
        NodeState::VoteState vote_granted = NodeState::VoteState::RETRY;
        while (vote_granted == NodeState::VoteState::RETRY) {
            vote_granted = m_state.castVote(req_vote.candidate_id(), req_vote.term(),
                         m_log,
                         req_vote.last_log_index(), req_vote.last_log_term());
        }
        reply.set_vote_granted(vote_granted == NodeState::VoteState::SUCCESS);
        reply.set_term(req_vote.term());

        if (vote_granted == NodeState::VoteState::SUCCESS) {
            // 定时器改变
            co_await becomeFollower(req_vote.term());
        }
    }

    // 响应回复
    RPC::Response resp;
    resp.set_id(req->id());
    resp.set_code(static_cast<int>(RPC::RpcCode::OK));
    resp.set_data(reply.SerializeAsString());

    m_sylar::IOState st = co_await session->co_sendRpcMessage(resp);
    if (st != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_WARN(g_logger) <<  "选举响应失败";
    }
    co_return ;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> RaftNode::appendEntries(std::shared_ptr<AppendEntriesArgs> entry) {
    auto req_bytes = std::make_shared<std::string>();
    auto resp_bytes = std::make_shared<std::string>();
    *req_bytes = entry->SerializeAsString();


    // 状态:

    // TODO 改为并行发送请求
    for (int i = 0; i < m_node_count; i++) {
        if (i == m_id) {continue; }

        RPC::CallState st = co_await m_mgr->getClient()->call(i, "craft", "appendEntries", req_bytes, resp_bytes);
        if (st != RPC::CallState::SUCCESS) {
            M_SYLAR_LOG_DEBUG(g_logger) << "和节点 {" << i << "} 复制日志失败";
            continue;
        }

        // resp 解析
        AppendEntriesReply reply;
        reply.ParseFromString(*resp_bytes);     // 响应

        // TODO 对响应处理, 如对方term大...
        {
            m_sylar::CoUniqueLock w_lock(m_mutex);
            co_await w_lock.lock();
            if (reply.term() > m_state.getTerm()) {     // 对方term更大
                // TODO 回退至follower
                co_await becomeFollower(reply.term());
                co_return;
            }
            else if (!reply.success()) {        // 对方term和自己一样但拒绝了，需回退日志
                // TODO 逐次回退到最近未同步
            }
            else {      // 同步成功，等待下次同步
                // TODO 继续同步逻辑
            }
        }
    }
    co_return;
}


m_sylar::Task<void> RaftNode::handleAppendEntries
            (std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    // 日志处理函数
    AppendEntriesArgs entry;
    entry.ParseFromString(req->data());
    // 心跳分支
    if (entry.entries().size() == 0) {    // 心跳entry
        RPC::Response resp;
        {
            m_sylar::CoUniqueLock w_lock(m_mutex);
            co_await w_lock.lock();
            AppendEntriesReply reply = co_await handleHeartBeat(entry);
            resp.set_id(req->id());
            resp.set_code(static_cast<int>(RPC::RpcCode::OK));
            resp.set_data(reply.SerializeAsString());
        }

        co_await session->co_sendRpcMessage(resp);
        co_return;
    }

    // TODO 日志处理
    {
        m_sylar::CoUniqueLock w_lock(m_mutex);
        co_await w_lock.lock();
        M_SYLAR_LOG_INFO(g_logger) << "暂无日志处理逻辑";
    }
    co_return;
}


m_sylar::Task<void> RaftNode::SyncNodeMessage() {
    auto req_bytes = std::make_shared<std::string>();
    auto resp_bytes = std::make_shared<std::string>();

    std::vector<PeerState> temps_state;
    temps_state.resize(m_node_count);
    // TODO 改为并行发送请求
    for (int i = 0; i < m_node_count; i++) {
        if (i == m_id) {continue; }

        RPC::CallState st = co_await m_mgr->getClient()->call(i, "craft", "syncMessage", req_bytes, resp_bytes);
        if (st != RPC::CallState::SUCCESS) {
            M_SYLAR_LOG_DEBUG(g_logger) << "未能和节点 {" << i << "} 同步日志";
            continue;
        }

        // resp 解析
        SyncMessageReply reply;
        reply.ParseFromString(*resp_bytes);     // 响应
        // TODO 对响应处理, 如对方term大...
        {
            if (reply.term() > m_state.getTerm()) {     // 对方term更大
                // TODO 回退至follower
                co_await becomeFollower(reply.term());
                co_return;
            }
            // 更新同步信息
            temps_state[i].last_log_index = reply.last_log_index();
            temps_state[i].commit_log_index = reply.commit_log_index();
            temps_state[i].state = PeerState::State::Ready;
        }
    }

    // 更新信息
    m_sylar::CoUniqueLock w_lock(m_mutex);
    co_await w_lock.lock();
    m_peers_state = temps_state;
    co_return;
}


static std::ostream& operator<< (std::ostream& os, const SyncMessageReply& message) {
    os  << "\ncommit_log_index: " << message.commit_log_index()
        << "\nlast_log_index:  " << message.last_log_index() << "\n";
}

m_sylar::Task<void> RaftNode::handleSyncNodeMessage(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    // 日志处理函数
    // 心跳分支
    RPC::Response resp;
    {
        m_sylar::CoUniqueLock w_lock(m_mutex);
        co_await w_lock.lock();
        SyncMessageReply reply;
        reply.set_commit_log_index(m_log.getCommitIndex());
        reply.set_last_log_index(m_log.getLastLogIdx());
        M_SYLAR_LOG_DEBUG(g_logger) << "[handleSyncNodeMessage] 节点信息同步: " << reply;
        resp.set_id(req->id());
        resp.set_code(static_cast<int>(RPC::RpcCode::OK));
        resp.set_data(reply.SerializeAsString());
    }

    co_await session->co_sendRpcMessage(resp);
    co_return;
}



m_sylar::Task<void, m_sylar::TaskBeginExecuter> RaftNode::heartBeat() {

    // 心跳构造
    std::shared_ptr<AppendEntriesArgs> entries = std::make_shared<AppendEntriesArgs>();

    {
        m_sylar::CoUniqueLock w_lock(m_mutex);
        co_await w_lock.lock();
        // 状态检查
        if (m_state.getState() != NodeState::State::Leader) {
            co_return;
        }

        entries->set_term(m_state.getTerm());
        entries->set_leader_commit(m_log.getCommitIndex());
        entries->set_leader_id(m_id);
        entries->set_prev_log_index(m_log.getLastLogIdx());
        entries->set_prev_log_term(m_log.getLastLogTerm());
    }



    m_sylar::IOManager::getInstance()->schedule(
            m_sylar::TaskCoro20::create_coro(std::bind(&RaftNode::appendEntries, this, entries)));
}


m_sylar::Task<AppendEntriesReply> RaftNode::handleHeartBeat(const AppendEntriesArgs& entries) {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 收到主节点 " << entries.leader_id() << " 的心跳， term: " << m_state.getTerm();
    AppendEntriesReply reply;
    reply.set_term(m_state.getTerm());
    // 更新定时器信息
    // if (m_state.getState() == NodeState::State::Leader) {
    //     reply.set_success(false);
    //     M_SYLAR_LOG_WARN(g_logger) << "[非法状态] 节点 " << m_id << " 是leader但收到请心跳";
    //     co_return reply;
    // }
    if (entries.term() < m_state.getTerm()) {
        reply.set_success(false);
        co_return reply;
    }

    // 成为follower并更新定时器信息
    co_await becomeFollower(entries.term());
    m_recent_leader_heart = m_sylar::GetCurrentTime_ms();
    reply.set_term(m_state.getTerm());

    // 检查日志状态
    if (!m_log.matchLog(entries.prev_log_index(), entries.prev_log_term())) {       // 日志匹配
        reply.set_success(false);
        co_return reply;
    }

    reply.set_success(true);
    co_return reply;
}



m_sylar::Task<bool> RaftNode::becomeFollower(uint64_t term) {

    if (m_state.getState() == NodeState::State::Follower && term == m_state.getTerm()) {
        m_recent_leader_heart = m_sylar::GetCurrentTime_ms();
        co_return true;
    }
    if (!m_leader_timer->getIsCanceled()) {
        m_leader_timer->cancel();
    }
    if (m_election_timer->getIsCanceled()) {
        m_election_timer->restart();
    }

    co_return m_state.becomeFollower(term);
}

m_sylar::Task<bool>  RaftNode::becomeCandidate() {
    // if (m_state.getState() == NodeState::State::Candidate) {
    //     co_return true;
    // }

    if (!m_leader_timer->getIsCanceled()) {
        m_leader_timer->cancel();
    }
    if (m_election_timer->getIsCanceled()) {
        m_election_timer->restart();
    }
    co_return m_state.becomeCandidate();

}

m_sylar::Task<bool>  RaftNode::becomeLeader() {
    if (m_state.getState() == NodeState::State::Leader) {
        co_return true;
    }
    // 启动定时器
    if (m_leader_timer->getIsCanceled()) {
        m_leader_timer->restart();
    }
    if (!m_election_timer->getIsCanceled()) {
        m_election_timer->cancel();
    }



    co_return m_state.becomeLeader();
}

}
