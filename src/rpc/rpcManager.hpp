#pragma once
#include <sylar/basic/singleton.h>
#include "rpcServer.hpp"
#include  "rpcClient.hpp"


namespace craft {
class RPCManager : public m_sylar::Singleton<RPCManager> {
public:
    RPCServer::ptr getServer() {return m_server; }


private:
    RPCClient::ptr m_client;
    RPCServer::ptr m_server;

};
}