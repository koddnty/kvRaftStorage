#pragma once
#include <sylar/basic/singleton.h>
#include "rpcServer.hpp"
#include  "rpcClient.hpp"


namespace craft {
class RPCManager : public m_sylar::Singleton<RPCManager> {
public:
    RPCClient::ptr getServer() {return m_server; }


private:
    RPCClient::ptr m_client;
    RPCClient::ptr m_server;

};
}