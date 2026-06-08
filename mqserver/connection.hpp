#ifndef __M_CONNECTION_H__
#define __M_CONNECTION_H__

#include "channel.hpp"
namespace mq
{
    using openChannelRequstptr = std::shared_ptr<openChannelRequst>;
    using cloceChannelRequstptr = std::shared_ptr<cloceChannelRequst>;
    class Connection
    {
    public:
        using ptr = std::shared_ptr<Connection>;

        Connection(const muduo::net::TcpConnectionPtr &connptr,
                   const ProtobufCodecPtr &codecptr,
                   const ConsumerManager::ptr &cmptr,
                   const VirtualHost::ptr &host,
                   const Threadpool::ptr &pool) : _connptr(connptr),
                                                  _codecptr(codecptr),
                                                  _cmptr(cmptr),
                                                  _host(host),
                                                  _pool(pool),
                                                  _chmptr(std::make_shared<ChannelManager>()) {}

        void openChannel(const openChannelRequstptr &req)
        {
            // 判断信道是否存在
            bool ret = _chmptr->openChannel(req->cid(), _connptr, _codecptr, _cmptr, _host, _pool);
            if (ret == false)
            {
                DBG_LOG("信道id已存在!");
                return basicResponse(req->rid(), req->cid(), false);
            }
            // 给客户端发送回复
            DBG_LOG("信道%s打开成功!", req->cid().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }
        void closeChannel(const cloceChannelRequstptr &req)
        {
            if (_chmptr->getChannel(req->cid()).get() == nullptr)
            {
                return basicResponse(req->rid(), req->cid(), false);
            }
            _chmptr->closeChannel(req->cid());
            DBG_LOG("信道%s关闭成功!", req->cid().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }

        Channel::ptr getChannel(const std::string &id)
        {
            return _chmptr->getChannel(id);
        }

    private:
        void basicResponse(const std::string &rid, const std::string &cid, bool ok)
        {
            basicCommonResponse resp;
            resp.set_rid(rid);
            resp.set_cid(cid);
            resp.set_ok(ok);
            _codecptr->send(_connptr, resp);
        }

    private:
        muduo::net::TcpConnectionPtr _connptr; // 连接的操作句柄
        ProtobufCodecPtr _codecptr;            // 协议管理器句柄
        ConsumerManager::ptr _cmptr;           // 消费者管理句柄
        VirtualHost::ptr _host;                // 虚拟机的管理句柄
        Threadpool::ptr _pool;                 // 线程池的管理句柄
        ChannelManager::ptr _chmptr;           // 信道的管理句柄
    };

    class ConnectionManager
    {
    public:
        using ptr = std::shared_ptr<ConnectionManager>;
        ConnectionManager() {};
        void newConnection(const muduo::net::TcpConnectionPtr &connptr,
                           const ProtobufCodecPtr &codecptr,
                           const ConsumerManager::ptr &cmptr,
                           const VirtualHost::ptr &host,
                           const Threadpool::ptr &pool)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _connections.find(connptr);
            if (it != _connections.end())
            {
                DBG_LOG("连接已存在!");
                return;
            }
            Connection::ptr conn = std::make_shared<Connection>(connptr, codecptr, cmptr, host, pool);
            _connections.insert(std::make_pair(connptr, conn));
        }
        void deleteConnection(const muduo::net::TcpConnectionPtr &connptr)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _connections.erase(connptr);
        }
        Connection::ptr getConnection(const muduo::net::TcpConnectionPtr &connptr)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _connections.find(connptr);
            if (it == _connections.end())
            {
                DBG_LOG("获取失败,连接不存在!");
                return Connection::ptr();
            }
            return it->second;
        }

    private:
        std::mutex _mutex;
        std::unordered_map<muduo::net::TcpConnectionPtr, Connection::ptr> _connections;
    };
}

#endif