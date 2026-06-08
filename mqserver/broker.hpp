#ifndef __M_BROKER_H__
#define __M_BROKER_H__

#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
// #include "muduo/base/Logging.h"
// #include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpServer.h"

#include "virtualhost.hpp"
#include "consumer.hpp"
#include "connection.hpp"
#include "../mqcommon/threadpool.hpp"
#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include "../mqcommon/proto.pb.h"

namespace mq
{
#define DBFILE "./data/meta.db"
#define BASEDIR "./data/message"
    class server
    {
    public:
        server(int port, const std::string &basedir) : _server(&_loop, muduo::net::InetAddress("0.0.0.0", port), "server", muduo::net::TcpServer::kNoReusePort),
                                                       _dispatcher([this](const muduo::net::TcpConnectionPtr &tcptr, const MessagePtr &message, muduo::Timestamp receiveTime)
                                                                   { onUnknownMessage(tcptr, message, receiveTime); }),
                                                       _codec(std::make_shared<ProtobufCodec>([this](const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp receiveTime)
                                                                                              { _dispatcher.onProtobufMessage(conn, message, receiveTime); })),
                                                       _host_ptr(std::make_shared<VirtualHost>("host", DBFILE, BASEDIR)),
                                                       _connsumer_ptr(std::make_shared<ConsumerManager>()),
                                                       _connection_ptr(std::make_shared<ConnectionManager>()),
                                                       _pool_ptr(std::make_shared<Threadpool>())
        {
            // 初始化消费者队列
            // 首先获取所有的队列
            QueueMap queues = _host_ptr->allQueues();
            for (auto &queue : queues)
            {
                _connsumer_ptr->initQueueConsumer(queue.first);
            }

            // 注册12个业务处理函数
            // 信道的打开与关闭
            _dispatcher.registerMessageCallback<mq::openChannelRequst>([this](const muduo::net::TcpConnectionPtr &conn, const openChannelRequstptr &message, muduo::Timestamp receiveTime)
                                                                       { openChannelRequst(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::cloceChannelRequst>([this](const muduo::net::TcpConnectionPtr &conn, const cloceChannelRequstptr &message, muduo::Timestamp receiveTime)
                                                                        { cloceChannelRequst(conn, message, receiveTime); });
            // 交换机的声明与删除
            _dispatcher.registerMessageCallback<mq::declareExchangeRequest>([this](const muduo::net::TcpConnectionPtr &conn, const declareExchangeRequestptr &message, muduo::Timestamp receiveTime)
                                                                            { declareExchangeRequest(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::deleteExchangeRequst>([this](const muduo::net::TcpConnectionPtr &conn, const deleteExchangeRequstptr &message, muduo::Timestamp receiveTime)
                                                                          { deleteExchangeRequst(conn, message, receiveTime); });
            // 队列的声明与删除
            _dispatcher.registerMessageCallback<mq::declareQueueRequst>([this](const muduo::net::TcpConnectionPtr &conn, const declareQueueRequstptr &message, muduo::Timestamp receiveTime)
                                                                        { declareQueueRequst(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::deleteQueueRequest>([this](const muduo::net::TcpConnectionPtr &conn, const deleteQueueRequestptr &message, muduo::Timestamp receiveTime)
                                                                        { deleteQueueRequest(conn, message, receiveTime); });
            // 绑定与解除绑定
            _dispatcher.registerMessageCallback<mq::queueBindRequest>([this](const muduo::net::TcpConnectionPtr &conn, const queueBindRequestptr &message, muduo::Timestamp receiveTime)
                                                                      { queueBindRequest(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::queueUnbindRequest>([this](const muduo::net::TcpConnectionPtr &conn, const queueUnbindRequestptr &message, muduo::Timestamp receiveTime)
                                                                        { queueUnbindRequest(conn, message, receiveTime); });
            // 消息的发布与确认
            _dispatcher.registerMessageCallback<mq::basicPublishRequest>([this](const muduo::net::TcpConnectionPtr &conn, const basicPublishRequestptr &message, muduo::Timestamp receiveTime)
                                                                         { basicPublishRequest(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::basicAckRequest>([this](const muduo::net::TcpConnectionPtr &conn, const basicAckRequestptr &message, muduo::Timestamp receiveTime)
                                                                     { basicAckRequest(conn, message, receiveTime); });
            // 队列的订阅与取消
            _dispatcher.registerMessageCallback<mq::basicConsumeRequest>([this](const muduo::net::TcpConnectionPtr &conn, const basicConsumeRequestptr &message, muduo::Timestamp receiveTime)
                                                                         { basicConsumeRequest(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::basicCancelRequest>([this](const muduo::net::TcpConnectionPtr &conn, const basicCancelRequestptr &message, muduo::Timestamp receiveTime)
                                                                        { basicCancelRequest(conn, message, receiveTime); });
            // 设置连接回调函数和信息回调函数
            _server.setMessageCallback([this](const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buf, muduo::Timestamp receiveTime)
                                       { _codec->onMessage(conn, buf, receiveTime); });
            _server.setConnectionCallback([this](const muduo::net::TcpConnectionPtr &conn)
                                          { onConnection(conn); });
        }

        void start()
        {
            _server.start();
            _loop.loop();
            DBG_LOG("服务器启动成功!");
        }

    private:
        // 未知消息处理函数
        void onUnknownMessage(const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp)
        {
            DBG_LOG("未知的请求!%s", message->GetTypeName().c_str());
            conn->shutdown();
        }
        // 信道的打开与关闭
        void openChannelRequst(const muduo::net::TcpConnectionPtr &conn, const openChannelRequstptr &message, muduo::Timestamp)
        {
            // 查找连接是否存在
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("打开信道时,未知的连接!")
                conn->shutdown();
                return;
            }
            return connptr->openChannel(message);
        }
        void cloceChannelRequst(const muduo::net::TcpConnectionPtr &conn, const cloceChannelRequstptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("关闭信道时,未知的连接!")
                conn->shutdown();
                return;
            }
            return connptr->closeChannel(message);
        }
        // 交换机的声明与删除
        void declareExchangeRequest(const muduo::net::TcpConnectionPtr &conn, const declareExchangeRequestptr &message, muduo::Timestamp)
        {
            // 在连接管理句柄中查找对应的连接
            // 找到连接后,从连接中查找对应的信道
            // 在信道中进行相关的操作
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("声明交换机时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("声明交换机时,没有找到对应的信道!")
                return;
            }
            return chptr->declareExchange(message);
        }
        void deleteExchangeRequst(const muduo::net::TcpConnectionPtr &conn, const deleteExchangeRequstptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("删除交换机时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("删除交换机时,没有找到对应的信道!")
                return;
            }
            return chptr->deleteExchange(message);
        }
        // 队列的声明与删除
        void declareQueueRequst(const muduo::net::TcpConnectionPtr &conn, const declareQueueRequstptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("声明队列时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("声明队列时,没有找到对应的信道!")
                return;
            }
            return chptr->declareQueue(message);
        }
        void deleteQueueRequest(const muduo::net::TcpConnectionPtr &conn, const deleteQueueRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("删除队列时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("删除队列时,没有找到对应的信道!")
                return;
            }
            return chptr->deleteQueue(message);
        }
        // 绑定与解除绑定
        void queueBindRequest(const muduo::net::TcpConnectionPtr &conn, const queueBindRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("进行绑定时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("进行绑定时,没有找到对应的信道!")
                return;
            }
            return chptr->queueBind(message);
        }
        void queueUnbindRequest(const muduo::net::TcpConnectionPtr &conn, const queueUnbindRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("解除绑定时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("解除绑定时,没有找到对应的信道!")
                return;
            }
            return chptr->queueUnbind(message);
        }
        // 消息的发布与确认
        void basicPublishRequest(const muduo::net::TcpConnectionPtr &conn, const basicPublishRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("消息发布时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("消息发布时,没有找到对应的信道!")
                return;
            }
            return chptr->basicPublish(message);
        }
        void basicAckRequest(const muduo::net::TcpConnectionPtr &conn, const basicAckRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("消息确认时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("消息确认时,没有找到对应的信道!")
                return;
            }
            return chptr->basicAck(message);
        }
        // 队列的订阅与取消
        void basicConsumeRequest(const muduo::net::TcpConnectionPtr &conn, const basicConsumeRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("订阅队列消息时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("订阅队列消息时,没有找到对应的信道!")
                return;
            }
            return chptr->basicConsume(message);
        }
        void basicCancelRequest(const muduo::net::TcpConnectionPtr &conn, const basicCancelRequestptr &message, muduo::Timestamp)
        {
            Connection::ptr connptr = _connection_ptr->getConnection(conn);
            if (connptr.get() == nullptr)
            {
                DBG_LOG("取消订阅队列消息时,未知的连接!")
                conn->shutdown();
                return;
            }
            Channel::ptr chptr = connptr->getChannel(message->cid());
            if (chptr.get() == nullptr)
            {
                DBG_LOG("取消订阅队列消息时,没有找到对应的信道!")
                return;
            }
            return chptr->basicCancel(message);
        }

        // 连接成功或关闭时回调函数
        void onConnection(const muduo::net::TcpConnectionPtr &conn)
        {
            // 新连接建立时调用
            if (conn->connected() == true)
            {
                DBG_LOG("新连接建立成功");
                _connection_ptr->newConnection(conn, _codec, _connsumer_ptr, _host_ptr, _pool_ptr);
            }
            else
            {
                DBG_LOG("连接已关闭");
                _connection_ptr->deleteConnection(conn);
            }
        }

    private:
        muduo::net::EventLoop _loop;    // 事件监控对象
        muduo::net::TcpServer _server;  // 服务器对象
        ProtobufDispatcher _dispatcher; // 请求分发器对象--要向其中注册请求处理函数
        ProtobufCodecPtr _codec;        // protobuf协议处理器--针对收到的请求进行protobuf协议处理
        VirtualHost::ptr _host_ptr;
        ConsumerManager::ptr _connsumer_ptr;
        ConnectionManager::ptr _connection_ptr;
        Threadpool::ptr _pool_ptr;
    };
}

#endif