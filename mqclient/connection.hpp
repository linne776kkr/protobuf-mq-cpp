#ifndef __M_CONNECTION_H__
#define __M_CONNECTION_H__

#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"

#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpClient.h"

#include "muduo/base/CountDownLatch.h"
#include "muduo/net/EventLoopThread.h"

#include "channel.hpp"
#include "worker.hpp"

namespace mq
{
    class client
    {
    public:
        client(const std::string& ip, int port, const AsyncWorker::ptr &worker)
            : _latch(1),
              _worker(worker),
              _client(_worker->loopthread.startLoop(), muduo::net::InetAddress(ip, port), "client"),
              _dispatcher([this](const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp receiveTime)
                          { client::onUnknownMessage(conn, message, receiveTime); }),
              _codecptr(std::make_shared<ProtobufCodec>([this](const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp receiveTime)
                                                        { _dispatcher.onProtobufMessage(conn, message, receiveTime); })),
              _channels_ptr(std::make_shared<ChannelManager>())
        {
            // 注册消息分发器
            _dispatcher.registerMessageCallback<mq::basicConsumeResponse>([this](const muduo::net::TcpConnectionPtr &conn, const basicConsumeResponsePtr &message, muduo::Timestamp receiveTime)
                                                                          { basicConsume(conn, message, receiveTime); });
            _dispatcher.registerMessageCallback<mq::basicCommonResponse>([this](const muduo::net::TcpConnectionPtr &conn, const basicCommonResponsePtr &message, muduo::Timestamp receiveTime)
                                                                         { basicCommon(conn, message, receiveTime); });
            // 绑定连接回调函数和消息回调函数
            _client.setConnectionCallback([this](const muduo::net::TcpConnectionPtr &conn)
                                          { onConnection(conn); });
            _client.setMessageCallback([this](const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buf, muduo::Timestamp receiveTime)
                                       { _codecptr->onMessage(conn, buf, receiveTime); });

            // 连接服务器,在成功连接前需要等待
            _client.connect();
            _latch.wait();
        }

        Channel::ptr openChannel()
        {
            Channel::ptr cptr = _channels_ptr->createChannel(_connptr, _codecptr);
            return cptr;
        }
        void closeChannel(const Channel::ptr& cptr){
            _channels_ptr->removeChannel(cptr->cid());
        }

    private:
        // 连接成功时的回调函数,连接成功后,唤醒上方阻塞
        void onConnection(const muduo::net::TcpConnectionPtr &conn)
        {
            if (conn->connected() == true)
            {
                _connptr = conn;
                _latch.countDown();
                std::cout << "连接成功" << std::endl;
            }
            else
            {
                std::cout << "连接已断开" << std::endl;
            }
        }
        // 收到未知消息后调用的函数
        void onUnknownMessage(const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp)
        {
            DBG_LOG("未知的消息格式!");
        }
        // 收到推送消息回调函数
        void basicConsume(const muduo::net::TcpConnectionPtr &conn, const basicConsumeResponsePtr &message, muduo::Timestamp)
        {
            //找到对应的信道
            Channel::ptr cptr = _channels_ptr->getChannel(message->cid());
            if(cptr.get()==nullptr)
            {
                DBG_LOG("收到消息推送时,没有找到对应的信道!");
                return ;
            }
            //构建一个lambda任务加入到线程池中
            _worker->pool.push([cptr,message](){cptr->consume(message);});
        }
        // 基础消息响应回调函数
        void basicCommon(const muduo::net::TcpConnectionPtr &conn, const basicCommonResponsePtr &message, muduo::Timestamp)
        {
            //找到对应的信道
            Channel::ptr cptr = _channels_ptr->getChannel(message->cid());
            if(cptr.get()==nullptr)
            {
                DBG_LOG("收到消息回应时,没有找到对应的信道!");
                return ;
            }
             //构建一个lambda任务加入到线程池中
            //将响应信息添加到信道的hash表中
            _worker->pool.push([message,cptr](){cptr->putBasicResponse(message);});
        }

    private:
        muduo::CountDownLatch _latch;          // 实现同步的
        AsyncWorker::ptr _worker;              // 异步循环处理线程
        muduo::net::TcpConnectionPtr _connptr; // 客户端对应的连接
        muduo::net::TcpClient _client;         // 客户端对象
        ProtobufDispatcher _dispatcher;        // 请求分发器
        ProtobufCodecPtr _codecptr;            // 协议处理器
        ChannelManager::ptr _channels_ptr;     // 信道管理句柄
    };
}

#endif