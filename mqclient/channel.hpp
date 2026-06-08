#ifndef _M__CHANNEL__H_
#define _M__CHANNEL__H_

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include "../mqcommon/proto.pb.h"
#include <memory>
#include <condition_variable>

#include "consumer.hpp"

#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"

namespace mq
{
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;

    using basicConsumeResponsePtr = std::shared_ptr<basicConsumeResponse>;
    using basicCommonResponsePtr = std::shared_ptr<basicCommonResponse>;
    class Channel
    {
    public:
        using ptr = std::shared_ptr<Channel>;
        Channel(const muduo::net::TcpConnectionPtr &connptr,
                const ProtobufCodecPtr &codecptr) : _cid(UUIDHelper::uuid()), _connptr(connptr), _codecptr(codecptr) {}
        // 信道的打开与关闭
        bool openChannel()
        {
            const std::string rid = UUIDHelper::uuid();
            openChannelRequst req;
            req.set_rid(rid);
            req.set_cid(_cid);
            // 向服务端发送请求
            _codecptr->send(_connptr, req);
            // 等待服务端响应
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            // 返回
            if (resp->ok() == false)
            {
                DBG_LOG("信道打开失败!");
                return false;
            }
            return resp->ok();
        }
        bool closeChannel()
        {
            const std::string rid = UUIDHelper::uuid();
            cloceChannelRequst req;
            req.set_rid(rid);
            req.set_cid(_cid);
            // 向服务端发送请求
            _codecptr->send(_connptr, req);
            // 等待服务端响应
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            // 返回
            if (resp->ok() == false)
            {
                DBG_LOG("信道关闭失败!");
                return false;
            }
            return resp->ok();
        }
        // 交换机的声明与删除
        bool declareExchange(const std::string &exchange_name,
                             ExchangeType exchange_type,
                             DeliveryMode durable,
                             bool auto_delete,
                             google::protobuf::Map<std::string, std::string> &args)
        {
            // 构造声明交换机请求对象
            std::string rid = UUIDHelper::uuid();
            declareExchangeRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(exchange_name);
            req.set_exchange_type(exchange_type);
            req.set_durable(durable);
            req.set_auto_delete(auto_delete);
            req.mutable_args()->swap(args);
            // 向服务端发送请求
            _codecptr->send(_connptr, req);
            // 等待服务端响应
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            // 返回
            return resp->ok();
        }
        bool deleteExchange(const std::string &exchange_name)
        {
            // 构造删除交换机请求duixiang
            std::string rid = UUIDHelper::uuid();
            deleteExchangeRequst req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(exchange_name);
            // 向服务器发送请求
            _codecptr->send(_connptr, req);
            // 等待服务器响应
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            // 返回
            return resp->ok();
        }
        // 队列的声明与删除
        bool declareQueue(const std::string &queue_name,
                          bool durable,
                          bool exclusive,
                          bool auto_delete,
                          google::protobuf::Map<std::string, std::string> &args)
        {
            std::string rid = UUIDHelper::uuid();
            declareQueueRequst req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(queue_name);
            req.set_durable(durable);
            req.set_exclusive(exclusive);
            req.set_auto_delete(auto_delete);
            req.mutable_args()->swap(args);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        bool deleteQueue(const std::string &queue_name)
        {
            std::string rid = UUIDHelper::uuid();
            deleteQueueRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(queue_name);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        // 绑定与解除绑定
        bool queueBind(const std::string &exchange_name, const std::string &queue_name, const std::string &binding_key)
        {
            std::string rid = UUIDHelper::uuid();
            queueBindRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(exchange_name);
            req.set_queue_name(queue_name);
            req.set_binding_key(binding_key);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        bool queueUnbind(const std::string &exchange_name, const std::string &queue_name)
        {
            std::string rid = UUIDHelper::uuid();
            queueUnbindRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(exchange_name);
            req.set_queue_name(queue_name);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        // 消息的发布与确认
        bool basicPublish(const std::string &exchange_name, const BasecProperties *properties, const std::string &body)
        {
            std::string rid = UUIDHelper::uuid();
            basicPublishRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(exchange_name);
            if (properties != nullptr)
            {
                req.mutable_properties()->set_id(properties->id());
                req.mutable_properties()->set_deliver_mode(properties->deliver_mode());
                req.mutable_properties()->set_roting_key(properties->roting_key());
            }
            req.set_body(body);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        bool basicAck(const std::string &message_id)
        {
            if (_consumer_ptr.get() == nullptr)
            {
                DBG_LOG("确认消息时,当前信道没有对应的消费者!");
                return false;
            }
            std::string rid = UUIDHelper::uuid();
            basicAckRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(_consumer_ptr->qname);
            req.set_message_id(message_id);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            return resp->ok();
        }
        // 队列的订阅与取消
        bool basicConsume(const std::string &consumer_tag, const std::string &queue_name, bool auto_ack, const ConsumerCallback &cb)
        {
            if (_consumer_ptr.get() != nullptr)
            {
                DBG_LOG("当前信道已订阅队列!");
                return false;
            }
            std::string rid = UUIDHelper::uuid();
            basicConsumeRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_consumer_tag(consumer_tag);
            req.set_queue_name(queue_name);
            req.set_auto_ack(auto_ack);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            if (resp->ok() == false)
            {
                DBG_LOG("订阅队列%s失败!", queue_name.c_str());
                return false;
            }
            _consumer_ptr = std::make_shared<Consumer>(consumer_tag, queue_name, auto_ack, cb);
            return true;
        }
        bool basicCancel()
        {
            std::string rid = UUIDHelper::uuid();
            basicCancelRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_consumer_tag(_consumer_ptr->tag);
            req.set_queue_name(_consumer_ptr->qname);
            _codecptr->send(_connptr, req);
            basicCommonResponsePtr resp = waitBasicResponse(rid);
            if (resp->ok() == false)
            {
                DBG_LOG("取消订阅队列%s失败!", _consumer_ptr->qname.c_str());
                return false;
            }
            _consumer_ptr.reset();
            return true;
        }

        std::string cid()
        {
            return _cid;
        }

        // 连接收到基础响应后,向_basic_resp中添加响应
        void putBasicResponse(const basicCommonResponsePtr &resp)
        {
            // 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 将服务器发送的响应添加到_basic_resp中
            _basic_resp[resp->rid()] = resp;
            // 唤醒等待的线程
            _cv.notify_all();
        }
        // 连接收到消息推送后,需要通过信道找到对应的消费者对象,通过回调函数进行消息处理
        void consume(const basicConsumeResponsePtr &resp)
        {
            if (_consumer_ptr.get() == nullptr)
            {
                DBG_LOG("没有订阅者对象!");
                return;
            }
            if (_consumer_ptr->tag != resp->consumer_tag())
            {
                DBG_LOG("收到的消息中消费者标识,与当前信道消费者标识不一致!");
                return;
            }
            _consumer_ptr->callback(resp->consumer_tag(), &resp->properties(), resp->body());
        }

    private:
        // 等待服务器的响应
        basicCommonResponsePtr waitBasicResponse(const std::string &rid)
        {
            // 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 等待条件变量,当_basic_resp中有对应rid的响应时唤醒
            _cv.wait(lock, [&rid, this]()
                     { return _basic_resp.find(rid) != _basic_resp.end(); });
            // 删除_basic_resp中对应rid的响应
            basicCommonResponsePtr resp = _basic_resp[rid];
            _basic_resp.erase(rid);
            // 返回对象
            return resp;
        }

    private:
        std::string _cid;                                                    // 信道的唯一标识
        muduo::net::TcpConnectionPtr _connptr;                               // 信道对应的连接
        ProtobufCodecPtr _codecptr;                                          // 协议处理器操作句柄
        Consumer::ptr _consumer_ptr;                                         // 信道对应的订阅者
        std::mutex _mutex;                                                   // 控制同步锁的互斥量
        std::condition_variable _cv;                                         // 控制同步锁的条件变量
        std::unordered_map<std::string, basicCommonResponsePtr> _basic_resp; // 存储着服务器发送的回应
    };

    class ChannelManager
    {
    public:
        using ptr = std::shared_ptr<ChannelManager>;
        Channel::ptr createChannel(muduo::net::TcpConnectionPtr connptr, ProtobufCodecPtr codecptr)
        {
            // 构造信道对象
            Channel::ptr cptr = std::make_shared<Channel>(connptr, codecptr);
            { // 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                // 将对象添加进hash管理
                _channels.insert(std::make_pair(cptr->cid(), cptr));
            }
            // 向服务端发送打开信道请求
            int ret = cptr->openChannel();
            if (ret == false)
            {
                // 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                // 从hash管理中删除对象
                _channels.erase(cptr->cid());
                return std::shared_ptr<Channel>();
            }
            return cptr;
        }
        void removeChannel(const std::string &cid)
        {
            std::unordered_map<std::string, mq::Channel::ptr>::iterator it;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                it = _channels.find(cid);
                if (it == _channels.end())
                {
                    DBG_LOG("没有找到要关闭的信道!");
                    return;
                }
            }
            it->second->closeChannel();
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _channels.erase(it);
            }
        }
        Channel::ptr getChannel(const std::string &cid)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(cid);
            if (it == _channels.end())
            {
                DBG_LOG("信道%s不存在!", cid.c_str());
            }
            return it->second;
        }

    private:
        std::mutex _mutex;
        std::unordered_map<std::string, Channel::ptr> _channels;
    };
}

#endif