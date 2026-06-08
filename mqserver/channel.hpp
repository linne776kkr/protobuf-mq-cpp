#ifndef _M__CHANNEL__H_
#define _M__CHANNEL__H_

#include "../mqcommon/helper.hpp"
#include "../mqcommon/threadpool.hpp"
#include "../mqcommon/msg.pb.h"
#include "../mqcommon/proto.pb.h"
#include "memory"

#include "consumer.hpp"
#include "virtualhost.hpp"
#include "route.hpp"

#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"

namespace mq
{
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;

    using declareExchangeRequestptr = std::shared_ptr<declareExchangeRequest>;
    using deleteExchangeRequstptr = std::shared_ptr<deleteExchangeRequst>;
    using declareQueueRequstptr = std::shared_ptr<declareQueueRequst>;
    using deleteQueueRequestptr = std::shared_ptr<deleteQueueRequest>;
    using queueBindRequestptr = std::shared_ptr<queueBindRequest>;
    using queueUnbindRequestptr = std::shared_ptr<queueUnbindRequest>;
    using basicPublishRequestptr = std::shared_ptr<basicPublishRequest>;
    using basicAckRequestptr = std::shared_ptr<basicAckRequest>;
    using basicConsumeRequestptr = std::shared_ptr<basicConsumeRequest>;
    using basicCancelRequestptr = std::shared_ptr<basicCancelRequest>;

    class Channel
    {
    public:
        using ptr = std::shared_ptr<Channel>;
        Channel() {};
        Channel(const std::string &id,
                const muduo::net::TcpConnectionPtr &connptr,
                const ProtobufCodecPtr &codecptr,
                const ConsumerManager::ptr &cmptr,
                const VirtualHost::ptr &host,
                const Threadpool::ptr &pool) : _cid(id),
                                               _connptr(connptr),
                                               _codecptr(codecptr),
                                               _cmptr(cmptr),
                                               _host(host),
                                               _pool(pool) {}

        ~Channel()
        {
            if (_consumer.get() != nullptr)
            {
                _cmptr->remove(_consumer->tag, _consumer->qname);
            }
        }

        // 交换机的声明与删除
        void declareExchange(const declareExchangeRequestptr &req)
        {
            bool ret = _host->declareExchange(req->exchange_name(), req->exchange_type(), req->durable(), req->auto_delete(), req->args());
            if (ret == false)
            {
                DBG_LOG("交换机%s声明失败!", req->exchange_name().c_str());
            }
            else
            {
                DBG_LOG("交换机%s声明成功!", req->exchange_name().c_str());
                //打印交换机的类型
            }
            return basicResponse(req->rid(), req->cid(), ret);
        }
        void deleteExchange(const deleteExchangeRequstptr &req)
        {
            bool ret = _host->deleteExchange(req->exchange_name());
            if (ret == false)
            {
                DBG_LOG("交换机%s删除失败!", req->exchange_name().c_str());
            }
            else
            {
                DBG_LOG("交换机%s删除成功!", req->exchange_name().c_str());
            }
            return basicResponse(req->rid(), req->cid(), ret);
        }
        // 队列的声明与删除
        void declareQueue(const declareQueueRequstptr &req)
        {
            bool ret = _host->declareQueue(req->queue_name(), req->durable(), req->exclusive(), req->auto_delete(), req->args());
            if (ret == false)
            {
                DBG_LOG("队列%s声明失败!", req->queue_name().c_str());
                return basicResponse(req->rid(), req->cid(), false);
            }
            else
            {
                DBG_LOG("队列%s声明成功!", req->queue_name().c_str());
                _cmptr->initQueueConsumer(req->queue_name());
            }
            return basicResponse(req->rid(), req->cid(), ret);
        }
        void deleteQueue(const deleteQueueRequestptr &req)
        {
            bool ret = _host->deleteQueue(req->queue_name());
            if (ret == false)
            {
                DBG_LOG("队列%s删除失败!", req->queue_name().c_str());
            }
            else
            {
                _cmptr->destroyQueueConsumer(req->queue_name());
                DBG_LOG("队列%s删除成功!", req->queue_name().c_str());
            }
            return basicResponse(req->rid(), req->cid(), ret);
        }
        // 队列的绑定与解除绑定
        void queueBind(const queueBindRequestptr &req)
        {
            bool ret = _host->bind(req->exchange_name(), req->queue_name(), req->binding_key());
            if (ret == false)
            {
                DBG_LOG("交换机%s与队列%s绑定失败!", req->exchange_name().c_str(), req->queue_name().c_str());
            }
            else
            {
                DBG_LOG("交换机%s与队列%s绑定成功!", req->exchange_name().c_str(), req->queue_name().c_str());
            }
            return basicResponse(req->rid(), req->cid(), ret);
        }
        void queueUnbind(const queueUnbindRequestptr &req)
        {
            _host->unBind(req->exchange_name(), req->queue_name());
            DBG_LOG("交换机%s与队列%s解除绑定成功!", req->exchange_name().c_str(), req->queue_name().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }

        // 消息的发布
        void basicPublish(const basicPublishRequestptr &req)
        {
            // 判断交换机是否存在
            Exchange::ptr eptr = _host->selectExchange(req->exchange_name());
            if (eptr.get() == nullptr)
            {
                DBG_LOG("发布消息时交换机%s不存在!", req->exchange_name().c_str());
                return basicResponse(req->rid(), req->cid(), false);
            }
            // 获取交换机对应的绑定信息
            MsgQueueBindingMap mqbmap = _host->exchangeBindings(req->exchange_name());
            // 进行路由交换,根据绑定信息将消息添加到队列中
            BasecProperties *properties = nullptr;
            std::string roting_key;
            if (req->has_properties())
            {
                properties = req->mutable_properties();
                roting_key = req->properties().roting_key();
            }
            for (auto &mqb : mqbmap)
            {
                //打印交换机格式
                
                if (router::route(eptr->type, mqb.second->binding_key, roting_key))
                {
                    // 将消息添加到队列中
                    DBG_LOG("路由交换成功,将消息%s添加到队列%s中!", req->body().c_str(), mqb.first.c_str());
                    _host->basicPublish(mqb.first, properties, req->body());
                    // 向线程池中添加一个发布消息的任务(向指定队列的订阅者推送消息)
                    _pool->push([this, mqb]()
                                { consume(mqb.first); });
                }
            }
            return basicResponse(req->rid(), req->cid(), true);
        }
        // 消息的确认
        void basicAck(const basicAckRequestptr &req)
        {
            _host->basicAck(req->queue_name(), req->message_id());
            DBG_LOG("消息%s确认成功!", req->message_id().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }
        // 订阅队列消息
        void basicConsume(const basicConsumeRequestptr &req)
        {
            // 判断队列是否存在
            bool ret = _host->existsQueue(req->queue_name());
            if (ret == false)
            {
                DBG_LOG("订阅队列%s失败,队列不存在!", req->queue_name().c_str());
                return basicResponse(req->rid(), req->cid(), false);
            }
            // 创建消费者
            _consumer = _cmptr->create(req->consumer_tag(), req->queue_name(), req->auto_ack(),
                                       [this](const std::string &tag, const BasecProperties *bp, const std::string &body)
                                       { callback(tag, bp, body); });
            DBG_LOG("订阅队列%s成功!", req->queue_name().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }
        // 取消订阅
        void basicCancel(const basicCancelRequestptr &req)
        {
            _cmptr->remove(req->consumer_tag(), req->queue_name());
            _consumer = nullptr;
            DBG_LOG("取消订阅队列%s成功!", req->queue_name().c_str());
            return basicResponse(req->rid(), req->cid(), true);
        }

    private:
        // 消费者的回调函数,功能是构建消息格式并发送给对应的消费者(就是自己)
        // 设置回调函数时会将信道的this指针绑定到该函数上,以便于调用时能够找到自己的连接并成功发送
        // 发布一条消息的时候会调用这个函数,将消息的参数发给消费者对象,并通过this指针发送给自己
        void callback(const std::string &tag, const BasecProperties *bp, const std::string &body)
        {
            // 构建推送消息格式
            basicConsumeResponse resp;
            resp.set_cid(_cid);
            resp.set_consumer_tag(tag);
            if (bp != nullptr)
            {
                resp.mutable_properties()->set_id(bp->id());
                resp.mutable_properties()->set_deliver_mode(bp->deliver_mode());
                resp.mutable_properties()->set_roting_key(bp->roting_key());
            }
            resp.set_body(body);
            // 发送给对应的消费者
            DBG_LOG("向消费者%s推送消息!", tag.c_str());
            _codecptr->send(_connptr, resp);
        }
        // 向订阅者发布一条信息,由线程池执行
        void consume(const std::string &queue_name)
        {
            // 从指定队列获取一条信息
            MessageMapper::MessagePtr mptr = _host->basicConsume(queue_name);
            if (mptr.get() == nullptr)
            {
                DBG_LOG("执行消费任务失败,%s队列中没有消息!", queue_name.c_str());
                return;
            }
            // 从指定队列获取一位订阅者
            Consumer::ptr cptr = _cmptr->choose(queue_name);
            if (cptr.get() == nullptr)
            {
                DBG_LOG("执行消费任务失败,%s队列中没有消费者!", queue_name.c_str());
                return;
            }
            // 调用订阅者的回调函数,实现消息的推送
            cptr->callback(cptr->tag, mptr->mutable_payload()->mutable_properties(), mptr->payload().body());
            // 如果是自动确认则不需要等待,否则需要外部收到消息确认后删除
            if (cptr->auto_ack)
                _host->basicAck(queue_name, mptr->payload().properties().id());
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
        std::string _cid;                      // 信道的id
        Consumer::ptr _consumer;               // 关联的消费者句柄
        muduo::net::TcpConnectionPtr _connptr; // 连接的操作句柄
        ProtobufCodecPtr _codecptr;            // 协议管理器句柄
        ConsumerManager::ptr _cmptr;           // 消费者管理句柄
        VirtualHost::ptr _host;                // 虚拟机的管理句柄
        Threadpool::ptr _pool;                 // 线程池的管理句柄
    };

    class ChannelManager
    {
    public:
        using ptr = std::shared_ptr<ChannelManager>;
        ChannelManager() {}
        bool openChannel(const std::string &id,
                         const muduo::net::TcpConnectionPtr &connptr,
                         const ProtobufCodecPtr &codecptr,
                         const ConsumerManager::ptr &cmptr,
                         const VirtualHost::ptr &host,
                         const Threadpool::ptr &pool)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(id);
            if (it != _channels.end())
            {
                DBG_LOG("信道%s已经存在!", id.c_str());
                return false;
            }
            Channel::ptr cptr = std::make_shared<Channel>(id, connptr, codecptr, cmptr, host, pool);
            _channels.insert(std::make_pair(id, cptr));
            return true;
        }
        void closeChannel(const std::string &id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _channels.erase(id);
        }
        Channel::ptr getChannel(const std::string &id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(id);
            if (it == _channels.end())
            {
                DBG_LOG("信道%s不存在!", id.c_str());
                return Channel::ptr();
            }
            return it->second;
        }

    private:
        std::mutex _mutex;
        std::unordered_map<std::string, Channel::ptr> _channels;
    };
}

#endif