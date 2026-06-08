#include "connection.hpp"
// 参数是消费者标识,信息属性,信息主体
void callback(mq::Channel::ptr cptr, const std::string &consumer_tag, const mq::BasecProperties *bp, const std::string &body)
{
    DBG_LOG("消费者%s收到消息: %s", consumer_tag.c_str(), body.c_str());
    // 消息处理完成后,向服务器发送ack
    cptr->basicAck(bp->id());
}
int main()
{
    // 实例化异步工作线程对象
    mq::AsyncWorker::ptr worker_ptr = std::make_shared<mq::AsyncWorker>();
    // 实例化连接对象
    mq::client consumer_clint("127.0.0.1", 8888, worker_ptr);
    // 通过连接创建信道
    mq::Channel::ptr c1 = consumer_clint.openChannel();
    mq::Channel::ptr c2 = consumer_clint.openChannel();
    google::protobuf::Map<std::string, std::string> empty_map;
    c1->declareExchange("exchange1", mq::ExchangeType::TOPIC, mq::DURABLE, false, empty_map);
    // 声明一个队列queue1
    c1->declareQueue("queue1", true, true, false, empty_map);
    // 声明一个队列queue2
    c1->declareQueue("queue2", true, true, false, empty_map);
    // 绑定queue1-exchange1,且binding_key设置为queue1
    c1->queueBind("exchange1", "queue1", "queue1");
    // 绑定queue2-exchange1,且binding_key设置为news.music.#
    c1->queueBind("exchange1", "queue2", "news.music.#");

    // c1订阅队列queue1
    c1->basicConsume("consumer1", "queue1", false, [c1](const std::string &consumer_tag, const mq::BasecProperties *bp, const std::string &body)
                     { callback(c1, consumer_tag, bp, body); });
    // c2订阅队列queue2
    c2->basicConsume("consumer2", "queue2", false, [c2](const std::string &consumer_tag, const mq::BasecProperties *bp, const std::string &body)
                     { callback(c2, consumer_tag, bp, body); });

    while (1)
        std::this_thread::sleep_for(std::chrono::seconds(3));
    consumer_clint.closeChannel(c1);
    consumer_clint.closeChannel(c2);
    return 0;
}