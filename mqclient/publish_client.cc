#include "connection.hpp"
int main()
{
    // 实例化异步工作线程对象
    mq::AsyncWorker::ptr worker_ptr = std::make_shared<mq::AsyncWorker>();
    // 实例化连接对象
    mq::client publish_clint("127.0.0.1", 8888, worker_ptr);
    // 通过连接创建信道
    mq::Channel::ptr c1 = publish_clint.openChannel();
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
    // 循环向交换机发布消息
    for (int i = 1; i <= 10; i++)
    {
        mq::BasecProperties bp;
        bp.set_id(mq::UUIDHelper::uuid());
        bp.set_deliver_mode(mq::DURABLE);
        bp.set_roting_key("news.music");
        c1->basicPublish("exchange1", &bp, "伊蕾" + std::to_string(i));
        sleep(1);
    }
    // 关闭信道
    publish_clint.closeChannel(c1);
    // 关闭连接
    return 0;
}