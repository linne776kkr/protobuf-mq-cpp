#include "../mqserver/virtualhost.hpp"
#include <gtest/gtest.h>

class HostTest : public testing::Test
{
public:
    virtual void SetUp() override
    {
        hptr = std::make_shared<mq::VirtualHost>("虚拟机", "./data/meta.db", "./data/message");
        std::unordered_map<std::string, std::string> map;
        hptr->declareExchange("exchange1", mq::ExchangeType::DIRECT, mq::DeliveryMode::DURABLE, false, map);
        hptr->declareExchange("exchange2", mq::ExchangeType::DIRECT, mq::DeliveryMode::DURABLE, false, map);
        hptr->declareExchange("exchange3", mq::ExchangeType::DIRECT, mq::DeliveryMode::DURABLE, false, map);

        hptr->declareQueue("queue1", true, false, false, map);
        hptr->declareQueue("queue2", true, false, false, map);
        hptr->declareQueue("queue3", true, false, false, map);

        hptr->bind("exchange1", "queue1", "news.music.#");
        hptr->bind("exchange1", "queue2", "news.music.#");
        hptr->bind("exchange1", "queue3", "news.music.#");

        hptr->bind("exchange2", "queue1", "news.music.#");
        hptr->bind("exchange2", "queue2", "news.music.#");
        hptr->bind("exchange2", "queue3", "news.music.#");

        hptr->bind("exchange3", "queue1", "news.music.#");
        hptr->bind("exchange3", "queue2", "news.music.#");
        hptr->bind("exchange3", "queue3", "news.music.#");

        hptr->basicPublish("queue1", nullptr, "伊雷娜1");
        hptr->basicPublish("queue1", nullptr, "伊雷娜2");
        hptr->basicPublish("queue1", nullptr, "伊雷娜3");

        hptr->basicPublish("queue2", nullptr, "伊雷娜1");
        hptr->basicPublish("queue2", nullptr, "伊雷娜2");
        hptr->basicPublish("queue2", nullptr, "伊雷娜3");

        hptr->basicPublish("queue3", nullptr, "伊雷娜1");
        hptr->basicPublish("queue3", nullptr, "伊雷娜2");
        hptr->basicPublish("queue3", nullptr, "伊雷娜3");
    }
    virtual void TearDown() override
    {
        hptr->clear();
    }

public:
    mq::VirtualHost::ptr hptr;
};

TEST_F(HostTest, init_test)
{
    ASSERT_EQ(hptr->existsExchange("exchange1"), true);
    ASSERT_EQ(hptr->existsExchange("exchange2"), true);
    ASSERT_EQ(hptr->existsExchange("exchange3"), true);

    ASSERT_EQ(hptr->existsQueue("queue1"), true);
    ASSERT_EQ(hptr->existsQueue("queue2"), true);
    ASSERT_EQ(hptr->existsQueue("queue3"), true);

    ASSERT_EQ(hptr->existsBinding("exchange1", "queue1"), true);
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue2"), true);
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue3"), true);

    ASSERT_EQ(hptr->existsBinding("exchange2", "queue1"), true);
    ASSERT_EQ(hptr->existsBinding("exchange2", "queue2"), true);
    ASSERT_EQ(hptr->existsBinding("exchange2", "queue3"), true);

    ASSERT_EQ(hptr->existsBinding("exchange3", "queue1"), true);
    ASSERT_EQ(hptr->existsBinding("exchange3", "queue2"), true);
    ASSERT_EQ(hptr->existsBinding("exchange3", "queue3"), true);

    mq::MessageMapper::MessagePtr msgptr1 = hptr->basicConsume("queue1");
    ASSERT_EQ(msgptr1->payload().body(), "伊雷娜1");
    hptr->basicAck("queue1",msgptr1->payload().properties().id());

    mq::MessageMapper::MessagePtr msgptr2 = hptr->basicConsume("queue1");
    ASSERT_EQ(msgptr2->payload().body(), "伊雷娜2");
    hptr->basicAck("queue1",msgptr2->payload().properties().id());

    mq::MessageMapper::MessagePtr msgptr3 = hptr->basicConsume("queue1");
    ASSERT_EQ(msgptr3->payload().body(), "伊雷娜3");
    hptr->basicAck("queue1",msgptr3->payload().properties().id());

    mq::MessageMapper::MessagePtr msgptr4 = hptr->basicConsume("queue1");
    ASSERT_EQ(msgptr4,nullptr);
}

TEST_F(HostTest,remove_exchange){
    hptr->deleteExchange("exchange1");
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue1"), false);
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue2"), false);
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue3"), false);
}

TEST_F(HostTest,remove_queue){
    hptr->deleteQueue("queue1");
    ASSERT_EQ(hptr->existsBinding("exchange1", "queue1"), false);
    ASSERT_EQ(hptr->existsBinding("exchange2", "queue1"), false);
    ASSERT_EQ(hptr->existsBinding("exchange3", "queue1"), false);
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
    
}