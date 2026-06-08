#include "../mqserver/message.hpp"
#include <gtest/gtest.h>

mq::MessageManager::ptr mmptr;

class BindingTest : public testing::Environment
{
public:
    virtual void SetUp() override
    {
        mmptr = std::make_shared<mq::MessageManager>("./data/message");
        mq::FileHelper a("data/message/queue1.mqd");
        mmptr->initQueueMessage("queue1");
        mmptr->initQueueMessage("queue2");
        mmptr->initQueueMessage("queue3");
    }
    virtual void TearDown() override
    {
        mmptr->clear();
    }
};
TEST(message_test, insert_test)
{
    mq::BasecProperties properties;
    properties.set_id(mq::UUIDHelper::uuid());
    properties.set_deliver_mode(mq::DURABLE);
    properties.set_roting_key("news.music.pop");
    mmptr->insert("queue1", &properties, "伊雷娜", mq::DURABLE);
    mmptr->insert("queue2", nullptr, "伊雷", mq::DURABLE);
    mmptr->insert("queue2", nullptr, "娜", mq::DURABLE);
    mmptr->insert("queue3", &properties, "伊", mq::DURABLE);
    mmptr->insert("queue3", nullptr, "雷", mq::DURABLE);
    mmptr->insert("queue3", nullptr, "娜", mq::UNDURABLE);
    ASSERT_EQ(mmptr->push_count("queue1"), 1);
    ASSERT_EQ(mmptr->total_count("queue1"), 1);
    ASSERT_EQ(mmptr->durable_count("queue1"), 1);
    ASSERT_EQ(mmptr->waitack_count("queue1"), 0);
    ASSERT_EQ(mmptr->push_count("queue2"), 2);
    ASSERT_EQ(mmptr->total_count("queue2"), 2);
    ASSERT_EQ(mmptr->durable_count("queue2"), 2);
    ASSERT_EQ(mmptr->waitack_count("queue2"), 0);
    ASSERT_EQ(mmptr->push_count("queue3"), 3);
    ASSERT_EQ(mmptr->total_count("queue3"), 2);
    ASSERT_EQ(mmptr->durable_count("queue3"), 2);
    ASSERT_EQ(mmptr->waitack_count("queue3"), 0);
}

TEST(message_test, select_test){
    mq::MessageMapper::MessagePtr mptr;
    mptr = mmptr->front("queue1");
    ASSERT_EQ(mptr->payload().properties().deliver_mode(),mq::DURABLE);
    ASSERT_EQ(mptr->payload().properties().roting_key(),"news.music.pop");
    ASSERT_EQ(mptr->payload().body(),"伊雷娜");
    ASSERT_EQ(mptr->payload().valid(),"1");
    mptr = mmptr->front("queue2");
    ASSERT_EQ(mptr->payload().properties().deliver_mode(),mq::DURABLE);
    ASSERT_EQ(mptr->payload().properties().roting_key(),"");
    ASSERT_EQ(mptr->payload().body(),"伊雷");
    ASSERT_EQ(mptr->payload().valid(),"1");
    mptr = mmptr->front("queue2");
    ASSERT_EQ(mptr->payload().properties().deliver_mode(),mq::DURABLE);
    ASSERT_EQ(mptr->payload().properties().roting_key(),"");
    ASSERT_EQ(mptr->payload().body(),"娜");
    ASSERT_EQ(mptr->payload().valid(),"1");
    mptr = mmptr->front("queue3");
    ASSERT_EQ(mptr->payload().properties().deliver_mode(),mq::DURABLE);
    ASSERT_EQ(mptr->payload().properties().roting_key(),"news.music.pop");
    ASSERT_EQ(mptr->payload().body(),"伊");
    ASSERT_EQ(mptr->payload().valid(),"1");
    mptr = mmptr->front("queue3");
    ASSERT_EQ(mptr->payload().properties().deliver_mode(),mq::DURABLE);
    ASSERT_EQ(mptr->payload().properties().roting_key(),"");
    ASSERT_EQ(mptr->payload().body(),"雷");
    ASSERT_EQ(mptr->payload().valid(),"1");

    ASSERT_EQ(mmptr->push_count("queue1"), 0);
    ASSERT_EQ(mmptr->total_count("queue1"), 1);
    ASSERT_EQ(mmptr->durable_count("queue1"), 1);
    ASSERT_EQ(mmptr->waitack_count("queue1"), 1);
    ASSERT_EQ(mmptr->push_count("queue2"), 0);
    ASSERT_EQ(mmptr->total_count("queue2"), 2);
    ASSERT_EQ(mmptr->durable_count("queue2"), 2);
    ASSERT_EQ(mmptr->waitack_count("queue2"), 2);
    ASSERT_EQ(mmptr->push_count("queue3"), 1);
    ASSERT_EQ(mmptr->total_count("queue3"), 2);
    ASSERT_EQ(mmptr->durable_count("queue3"), 2);
    ASSERT_EQ(mmptr->waitack_count("queue3"), 2);

    mmptr->ack("queue3",mptr->payload().properties().id());
    ASSERT_EQ(mmptr->push_count("queue3"), 1);
    ASSERT_EQ(mmptr->total_count("queue3"), 2);
    ASSERT_EQ(mmptr->durable_count("queue3"), 1);
    ASSERT_EQ(mmptr->waitack_count("queue3"), 1);
}

TEST(message_test, delete_test){
    mmptr->destroyQueueMessage("queue2");
    ASSERT_EQ(mmptr->push_count("queue2"),0);
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new BindingTest);
    return RUN_ALL_TESTS();
}