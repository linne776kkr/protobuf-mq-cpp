#include "../mqserver/consumer.hpp"
#include <gtest/gtest.h>

mq::ConsumerManager::ptr cmptr;

class ConsumerTest : public testing::Environment
{
public:
    virtual void SetUp() override
    {
        cmptr = std::make_shared<mq::ConsumerManager>();
        cmptr->initQueueConsumer("queue1");
        cmptr->initQueueConsumer("queue2");
        cmptr->initQueueConsumer("queue3");
        cmptr->initQueueConsumer("queue4");
        cmptr->initQueueConsumer("queue5");
    }
    virtual void TearDown() override
    {
        cmptr->clear();
    }
};

TEST(ConsumerTest, create_test)
{
    cmptr->create("consumer1", "queue1", true, nullptr);
    cmptr->create("consumer2", "queue1", true, nullptr);
    cmptr->create("consumer3", "queue2", true, nullptr);
    cmptr->create("consumer4", "queue2", true, nullptr);
    cmptr->create("consumer5", "queue3", true, nullptr);
    cmptr->create("consumer6", "queue3", true, nullptr);
    cmptr->create("consumer7", "queue4", true, nullptr);
    cmptr->create("consumer8", "queue4", true, nullptr);
    cmptr->create("consumer9", "queue5", true, nullptr);
    cmptr->create("consumer10", "queue5", true, nullptr);

    ASSERT_EQ(cmptr->empty("queue1"), false);
    ASSERT_EQ(cmptr->empty("queue2"), false);
    ASSERT_EQ(cmptr->empty("queue3"), false);
    ASSERT_EQ(cmptr->empty("queue4"), false);
    ASSERT_EQ(cmptr->empty("queue5"), false);

    ASSERT_EQ(cmptr->exists("consumer1", "queue1"), true);
    ASSERT_EQ(cmptr->exists("consumer2", "queue1"), true);
    ASSERT_EQ(cmptr->exists("consumer3", "queue2"), true);
    ASSERT_EQ(cmptr->exists("consumer4", "queue2"), true);
    ASSERT_EQ(cmptr->exists("consumer5", "queue3"), true);
    ASSERT_EQ(cmptr->exists("consumer6", "queue3"), true);
    ASSERT_EQ(cmptr->exists("consumer7", "queue4"), true);
    ASSERT_EQ(cmptr->exists("consumer8", "queue4"), true);
    ASSERT_EQ(cmptr->exists("consumer9", "queue5"), true);
    ASSERT_EQ(cmptr->exists("consumer10", "queue5"), true);
}

TEST(ConsumerTest, choose_test)
{
    ASSERT_EQ(cmptr->choose("queue1")->tag, "consumer1");
    ASSERT_EQ(cmptr->choose("queue1")->tag, "consumer2");
    ASSERT_EQ(cmptr->choose("queue2")->tag, "consumer3");
    ASSERT_EQ(cmptr->choose("queue2")->tag, "consumer4");
    ASSERT_EQ(cmptr->choose("queue3")->tag, "consumer5");
    ASSERT_EQ(cmptr->choose("queue3")->tag, "consumer6");
    ASSERT_EQ(cmptr->choose("queue4")->tag, "consumer7");
    ASSERT_EQ(cmptr->choose("queue4")->tag, "consumer8");
    ASSERT_EQ(cmptr->choose("queue5")->tag, "consumer9");
    ASSERT_EQ(cmptr->choose("queue5")->tag, "consumer10");
}

TEST(ConsumerTest, remove_test)
{
    cmptr->remove("consumer1", "queue1");
    cmptr->remove("consumer3", "queue2");
    cmptr->remove("consumer5", "queue3");
    cmptr->remove("consumer7", "queue4");
    cmptr->remove("consumer9", "queue5");
    ASSERT_EQ(cmptr->exists("consumer1", "queue1"), false);
    ASSERT_EQ(cmptr->exists("consumer3", "queue2"), false);
    ASSERT_EQ(cmptr->exists("consumer5", "queue3"), false);
    ASSERT_EQ(cmptr->exists("consumer7", "queue4"), false);
    ASSERT_EQ(cmptr->exists("consumer9", "queue5"), false);
}

TEST(ConsumerTest, destroy_test)
{
    cmptr->destroyQueueConsumer("queue1");
    cmptr->destroyQueueConsumer("queue2");
    cmptr->destroyQueueConsumer("queue3");
    ASSERT_EQ(cmptr->empty("queue1"), false);
    ASSERT_EQ(cmptr->empty("queue2"), false);
    ASSERT_EQ(cmptr->empty("queue3"), false);
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ConsumerTest);
    return RUN_ALL_TESTS();
}