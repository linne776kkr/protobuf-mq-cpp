#include "../mqserver/route.hpp"
#include <gtest/gtest.h>

class router : public testing::Environment
{
public:
    virtual void SetUp() override
    {

    }
    virtual void TearDown() override
    {
        
    }
};

TEST(route_test, legal_routing_key) {
    std::string rkey1 = "news.music.pop";
    std::string rkey2 = "news..music.pop";
    std::string rkey3 = "news.,music.pop";
    std::string rkey4 = "news.music_123.pop";
    ASSERT_EQ(mq::router::isLegalRoutingKey(rkey1), true);
    ASSERT_EQ(mq::router::isLegalRoutingKey(rkey2), true);
    ASSERT_EQ(mq::router::isLegalRoutingKey(rkey3), false);
    ASSERT_EQ(mq::router::isLegalRoutingKey(rkey4), true);
}
TEST(route_test, legal_binding_key) {
    std::string bkey1 = "news.music.pop";
    std::string bkey2 = "news.#.music.pop";
    std::string bkey3 = "news.#.*.music.pop";
    std::string bkey4 = "news.*.#.music.pop";
    std::string bkey5 = "news.#.#.music.pop";
    std::string bkey6 = "news.*.*.music.pop";
    std::string bkey7 = "news.,music_123.pop";
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey1), true);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey2), true);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey3), false);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey4), false);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey5), false);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey6), true);
    ASSERT_EQ(mq::router::isLegalBindingKey(bkey7), false);
}

TEST(router, route)
{
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa", "aaa"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.bbb", "aaa.bbb"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.bbb", "aaa.bbb.ccc"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.bbb", "aaa.ccc"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.bbb", "aaa.bbb.ccc"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.bbb.#", "aaa.ccc.bbb"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "#.bbb.ccc", "aaa.bbb.ccc.ddd"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.bbb.ccc", "aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.*", "aaa.bbb"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.*.bbb", "aaa.bbb.ccc"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "*.aaa.bbb", "aaa.bbb"), false);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "#", "aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#", "aaa.bbb"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#", "aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.ccc", "aaa.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.ccc", "aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.ccc", "aaa.aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "#.ccc", "ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "#.ccc", "aaa.bbb.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.ccc.ccc", "aaa.bbb.ccc.ccc.ccc"), true);
    ASSERT_EQ(mq::router::route(mq::ExchangeType::TOPIC, "aaa.#.bbb.*.bbb", "aaa.ddd.ccc.bbb.eee.bbb"), true);
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new router);
    return RUN_ALL_TESTS();
}