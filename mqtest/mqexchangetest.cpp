#include "../mqserver/exchange.hpp"
#include<gtest/gtest.h>

mq::ExchangeManager::ptr exmptr;

class ExchangeTest : public testing::Environment{
    public:
        virtual void SetUp() override{
            exmptr = std::make_shared<mq::ExchangeManager>("./data/meta.db");
        }
        virtual void TearDown() override{
            //exmptr->clear();
            std::cout<<"假装清理了,并没有删除表和表中数据"<<std::endl;
        }
};

TEST(exchange_test,insert_test){
    //std::unordered_map<std::string,std::string> map = {{"k1","v1"},{"k2","v2"}};
    std::unordered_map<std::string,std::string> map;
    exmptr->declareExchange("exchange1",mq::ExchangeType::DIRECT,mq::DeliveryMode::DURABLE,false,map);
    exmptr->declareExchange("exchange2",mq::ExchangeType::DIRECT,mq::DeliveryMode::DURABLE,false,map);
    exmptr->declareExchange("exchange3",mq::ExchangeType::DIRECT,mq::DeliveryMode::DURABLE,false,map);
    exmptr->declareExchange("exchange4",mq::ExchangeType::DIRECT,mq::DeliveryMode::DURABLE,false,map);
    ASSERT_EQ(exmptr->size(),4);
}

TEST(exchange_test,select_test){
    mq::Exchange::ptr exptr = exmptr->selectExchange("exchange3");
    ASSERT_EQ(exptr->name,"exchange3");
    ASSERT_EQ(exptr->type,mq::ExchangeType::DIRECT);
    ASSERT_EQ(exptr->durable,mq::DeliveryMode::DURABLE);
    ASSERT_EQ(exptr->auto_delete,false);
    ASSERT_EQ(exptr->getArgs(),std::string("k1=v1&k2=v2&"));
}

TEST(exchange_test,remove_test){
    exmptr->deleteExchange("exchange3");
    mq::Exchange::ptr exptr = exmptr->selectExchange("exchange3");
    ASSERT_EQ(exptr,nullptr);
    ASSERT_EQ(exmptr->exists("exchange3"),false);
}

TEST(exchange_test,recover_test){
    mq::Exchange::ptr exptr1 = exmptr->selectExchange("exchange1");
    mq::Exchange::ptr exptr2 = exmptr->selectExchange("exchange2");
    mq::Exchange::ptr exptr3 = exmptr->selectExchange("exchange3");
    mq::Exchange::ptr exptr4 = exmptr->selectExchange("exchange4");
    ASSERT_NE(exptr1,nullptr);
    ASSERT_NE(exptr2,nullptr);
    ASSERT_EQ(exptr3,nullptr);
    ASSERT_NE(exptr4,nullptr);
}

int main(int argc,char* argv[])
{
    testing::InitGoogleTest(&argc,argv);
    testing::AddGlobalTestEnvironment(new ExchangeTest);
    return RUN_ALL_TESTS();
}