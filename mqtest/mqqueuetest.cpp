#include "../mqserver/queue.hpp"
#include<gtest/gtest.h>

mq::MsgQueueManager::ptr mqmptr;

class ExchangeTest : public testing::Environment{
    public:
        virtual void SetUp() override{
            mqmptr = std::make_shared<mq::MsgQueueManager>("./data/meta.db");
        }
        virtual void TearDown() override{
            //mqmptr->clear();
            std::cout<<"假装清理了,并没有删除表和表中数据"<<std::endl;
        }
};

TEST(MsgQueue_test,insert_test){
    //std::unordered_map<std::string,std::string> map = {{"k1","v1"},{"k2","v2"}};
    std::unordered_map<std::string,std::string> map = {{"k1","v1"}};
    mqmptr->declareQueue("Queue1",true,false,false,map);
    mqmptr->declareQueue("Queue2",true,false,false,map);
    mqmptr->declareQueue("Queue3",true,false,false,map);
    mqmptr->declareQueue("Queue4",true,false,false,map);
    ASSERT_EQ(mqmptr->size(),4);
}

TEST(MsgQueue_test,select_test){
    mq::MsgQueue::ptr mqptr = mqmptr->selectQueue("Queue3");
    ASSERT_EQ(mqptr->name,"Queue3");
    ASSERT_EQ(mqptr->durable,true);
    ASSERT_EQ(mqptr->exclusive,false);
    ASSERT_EQ(mqptr->auto_delete,false);
    ASSERT_EQ(mqptr->getArgs(),std::string("k1=v1&"));
}

TEST(MsgQueue_test,remove_test){
    mqmptr->deleteQueue("Queue3");
    mq::MsgQueue::ptr mqptr = mqmptr->selectQueue("Queue3");
    ASSERT_EQ(mqptr,nullptr);
    ASSERT_EQ(mqmptr->exists("Queue3"),false);
}

TEST(MsgQueue_test,recover_test){
    mq::MsgQueue::ptr mqptr1 = mqmptr->selectQueue("Queue1");
    mq::MsgQueue::ptr mqptr2 = mqmptr->selectQueue("Queue2");
    mq::MsgQueue::ptr mqptr3 = mqmptr->selectQueue("Queue3");
    mq::MsgQueue::ptr mqptr4 = mqmptr->selectQueue("Queue4");
    ASSERT_NE(mqptr1,nullptr);
    ASSERT_NE(mqptr2,nullptr);
    ASSERT_EQ(mqptr3,nullptr);
    ASSERT_NE(mqptr4,nullptr);
}

int main(int argc,char* argv[])
{
    testing::InitGoogleTest(&argc,argv);
    testing::AddGlobalTestEnvironment(new ExchangeTest);
    return RUN_ALL_TESTS();
}