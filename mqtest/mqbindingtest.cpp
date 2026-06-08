#include "../mqserver/binding.hpp"
#include<gtest/gtest.h>

mq::BindingManager::ptr bmptr;

class BindingTest : public testing::Environment{
    public:
        virtual void SetUp() override{
            bmptr = std::make_shared<mq::BindingManager>("./data/meta.db");
        }
        virtual void TearDown() override{
            //bmptr->clear();
            std::cout<<"假装清理了,并没有删除表和表中数据"<<std::endl;
        }
};

TEST(binding_test,recover_test){
    ASSERT_EQ(bmptr->size(),1);
}

TEST(binding_test,insert_test){
    bmptr->bind("exchange1","queue1","key1",true);
    bmptr->bind("exchange1","queue1","key1",true);
    bmptr->bind("exchange1","queue2","key2",true);
    bmptr->bind("exchange2","queue3","key3",true);
    bmptr->bind("exchange2","queue4","key4",true);
    bmptr->bind("exchange3","queue1","key5",true);
    bmptr->bind("exchange3","queue2","key6",true);
    ASSERT_EQ(bmptr->size(),6);
}

TEST(binding_test,select_test){
    mq::MsgQueueBindingMap mqmap = bmptr->getExchangeBindings("exchange1");
    ASSERT_EQ(mqmap.size(),2);
    mq::Binding::ptr bptr = bmptr->getBinding("exchange2","queue3");
    ASSERT_EQ(bptr->binding_key,"key3");
}

TEST(binding_test,remove_test){
    bmptr->unBind("exchange1","queue1");
    ASSERT_EQ(bmptr->size(),5);
    bmptr->removeExchangeBindings("exchange2");
    ASSERT_EQ(bmptr->size(),3);
    bmptr->removeMsgQueueBindings("queue2");
    ASSERT_EQ(bmptr->size(),1);
    std::cout<<bmptr->size();
}



int main(int argc,char* argv[])
{
    testing::InitGoogleTest(&argc,argv);
    testing::AddGlobalTestEnvironment(new BindingTest);
    return RUN_ALL_TESTS();
}