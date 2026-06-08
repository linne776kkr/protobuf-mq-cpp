#include<iostream>
#include<gtest/gtest.h>

TEST(test,great_than){
    int a = 20;
    ASSERT_GT(a,18);
    std::cout<<"ok"<<std::endl;
}

int main(int argc,char* argv[])
{   
    testing::InitGoogleTest(&argc,argv);
    RUN_ALL_TESTS();
    return 0;
}