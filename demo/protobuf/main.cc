#include<iostream>
#include"contacts.pb.h"
int main()
{
    contacts::contact con;
    con.set_sn(241203606);
    con.set_name("伊雷娜");
    con.set_score(58.58);
    std::string str;
    //将序列化后的数据存储在str中
    con.SerializeToString(&str);
    std::cout<<str<<std::endl;

    contacts::contact recon;
    bool ret = recon.ParseFromString(str);
    
    if(ret==0)std::cout<<"反序列化失败"<<std::endl;
    else 
    {
        std::cout<<recon.name()<<std::endl;
        std::cout<<recon.sn()<<std::endl;
        std::cout<<recon.score()<<std::endl;
    }
    return 0;
}