#include"../mqcommon/helper.hpp"
#include<iostream>
#include <unistd.h>
int main()
{
    mq::FileHelper text("../mqcommon/logger.hpp");
    DBG_LOG("../mqcommon/logger.hpp是否存在: %d",text.exists());
    size_t sz = text.size();
    DBG_LOG("文件大小: %ld",sz);
    std::string body;
    DBG_LOG("文件读是否成功: %d",text.read(body));

    mq::FileHelper text2("./aaa/bbb/ccc/yi");
    if(!text2.exists())
    {
        std::string path = text2.parentDirectory("./aaa/bbb/ccc/yi");
        if(!mq::FileHelper(path).exists()){
            bool flag = mq::FileHelper::createDirectory(path);
            DBG_LOG("创建目录%s是否成功: %d",path.c_str(),flag);
            if(flag==true){
                flag = mq::FileHelper::createFile("./aaa/bbb/ccc/yi");
                DBG_LOG("创建文件%s是否成功: %d","./aaa/bbb/ccc/yi",flag);
                if(flag==true){
                    DBG_LOG("./aaa/bbb/ccc/yi是否存在: %d",text2.exists());
                    DBG_LOG("文件写是否成功: %d",text2.write(body));
                }
            }
        }
    }
    sleep(3);
    //改名
    DBG_LOG("将文件%s修改为%s是否成功: %d","yi","yi_lei_na",text2.rename("./aaa/bbb/ccc/yi_lei_na"));
    sleep(3);
    //删除文件
    DBG_LOG("删除文件yi_lei_na是否成功: %d",mq::FileHelper::removeFile("./aaa/bbb/ccc/yi_lei_na"));
    //删除文件夹
    DBG_LOG("删除文件夹是否成功: %d",mq::FileHelper::removeDirectory("./aaa"));
    return 0;
}