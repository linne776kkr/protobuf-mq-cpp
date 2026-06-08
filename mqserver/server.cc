#include"../mqserver/broker.hpp"

int main(){
    mq::server mq_server(8888,"./data");
    mq_server.start();
}