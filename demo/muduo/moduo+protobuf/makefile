all:server client
server:protobuf_server.cpp ../include/muduo/proto/codec.cc request.pb.cc
	g++ -std=c++11 $^ -o $@ -I ../include -L ../lib -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz
client:protobuf_client.cpp ../include/muduo/proto/codec.cc request.pb.cc
	g++ -std=c++11 $^ -o $@ -I ../include -L ../lib -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz
.PHONY:clean
clean:
	rm server client