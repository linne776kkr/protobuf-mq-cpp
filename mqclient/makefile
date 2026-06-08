all:publish_client consumer_client
publish_client:publish_client.cc ../third/include/muduo/proto/codec.cc ../mqcommon/msg.pb.cc ../mqcommon/proto.pb.cc
	g++ -std=c++11 $^ -o $@ -I ../third/include -L ../third/lib -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz
consumer_client:consumer_client.cc ../third/include/muduo/proto/codec.cc ../mqcommon/msg.pb.cc ../mqcommon/proto.pb.cc
	g++ -std=c++11 $^ -o $@ -I ../third/include -L ../third/lib -lmuduo_net -lmuduo_base -pthread -lprotobuf -lz
.PHONY:clean
clean:
	rm publish_client consumer_client