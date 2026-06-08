mqbrokertest:mqbrokertest.cpp ../mqcommon/msg.pb.cc ../mqcommon/proto.pb.cc 
	g++ -o $@ $^ -std=c++11 -lgtest -lprotobuf -lsqlite3 -lmuduo_net -I../third/include -L ../lib
.PHONY:clean
clean:
	rm mqbrokertest