package hello

import (
	"context"
	"github.com/cloudwego/kitex/client"
	"github.com/cylScripter/apiopen/hello"
)

func SayHello(ctx context.Context, req *hello.SayHelloReq, callOptions ...client.Option) (resp *hello.SayHelloResp, err error) {
	clients := MustNewClient("hello", callOptions...)
	return clients.SayHello(ctx, req)
}
