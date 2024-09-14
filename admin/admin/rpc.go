package admin

import (
	"context"
	"github.com/cloudwego/kitex/client"
	"github.com/cylScripter/apiopen/admin"
)

func GetUserList(ctx context.Context, req *admin.GetUserReq, callOptions ...client.Option) (resp *admin.GetUserResp, err error) {
	clients := MustNewClient("admin", callOptions...)
	return clients.GetUserList(ctx, req)
}
