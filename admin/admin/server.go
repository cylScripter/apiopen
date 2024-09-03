// Code generated by Kitex v0.10.3. DO NOT EDIT.
package admin

import (
	server "github.com/cloudwego/kitex/server"
	admin "github.com/cylScripter/apiopen/admin"
)

// NewServer creates a server.Server with the given handler and options.
func NewServer(handler admin.Admin, opts ...server.Option) server.Server {
	var options []server.Option

	options = append(options, opts...)
	options = append(options, server.WithCompatibleMiddlewareForUnary())

	svr := server.NewServer(options...)
	if err := svr.RegisterService(serviceInfo(), handler); err != nil {
		panic(err)
	}
	return svr
}

func RegisterService(svr server.Server, handler admin.Admin, opts ...server.RegisterOption) error {
	return svr.RegisterService(serviceInfo(), handler, opts...)
}
