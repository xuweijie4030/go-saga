package rpc

import (
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/rpcxclient"
	"time"
)

func getOptions(basePath string, serverName string, timeout time.Duration) *rpcxclient.Options {
	options := rpcxclient.DefaultOptions
	options.RegistryOption.BasePath = basePath
	options.RegistryOption.ServerName = serverName
	options.RegistryOption.Addr = config.GetStringSlice("Registry.Addr")
	options.RegistryOption.Group = config.GetString("Registry.Group")
	options.Timeout = timeout * time.Second

	return options
}
