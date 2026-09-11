package metrics

import (
	"sync"
)

type ConnectionOpenInfo struct {
	ProxyName  string
	ProxyType  string
	RemoteAddr string
	LocalAddr  string
}

type ServerMetrics interface {
	NewClient()
	CloseClient()
	NewProxy(name string, proxyType string, user string, clientID string)
	CloseProxy(name string, proxyType string)
	OpenConnection(name string, proxyType string)
	CloseConnection(name string, proxyType string)
	AddTrafficIn(name string, proxyType string, trafficBytes int64)
	AddTrafficOut(name string, proxyType string, trafficBytes int64)
}

type DetailedServerMetrics interface {
	OpenConnectionWithInfo(info ConnectionOpenInfo) uint64
	CloseConnectionWithInfo(id uint64, name string, proxyType string, trafficIn int64, trafficOut int64)
}

var Server ServerMetrics = noopServerMetrics{}

var registerMetrics sync.Once

func Register(m ServerMetrics) {
	registerMetrics.Do(func() {
		Server = m
	})
}

type noopServerMetrics struct{}

func (noopServerMetrics) NewClient()                              {}
func (noopServerMetrics) CloseClient()                            {}
func (noopServerMetrics) NewProxy(string, string, string, string) {}
func (noopServerMetrics) CloseProxy(string, string)               {}
func (noopServerMetrics) OpenConnection(string, string)           {}
func (noopServerMetrics) CloseConnection(string, string)          {}
func (noopServerMetrics) AddTrafficIn(string, string, int64)      {}
func (noopServerMetrics) AddTrafficOut(string, string, int64)     {}

func OpenConnectionWithInfo(info ConnectionOpenInfo) uint64 {
	if detailed, ok := Server.(DetailedServerMetrics); ok {
		return detailed.OpenConnectionWithInfo(info)
	}
	Server.OpenConnection(info.ProxyName, info.ProxyType)
	return 0
}

func CloseConnectionWithInfo(id uint64, name string, proxyType string, trafficIn int64, trafficOut int64) {
	if detailed, ok := Server.(DetailedServerMetrics); ok {
		detailed.CloseConnectionWithInfo(id, name, proxyType, trafficIn, trafficOut)
		return
	}
	Server.CloseConnection(name, proxyType)
}
