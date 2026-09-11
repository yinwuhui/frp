// Copyright 2017 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mem

import (
	"time"

	"github.com/fatedier/frp/pkg/util/metric"
)

const (
	ReserveDays            = 7
	ConnectionHistoryLimit = 10000
	ConnectionStatusActive = "active"
	ConnectionStatusClosed = "closed"
)

type ServerStats struct {
	TotalTrafficIn  int64
	TotalTrafficOut int64
	CurConns        int64
	ClientCounts    int64
	ProxyTypeCounts map[string]int64
}

type ProxyStats struct {
	Name            string
	Type            string
	User            string
	ClientID        string
	TodayTrafficIn  int64
	TodayTrafficOut int64
	LastStartTime   string
	LastCloseTime   string
	LastStartAt     int64
	LastCloseAt     int64
	CurConns        int64
}

type ProxyTrafficInfo struct {
	Name       string
	TrafficIn  []int64
	TrafficOut []int64
}

type ConnectionInfo struct {
	ID             uint64
	ProxyName      string
	ProxyType      string
	User           string
	ClientID       string
	RemoteAddr     string
	RemoteIP       string
	RemotePort     string
	LocalAddr      string
	LocalIP        string
	LocalPort      string
	Status         string
	ConnectedAt    int64
	DisconnectedAt int64
	Duration       int64
	TrafficIn      int64
	TrafficOut     int64
}

type ConnectionStatistics struct {
	ID             uint64
	ProxyName      string
	ProxyType      string
	RemoteAddr     string
	RemoteIP       string
	RemotePort     string
	LocalAddr      string
	LocalIP        string
	LocalPort      string
	ConnectedAt    time.Time
	DisconnectedAt time.Time
	TrafficIn      int64
	TrafficOut     int64
}

type ProxyStatistics struct {
	Name          string
	ProxyType     string
	User          string
	ClientID      string
	TrafficIn     metric.DateCounter
	TrafficOut    metric.DateCounter
	CurConns      metric.Counter
	LastStartTime time.Time
	LastCloseTime time.Time
}

type ServerStatistics struct {
	TotalTrafficIn  metric.DateCounter
	TotalTrafficOut metric.DateCounter
	CurConns        metric.Counter

	// counter for clients
	ClientCounts metric.Counter

	// counter for proxy types
	ProxyTypeCounts map[string]metric.Counter

	// statistics for different proxies
	// key is proxy name
	ProxyStatistics map[string]*ProxyStatistics

	NextConnectionID  uint64
	ActiveConns       map[uint64]*ConnectionStatistics
	ConnectionHistory []*ConnectionStatistics
}

type Collector interface {
	GetServer() *ServerStats
	GetProxiesByType(proxyType string) []*ProxyStats
	GetProxiesByTypeAndName(proxyType string, proxyName string) *ProxyStats
	GetProxyByName(proxyName string) *ProxyStats
	GetProxyTraffic(name string) *ProxyTrafficInfo
	GetProxyConnections(name string, status string, page int, pageSize int) ([]*ConnectionInfo, int)
	ClearOfflineProxies() (int, int)
	PruneOfflineProxies() (int, int)
}
