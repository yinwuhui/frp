// Copyright 2019 fatedier, fatedier@gmail.com
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
	"net"
	"sort"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/fatedier/frp/pkg/util/log"
	"github.com/fatedier/frp/pkg/util/metric"
	server "github.com/fatedier/frp/server/metrics"
)

var (
	sm = newServerMetrics()

	ServerMetrics  server.ServerMetrics
	StatsCollector Collector
)

func init() {
	ServerMetrics = sm
	StatsCollector = sm
	sm.run()
}

type serverMetrics struct {
	info  *ServerStatistics
	clock clock.WithTicker
	mu    sync.Mutex
}

func newServerMetrics() *serverMetrics {
	return newServerMetricsWithClock(clock.RealClock{})
}

func newServerMetricsWithClock(clk clock.WithTicker) *serverMetrics {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &serverMetrics{
		clock: clk,
		info: &ServerStatistics{
			TotalTrafficIn:  metric.NewDateCounter(ReserveDays),
			TotalTrafficOut: metric.NewDateCounter(ReserveDays),
			CurConns:        metric.NewCounter(),

			ClientCounts:    metric.NewCounter(),
			ProxyTypeCounts: make(map[string]metric.Counter),

			ProxyStatistics: make(map[string]*ProxyStatistics),
			ActiveConns:     make(map[uint64]*ConnectionStatistics),
		},
	}
}

func (m *serverMetrics) run() {
	go m.runUntil(nil)
}

func (m *serverMetrics) runUntil(stopCh <-chan struct{}) {
	ticker := m.clock.NewTicker(12 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C():
			start := m.clock.Now()
			count, total := m.clearUselessInfo(time.Duration(7*24) * time.Hour)
			log.Debugf("clear useless proxy statistics data count %d/%d, cost %v", count, total, m.clock.Since(start))
		case <-stopCh:
			return
		}
	}
}

func (m *serverMetrics) clearUselessInfo(continuousOfflineDuration time.Duration) (int, int) {
	count := 0
	total := 0
	// To check if there are any proxies that have been closed for more than continuousOfflineDuration and remove them.
	m.mu.Lock()
	defer m.mu.Unlock()
	total = len(m.info.ProxyStatistics)
	for name, data := range m.info.ProxyStatistics {
		if m.shouldClearProxyStats(data, continuousOfflineDuration) {
			delete(m.info.ProxyStatistics, name)
			count++
			log.Tracef("clear proxy [%s]'s statistics data, lastCloseTime: [%s]", name, data.LastCloseTime.String())
		}
	}
	return count, total
}

func (m *serverMetrics) shouldClearProxyStats(data *ProxyStatistics, continuousOfflineDuration time.Duration) bool {
	return !data.LastCloseTime.IsZero() &&
		data.LastStartTime.Before(data.LastCloseTime) &&
		m.clock.Since(data.LastCloseTime) > continuousOfflineDuration
}

func (m *serverMetrics) ClearOfflineProxies() (int, int) {
	return m.clearUselessInfo(0)
}

func (m *serverMetrics) PruneOfflineProxies() (int, int) {
	return m.clearUselessInfo(0)
}

func (m *serverMetrics) NewClient() {
	m.info.ClientCounts.Inc(1)
}

func (m *serverMetrics) CloseClient() {
	m.info.ClientCounts.Dec(1)
}

func (m *serverMetrics) NewProxy(name string, proxyType string, user string, clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	counter, ok := m.info.ProxyTypeCounts[proxyType]
	if !ok {
		counter = metric.NewCounter()
	}
	counter.Inc(1)
	m.info.ProxyTypeCounts[proxyType] = counter

	proxyStats, ok := m.info.ProxyStatistics[name]
	if !ok || proxyStats.ProxyType != proxyType {
		proxyStats = &ProxyStatistics{
			Name:       name,
			ProxyType:  proxyType,
			CurConns:   metric.NewCounter(),
			TrafficIn:  metric.NewDateCounter(ReserveDays),
			TrafficOut: metric.NewDateCounter(ReserveDays),
		}
		m.info.ProxyStatistics[name] = proxyStats
	}
	proxyStats.User = user
	proxyStats.ClientID = clientID
	proxyStats.LastStartTime = m.clock.Now()
}

func (m *serverMetrics) CloseProxy(name string, proxyType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if counter, ok := m.info.ProxyTypeCounts[proxyType]; ok {
		counter.Dec(1)
	}
	if proxyStats, ok := m.info.ProxyStatistics[name]; ok {
		proxyStats.LastCloseTime = m.clock.Now()
	}
}

func (m *serverMetrics) OpenConnection(name string, _ string) {
	m.info.CurConns.Inc(1)

	m.mu.Lock()
	defer m.mu.Unlock()
	proxyStats, ok := m.info.ProxyStatistics[name]
	if ok {
		proxyStats.CurConns.Inc(1)
	}
}

func (m *serverMetrics) OpenConnectionWithInfo(info server.ConnectionOpenInfo) uint64 {
	m.info.CurConns.Inc(1)

	m.mu.Lock()
	defer m.mu.Unlock()
	proxyStats, ok := m.info.ProxyStatistics[info.ProxyName]
	if ok {
		proxyStats.CurConns.Inc(1)
	}

	m.info.NextConnectionID++
	id := m.info.NextConnectionID
	remoteIP, remotePort := splitAddr(info.RemoteAddr)
	localIP, localPort := splitAddr(info.LocalAddr)
	m.info.ActiveConns[id] = &ConnectionStatistics{
		ID:          id,
		ProxyName:   info.ProxyName,
		ProxyType:   info.ProxyType,
		RemoteAddr:  info.RemoteAddr,
		RemoteIP:    remoteIP,
		RemotePort:  remotePort,
		LocalAddr:   info.LocalAddr,
		LocalIP:     localIP,
		LocalPort:   localPort,
		ConnectedAt: m.clock.Now(),
	}
	return id
}

func (m *serverMetrics) CloseConnection(name string, _ string) {
	m.info.CurConns.Dec(1)

	m.mu.Lock()
	defer m.mu.Unlock()
	proxyStats, ok := m.info.ProxyStatistics[name]
	if ok {
		proxyStats.CurConns.Dec(1)
	}
}

func (m *serverMetrics) CloseConnectionWithInfo(id uint64, name string, _ string, trafficIn int64, trafficOut int64) {
	proxyName := name

	m.info.CurConns.Dec(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	if conn, ok := m.info.ActiveConns[id]; ok {
		delete(m.info.ActiveConns, id)
		conn.DisconnectedAt = m.clock.Now()
		conn.TrafficIn = trafficIn
		conn.TrafficOut = trafficOut
		proxyName = conn.ProxyName
		m.info.ConnectionHistory = append(m.info.ConnectionHistory, conn)
		if len(m.info.ConnectionHistory) > ConnectionHistoryLimit {
			copy(m.info.ConnectionHistory, m.info.ConnectionHistory[len(m.info.ConnectionHistory)-ConnectionHistoryLimit:])
			m.info.ConnectionHistory = m.info.ConnectionHistory[:ConnectionHistoryLimit]
		}
	}

	if proxyName == "" {
		return
	}
	if proxyStats, ok := m.info.ProxyStatistics[proxyName]; ok {
		proxyStats.CurConns.Dec(1)
	}
}

func (m *serverMetrics) AddTrafficIn(name string, _ string, trafficBytes int64) {
	m.info.TotalTrafficIn.Inc(trafficBytes)

	m.mu.Lock()
	defer m.mu.Unlock()

	proxyStats, ok := m.info.ProxyStatistics[name]
	if ok {
		proxyStats.TrafficIn.Inc(trafficBytes)
	}
}

func (m *serverMetrics) AddTrafficOut(name string, _ string, trafficBytes int64) {
	m.info.TotalTrafficOut.Inc(trafficBytes)

	m.mu.Lock()
	defer m.mu.Unlock()

	proxyStats, ok := m.info.ProxyStatistics[name]
	if ok {
		proxyStats.TrafficOut.Inc(trafficBytes)
	}
}

// Get stats data api.

func (m *serverMetrics) GetServer() *ServerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &ServerStats{
		TotalTrafficIn:  m.info.TotalTrafficIn.TodayCount(),
		TotalTrafficOut: m.info.TotalTrafficOut.TodayCount(),
		CurConns:        int64(m.info.CurConns.Count()),
		ClientCounts:    int64(m.info.ClientCounts.Count()),
		ProxyTypeCounts: make(map[string]int64),
	}
	for k, v := range m.info.ProxyTypeCounts {
		s.ProxyTypeCounts[k] = int64(v.Count())
	}
	return s
}

func toProxyStats(name string, proxyStats *ProxyStatistics) *ProxyStats {
	ps := &ProxyStats{
		Name:            name,
		Type:            proxyStats.ProxyType,
		User:            proxyStats.User,
		ClientID:        proxyStats.ClientID,
		TodayTrafficIn:  proxyStats.TrafficIn.TodayCount(),
		TodayTrafficOut: proxyStats.TrafficOut.TodayCount(),
		CurConns:        int64(proxyStats.CurConns.Count()),
	}
	if !proxyStats.LastStartTime.IsZero() {
		ps.LastStartTime = proxyStats.LastStartTime.Format("01-02 15:04:05")
		ps.LastStartAt = proxyStats.LastStartTime.Unix()
	}
	if !proxyStats.LastCloseTime.IsZero() {
		ps.LastCloseTime = proxyStats.LastCloseTime.Format("01-02 15:04:05")
		ps.LastCloseAt = proxyStats.LastCloseTime.Unix()
	}
	return ps
}

func (m *serverMetrics) GetProxiesByType(proxyType string) []*ProxyStats {
	res := make([]*ProxyStats, 0)
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, proxyStats := range m.info.ProxyStatistics {
		if proxyStats.ProxyType != proxyType {
			continue
		}
		res = append(res, toProxyStats(name, proxyStats))
	}
	return res
}

func (m *serverMetrics) GetProxiesByTypeAndName(proxyType string, proxyName string) (res *ProxyStats) {
	m.mu.Lock()
	defer m.mu.Unlock()

	proxyStats, ok := m.info.ProxyStatistics[proxyName]
	if ok && proxyStats.ProxyType == proxyType {
		res = toProxyStats(proxyName, proxyStats)
	}
	return
}

func (m *serverMetrics) GetProxyByName(proxyName string) (res *ProxyStats) {
	m.mu.Lock()
	defer m.mu.Unlock()

	proxyStats, ok := m.info.ProxyStatistics[proxyName]
	if ok {
		res = toProxyStats(proxyName, proxyStats)
	}
	return
}

func (m *serverMetrics) GetProxyTraffic(name string) (res *ProxyTrafficInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	proxyStats, ok := m.info.ProxyStatistics[name]
	if ok {
		res = &ProxyTrafficInfo{
			Name: name,
		}
		res.TrafficIn = proxyStats.TrafficIn.GetLastDaysCount(ReserveDays)
		res.TrafficOut = proxyStats.TrafficOut.GetLastDaysCount(ReserveDays)
	}
	return
}

func (m *serverMetrics) GetProxyConnections(name string, status string, page int, pageSize int) ([]*ConnectionInfo, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items := make([]*ConnectionInfo, 0)
	if status == "" || status == ConnectionStatusActive {
		for _, conn := range m.info.ActiveConns {
			if name != "" && conn.ProxyName != name {
				continue
			}
			items = append(items, m.toConnectionInfo(conn, ConnectionStatusActive))
		}
	}
	if status == "" || status == ConnectionStatusClosed {
		for i := len(m.info.ConnectionHistory) - 1; i >= 0; i-- {
			conn := m.info.ConnectionHistory[i]
			if name != "" && conn.ProxyName != name {
				continue
			}
			items = append(items, m.toConnectionInfo(conn, ConnectionStatusClosed))
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ConnectedAt > items[j].ConnectedAt
	})

	total := len(items)
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 50
	}
	start := (page - 1) * pageSize
	if start >= total {
		return []*ConnectionInfo{}, total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return items[start:end], total
}

func (m *serverMetrics) toConnectionInfo(conn *ConnectionStatistics, status string) *ConnectionInfo {
	info := &ConnectionInfo{
		ID:          conn.ID,
		ProxyName:   conn.ProxyName,
		ProxyType:   conn.ProxyType,
		RemoteAddr:  conn.RemoteAddr,
		RemoteIP:    conn.RemoteIP,
		RemotePort:  conn.RemotePort,
		LocalAddr:   conn.LocalAddr,
		LocalIP:     conn.LocalIP,
		LocalPort:   conn.LocalPort,
		Status:      status,
		ConnectedAt: conn.ConnectedAt.Unix(),
		TrafficIn:   conn.TrafficIn,
		TrafficOut:  conn.TrafficOut,
	}
	if proxyStats, ok := m.info.ProxyStatistics[conn.ProxyName]; ok {
		info.User = proxyStats.User
		info.ClientID = proxyStats.ClientID
	}
	now := m.clock.Now()
	if status == ConnectionStatusClosed && !conn.DisconnectedAt.IsZero() {
		info.DisconnectedAt = conn.DisconnectedAt.Unix()
		info.Duration = int64(conn.DisconnectedAt.Sub(conn.ConnectedAt).Seconds())
	} else {
		info.Duration = int64(now.Sub(conn.ConnectedAt).Seconds())
	}
	return info
}

func splitAddr(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}
