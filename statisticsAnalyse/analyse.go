package statisticsAnalyse

import (
	"ICMPSimply/mylog"
	"go.uber.org/zap"
	"math"
	"os"
	"sync"
)

var logger = mylog.GetLogger()

type Cell struct {
	Endpoint    string
	SourceName  string
	Metric      string
	Timestamp   int64
	Step        int64
	Value       float64
	CounterType string
	Tags        string
	SourceIp    string
	DestIp      string
}

//每个包的记录信息
type RecvStatic struct {
	Seq           uint64
	Alias         string
	Proto         string
	Size          int
	RTT           float64
	TTL           int
	SendTimeStamp int64
	RecvTimeStamp int64
	IsValid       bool
}

func GetResult(seqs []uint64, recv *sync.Map) []float64 {
	rtts := make([]float64, len(seqs))
	if recv == nil {
		logger.Error("recv map is nil ")
		os.Exit(1)
	}
	for index := 0; index < len(seqs); index++ { // 获取有效的RTT值
		value, ok := recv.Load(seqs[index])
		if !ok {
			logger.Error("recv[seqs[index]] is nil:", zap.Uint64("seqs[index]", seqs[index]), zap.Int("index", index))
			continue
		}
		res, ok := value.(RecvStatic)
		if !ok {
			logger.Error("error value type ,it must be config.RecvStatic", zap.Any("res", res))
			continue
		}
		sta := res
		rtts[index] = math.Floor(sta.RTT*100) / 100
	}
	capRecv := 0
	recv.Range(func(key, value interface{}) bool {
		capRecv++
		return true
	})
	// 最值 方差 四分位数等等
	res := RttToAllStatistics(rtts)                                           //除丢包率，其他均获得
	lossRate := math.Floor((1-float64(len(seqs))/float64(capRecv))*100) / 100 // 丢包率
	//logger.Infof("packet_loss = %v", lossRate)
	res = append(res, lossRate)
	return res
}
