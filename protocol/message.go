package protocol

type Message struct {
	Address                 string
	Endpoint                string
	Size                    int
	Sequence                uint64 //
	Protocol                string
	IsFinish                bool //
	Alias                   string
	PeriodPacketNum         uint64
	PercentPacketIntervalMs int64
	PercentPacketIntervalUs int64
	PeriodNum               int64 //同一个包发多少次
	Step                    int64
	Hostname                string
	PeriodMs                uint64 //发包间隔
	PacketSeq               int    //第几个message
}
