package config

import (
	"ICMPSimply/mylog"
	"ICMPSimply/state"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
)

var logger = mylog.GetLogger()

const configSize = 1000 //大小定为1000
const protocol = "icmp"

type Config struct {
	Data   Data    `json:"settings" yaml:"settings"` //数据库保存
	Points []Point `json:"points" yaml:"points"`
}

type Data struct {
	Hostname      string `json:"hostname" yaml:"hostname"`
	RTCPP         int    `json:"RTCPP" yaml:"RTCPP"`
	RUDPP         int    `json:"RUDPP" yaml:"RUDPP"`
	IsContinuity  bool   `json:"isContinuity" yaml:"isContinuity"`
	Mixture       bool   `json:"mixture" yaml:"mixture"`
	MyPublicIp    string
	Step          int64  `json:"step" yaml:"step"`
	DataSizeMB    int64  `json:"dataSizeMB" yaml:"dataSizeMB"`
	MysqlAddress  string `json:"mysqlAddress" yaml:"mysqlAddress"`
	MysqlUser     string `json:"mysqlUser" yaml:"mysqlUser"`
	MysqlPassWord string `json:"mysqlPassWord" yaml:"mysqlPassWord"`
	UseDB         bool   `json:"useDB" yaml:"useDB"`
	//DBName        string `json:"dbName" yaml:"dbName"`
}

//每个机器的信息
type Point struct {
	Address string `json:"address" yaml:"address"`
	Alias   string `json:"alias" yaml:"alias"`
	Type    string //`json:"type" yaml:"type"`
	Size    int    `json:"size" yaml:"size"`
	//每个周期包数
	PeriodPacketNum     uint64 `json:"periodPacketNum" yaml:"periodPacketNum"`
	PerPacketIntervalMs int64  `json:"perPacketIntervalMs" yaml:"perPacketIntervalMs"`
	PerPacketIntervalUs int64  `json:"perPacketIntervalUs" yaml:"perPacketIntervalUs"`
	//间隔一般为0
	PeriodMs uint64 `json:"periodMs" yaml:"periodMs"`
	//发多少周期默认以前
	PeriodNum int64 `json:"periodNum" yaml:"periodNum"`
}
type cfgJson struct {
	Hostname string `json:"hostname" yaml:"alias"`
	Ip       string `json:"ip" yaml:"address"`
	//MeasureAgent string `yaml:"measureAgent"`
}

// LoadConfig 完成对外界配置的读取与检测，完成功能如下：
// (1) 从指定路径的配置文件获取配置信息 file 为测量配置，cfsFilename 为cfg文件路径
// (2) 删除自己的节点
// (3) 对每个需要测量的IP地址进行连通性测试
// (4) 检测设定的测量步长step是否满足要求
// (5) 删除同一数据中心的其他节点
func LoadConfig(configurationFilename string, cfsFilename string) (*Config, error) {
	//conf := Config{}
	conf, err := GetMeasureAgJson(configurationFilename)
	// 不配置 size或者size=0,同时把协议固定
	generate64byteAnd1024Byte(conf)
	conf.Data.Hostname, conf.Data.MyPublicIp = GetCfgJson(cfsFilename)
	//删除重复的和自己测量自己的
	//deleteMySelfPoint(conf)
	//检测ip是否正常
	IPCheck(conf)
	//检测保存时间是否大于测量时间
	checkStep(conf)
	return conf, err
}
func checkStep(conf *Config) {
	if conf.Data.Step == -1 {
		return
	}
	var timeMillion int64
	step := conf.Data.Step
	for _, point := range conf.Points {
		//所有发包数乘以发包时间，毫秒数
		timeMillion += int64(point.PeriodPacketNum) * (point.PerPacketIntervalMs)
	}
	if conf.Points[0].Size == 0 {
		//Duration是纳秒，但是乘以time.Second单位变为秒
		if time.Duration(step)*time.Second <= (time.Duration(timeMillion)*time.Millisecond+2*time.Second)*2 {
			//logger.Error("step must more than measure time,step= more than,measure time",
			//	zap.Int64("step", step),
			//	zap.Any("time.Duration(timeMillion)*time.Millisecond/time.Second+2) ", time.Duration(2*timeMillion/1e3)*time.Second))
			measureTime := time.Duration(2*timeMillion/1e3) * time.Second
			logger.Error(fmt.Sprintf("manage\tstep must more than measure time. step:%v,measure time:%v.\tcpu:%v,mem:%v", step, measureTime, state.LogCPU, state.LogMEM))

			os.Exit(1)
		}
	}
}
func IPCheck(config *Config) {
	// 域名或IP检测
	for _, point := range config.Points {
		if _, err := net.LookupHost(point.Address); err != nil {
			//logger.Warn("point can't access, please check your configuration", zap.Any("point", point))
			logger.Warn(fmt.Sprintf("manage\tpoint can't access, please check your configuration\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		}
	}
}
func deleteMySelfPoint(conf *Config) {
	points := make([]Point, 0)
	for _, point := range conf.Points {
		s := strings.Split(point.Alias, "_")
		str := ""
		if len(s) > 2 {
			str = s[0] + "_" + s[1]
		}
		//两个地址不相同
		if !strings.EqualFold(point.Address, conf.Data.MyPublicIp) {
			//再判断主机名是否相同
			if str == "" || !strings.Contains(conf.Data.Hostname, str) {
				points = append(points, point)
			}
		}
	}
	conf.Points = points
}
func GetMeasureAgJson(configurationFilename string) (*Config, error) {
	conf := &Config{}
	configBytes, err := ioutil.ReadFile(configurationFilename)
	//err = json.Unmarshal(configBytes, conf)
	err = yaml.Unmarshal(configBytes, conf)
	if err != nil {
		//logger.Error("can't find conf configurationFilename ", zap.String("filename", configurationFilename))
		logger.Error(fmt.Sprintf("manage\tcan't find conf configurationFilename\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	}
	//解密sl
	conf.Data.MysqlPassWord = AesDecrypt(conf.Data.MysqlPassWord, Key)
	conf.Data.MysqlAddress = AesDecrypt(conf.Data.MysqlAddress, Key)
	return conf, err
}
func GetCfgJson(cfgFilename string) (string, string) {
	cfg := &cfgJson{}
	hostnameBytes, err := ioutil.ReadFile(cfgFilename)
	//err = json.Unmarshal(hostnameBytes, cfg)
	err = yaml.Unmarshal(hostnameBytes, cfg)
	if err != nil {
		//logger.Error("get hostname from cfg.json fail:%v", zap.Error(err))
		logger.Error(fmt.Sprintf("manage\tfail to get hostname from cfg.yml\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	}
	return cfg.Hostname, cfg.Ip
}
func generate64byteAnd1024Byte(conf *Config) {
	points := make([]Point, 0)
	for _, point := range conf.Points {
		point.Type = protocol
		if point.Size == 0 {
			point.Size = 64
			points = append(points, point)
			point.Size = 1024
			point.PeriodPacketNum = 50
			point.PerPacketIntervalMs = 20
			points = append(points, point)
		} else {
			points = append(points, point)
		}
	}
	conf.Points = points
}
