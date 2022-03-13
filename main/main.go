package main

import (
	"ICMPSimply/config"
	"ICMPSimply/dataStore/dao"
	"ICMPSimply/dataStore/storeToFile"
	"ICMPSimply/measure"
	"ICMPSimply/mylog"
	"ICMPSimply/state"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

const (
	winDirOfConfig   = "./config/MeasureAgent_cur.yml"
	winDirOfCFG      = "./config/cfg.yaml"
	linuxDirOfConfig = "/root/MeasureAgent_cur.yml"
	linuxDirOfCFG    = "/root/point.yml"
)

var (
	configurationFilename string
	win10                 = runtime.GOOS == "windows"
	logger                = mylog.GetLogger()
)

func init() {
	if win10 {
		flag.StringVar(&configurationFilename, "config", winDirOfConfig, "config is invalid, use default.json replace")
	} else {
		flag.StringVar(&configurationFilename, "config", linuxDirOfConfig, "config is invalid, use default.json replace")
	}
	//flag.Parse()
}

func main() {

	go state.CheckCPUAndMem()
	go func() {
		time.Sleep(600*time.Second)
		storeToFile.AppendFile("./RetryStatus.txt","")
		logger.Error("After running for more than 600 s, the program exits!!!!!!!!!!!!!!!!!,RetryTime: "+strconv.Itoa(dao.RetryTime))
		os.Exit(1)
	}()
	//go func() {
	//	//logger.Info("%v", zap.Error(http.ListenAndServe("0.0.0.0:7070", nil)))
	//	logger.Info("%v", zap.Error(http.ListenAndServe("0.0.0.0:7070", nil)))
	//}()
	//flag.Parse()
	var cfgFilename string
	if win10 {
		cfgFilename = "./config/cfg.yaml"
	} else {
		cfgFilename = "/root/point.yml"
	}
	conf, err := config.LoadConfig(configurationFilename, cfgFilename)
	if err != nil {
		log.Fatalf("manage\tfail to load config\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM)
		return
	}
	//if !win10 {
	//	listening.ServerListen(conf) //开启服务器监听
	//}
	confChan := make(chan config.Config, 10)                                 //带缓存的channel，无缓存的channel的读写都将进行堵塞
	//config.DynamicUpdateConfig(configurationFilename, cfgFilename, confChan) //Linux赋权限和更新配置

	//logger.Debug("init end, wait server starting ...", zap.String("time", time.Since(start).String()))
	logger.Debug(fmt.Sprintf("manage\tinit end, wait to server starting ...\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	//listening.WaitServer(&conf) //等待其他节点启动
	//logger.Debug("wait server end,start measuring ...", zap.String("time", time.Since(start).String()))
	logger.Debug(fmt.Sprintf("manage\tstart measuring ...\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	measure.Measure(conf, confChan)
	//logger.Info("end measuring ...", zap.String("time", time.Since(start).String()))
	logger.Info(fmt.Sprintf("manage\tend measuring...\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	storeToFile.AppendFile("./RetryStatus.txt","")
}
