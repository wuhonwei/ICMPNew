package dao

import (
	"ICMPSimply/config"
	"ICMPSimply/dataStore/measureChange"
	"ICMPSimply/mylog"
	"ICMPSimply/state"
	"ICMPSimply/statisticsAnalyse"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xormplus/xorm"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var logger = mylog.GetLogger()
var RetryTime = 0
var InsertTableNums = 0
var Engine *xorm.Engine
var UploadSeconds int64

func BirthDBWithTimeout(conf *config.Config)  {
	done := make(chan struct{}, 1)
	go func() {
		args := fmt.Sprintf("%s:%s@%s(%s)/%s?charset=utf8&parseTime=true&loc=Local", conf.Data.MysqlUser, conf.Data.MysqlPassWord, "tcp", conf.Data.MysqlAddress, "measure-data")
		Engine, _ = xorm.NewMySQL(xorm.MYSQL_DRIVER, args)
		done <- struct{}{}
	}()

	select {
	case <-done:
		logger.Info(fmt.Sprintf("Make\tmysql Engine done\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return
	case <-time.After(10 * time.Second)://设置10s超时，经过测试机器正常情况10s内就能创建Engine，10s都没完成的话肯定是哪里出了问题，重来吧
		RetryTime++
		logger.Error(fmt.Sprintf("Make\tmysql Engine fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return
	}
}

func BirthDB(conf *config.Config) (err error) {
	//if !conf.Data.UseDB {
	//	//注意：这是使用配置文件的数据库地址
	args := fmt.Sprintf("%s:%s@%s(%s)/%s?charset=utf8&parseTime=true&loc=Local", conf.Data.MysqlUser, conf.Data.MysqlPassWord, "tcp", conf.Data.MysqlAddress, "measure-data")
	Engine, err = xorm.NewMySQL(xorm.MYSQL_DRIVER, args)
	//} else {
	//	//注意：这是使用ETCD中/measure/database的数据库
	//	//若出现127.0.0.1:3306的报错可能是etcd上没有获取到值（key到期？被删？）
	//	DatabaseFromETCD(conf)
	//	indexDB := Index(HashKeyByCRC32(conf.Data.MyPublicIp), len(conns))
	//	Engine, err = xorm.NewMySQL(xorm.MYSQL_DRIVER, conns[indexDB])
	//}

	if err != nil {
		return err
	}
	if err != nil {
		//logger.Error("Open "+conf.Data.MysqlAddress+" mysql fail ", zap.Error(err))
		logger.Error(fmt.Sprintf("store\topen %v mysql fail\tcpu:%v,mem:%v", conf.Data.MysqlAddress, state.LogCPU, state.LogMEM))
		return
	}
	return
}
func DeadDB() (err error) {
	if Engine != nil {
		err = Engine.Close()
		if err != nil {
			//logger.Error("Close mysql fail ", zap.Error(err))
			logger.Error(fmt.Sprintf("store\tClose mysql fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		}
	}
	return
}

func InsertTableWithTimeout(data []*measureChange.Data) (success int) {
	done := make(chan struct{}, 1)
	go func() {
		session := Engine.NewSession()
		session.Begin()
		count, err := session.Insert(data)
		if err == nil || count != 0 {
			success += int(count)
		}
		err = session.Commit()
		session.Close()
		done <- struct{}{}
	}()
	select {
	case <-done:
		logger.Info(fmt.Sprintf("Insert Table done\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		InsertTableNums++
		return 1
	case <-time.After(20 * time.Second)://设置20s超时，经过测试机器正常情况20s内就能插完一个婊，20s都没完成的话肯定是哪里出了问题，重来吧
		RetryTime++
		logger.Error(fmt.Sprintf("Insert Table fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return 0
	}
}
func IsTableExistWithTimeout(data0 *measureChange.Data) (ok bool ,err error,finish bool) {
	done := make(chan struct{}, 1)
	go func() {
		ok, err = Engine.IsTableExist(data0)
		done <- struct{}{}
	}()
	select {
	case <-done:
		logger.Info(fmt.Sprintf("Check IsTableExist done\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return ok,err,true
	case <-time.After(10 * time.Second)://设置10s超时，经过测试机器正常情况10s内就能完，10s都没完成的话肯定是哪里出了问题，重来吧
		RetryTime++
		logger.Error(fmt.Sprintf("Check IsTableExist fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return ok,err,false
	}
}

func CreateTablesWithTimeout(data0 *measureChange.Data) (err error,finish bool) {
	done := make(chan struct{}, 1)
	go func() {
		err = Engine.CreateTables(data0)
		done <- struct{}{}
	}()
	select {
	case <-done:
		logger.Info(fmt.Sprintf("Create Table done\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return err,true
	case <-time.After(10 * time.Second)://设置10s超时，经过测试机器正常情况10s内就能完，10s都没完成的话肯定是哪里出了问题，重来吧
		RetryTime++
		logger.Error(fmt.Sprintf("Create Table fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
		return err,false
	}
}

// TODO 封装各个插件的数据，然后利用GORM上传
func Store(cells []statisticsAnalyse.Cell, conf *config.Config) int {
	//debugTime:=time.Now()
	//fmt.Println("start store:",debugTime)
	UploadSeconds=time.Now().Unix()
	var err error
	for Engine == nil {
		BirthDBWithTimeout(conf)
	}
	if err != nil {
		logger.Error("Birth DB fail", zap.Error(err))
	}
	//debugTime=time.Now()
	//fmt.Println("after birth Engine:",debugTime)
	//err2 := Engine.Ping()
	//debugTime=time.Now()
	//fmt.Println("after ping test:",debugTime)
	//if err2 != nil {
	//	logger.Error("Ping mysql databases fail", zap.Error(err2))
	//	return 0
	//}
	logger.Info(fmt.Sprintf("store\tstart insert measure data\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	var affectRows = 0
	multiTable, tableSlice := sep2MultiTable(cells)
	for _, tableName := range tableSlice {
		data := multiTable[tableName]
		if len(data) == 0 {
			continue
		}
		ok, err,finish :=IsTableExistWithTimeout(data[0])
		for finish==false{
			ok, err,finish =IsTableExistWithTimeout(data[0])
		}
		if !ok || err != nil {
			err,finish=CreateTablesWithTimeout(data[0])
			for finish==false{
				err,finish=CreateTablesWithTimeout(data[0])
			}
			if err != nil {
				logger.Error("Create Table fail", zap.Error(err))
			}
		}
	}
	//debugTime=time.Now()
	//fmt.Println("before new session:",debugTime)
	//session := Engine.NewSession()
	//debugTime=time.Now()
	//fmt.Println("after new session:",debugTime)
	//err = session.Begin()
	//debugTime=time.Now()
	//fmt.Println("now session begin:",debugTime)

	for _, tableName := range tableSlice {
		s:= InsertTableWithTimeout(multiTable[tableName])
		for s==0{
			s= InsertTableWithTimeout(multiTable[tableName])
		}
	}
	UploadSeconds=time.Now().Unix()-UploadSeconds
	//debugTime=time.Now()
	//fmt.Println("after insert all table:",debugTime)
	//err = session.Commit()
	//debugTime=time.Now()
	//fmt.Println("after session commit:",debugTime)
	if err != nil {
		logger.Error("Commit create table fail:", zap.Error(err))
	}
	//logger.Info("finish record", zap.Int("nums", affectRows))
	logger.Info(fmt.Sprintf("store\tfinish record measure data\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	//err := session.Close()
	//if err != nil {
	//	//logger.Error("Close mysql fail ", zap.Error(err))
	//	logger.Error(fmt.Sprintf("store\tClose mysql fail\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	//	return 0
	//}

	//每次测量完查一次，影响效率
	//for _, tableName := range tableSlice {
	//	SendTimeStamp:=multiTable[tableName][0].TimeStamp
	//	sql_2_1 := "select count(*) from "+tableName+" where timestamp='"+SendTimeStamp+"' and source_name='"+conf.Data.Hostname+"'"
	//	results, err := Engine.QueryString(sql_2_1)
	//	if err != nil {
	//		logger.Error("Engine.QueryString fail:", zap.Error(err))
	//	}
	//	//fmt.Println(results[0]["count(*)"])
	//	s_insert,_:=strconv.Atoi(results[0]["count(*)"])
	//	SendTimeStampInt64, err := strconv.ParseInt(SendTimeStamp, 10, 64)
	//	//fmt.Println(tableName[:len(tableName)-9])
	//	n9e.Collect(conf.Data.Hostname,float64(s_insert),SendTimeStampInt64,conf.Data.Step,"proc.InsertNums."+tableName[:len(tableName)-9])
	//}

	//if Engine != nil {
	//	err = DeadDB()
	//}
	//if err != nil {
	//	logger.Error("Dead DB fail", zap.Error(err))
	//}
	return affectRows
}
func sep2MultiTable(cells []statisticsAnalyse.Cell) (dataMap map[string][]*measureChange.Data, tableNameSlice []string) {
	// 直接make，永远不要返回nil
	tableNameSlice = make([]string, 0)
	dataMap = make(map[string][]*measureChange.Data)
	for _, cell := range cells {
		metric := &measureChange.MetaValue{
			Endpoint: cell.Endpoint,
			Metric:   cell.Metric,
			Value:    cell.Value,
			Step:     strconv.FormatInt(cell.Step, 10),
			Type:     cell.CounterType,
			//
			Tags:       dictedTagstring(cell.Tags),
			Timestamp:  strconv.FormatInt(cell.Timestamp, 10),
			SourceIp:   cell.SourceIp,
			SourceName: cell.SourceName,
			DstIp:      cell.DestIp,
		}
		data := measureChange.Convert(metric)
		tableName := data.TableName()
		if _, ok := dataMap[tableName]; !ok {
			dataMap[tableName] = make([]*measureChange.Data, 0)
			tableNameSlice = append(tableNameSlice, tableName)
		}
		dataMap[tableName] = append(dataMap[tableName], data)
	}
	return
}
