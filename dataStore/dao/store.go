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
)

var logger = mylog.GetLogger()

var Engine *xorm.Engine

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

// TODO 封装各个插件的数据，然后利用GORM上传
func Store(cells []statisticsAnalyse.Cell, conf *config.Config) int {
	var err error
	if Engine == nil {
		err = BirthDB(conf)
	}
	if err != nil {
		logger.Error("Birth DB fail", zap.Error(err))
	}
	err2 := Engine.Ping()
	if err2 != nil {
		logger.Error("Ping mysql databases fail", zap.Error(err2))
		return 0
	}
	logger.Info(fmt.Sprintf("store\tstart insert measure data\tcpu:%v,mem:%v", state.LogCPU, state.LogMEM))
	var affectRows = 0
	multiTable, tableSlice := sep2MultiTable(cells)
	for _, tableName := range tableSlice {
		data := multiTable[tableName]
		if len(data) == 0 {
			continue
		}
		ok, err := Engine.IsTableExist(data[0])
		if !ok || err != nil {

			if err = Engine.CreateTables(data[0]); err != nil {
				logger.Error("Create Table fail", zap.Error(err))
			}

		}
	}
	session := Engine.NewSession()
	err = session.Begin()
	for _, tableName := range tableSlice {
		data := multiTable[tableName]
		count, err := session.Insert(data)
		if err == nil || count != 0 {
			affectRows += int(count)
		}
	}
	err = session.Commit()
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
	if Engine != nil {
		err = DeadDB()
	}
	if err != nil {
		logger.Error("Dead DB fail", zap.Error(err))
	}
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
