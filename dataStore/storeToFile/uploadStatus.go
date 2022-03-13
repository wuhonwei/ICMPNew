package storeToFile

import (
	"ICMPSimply/dataStore/dao"
	"ICMPSimply/measure"
	"bufio"
	"go.uber.org/zap"
	"os"
	"strconv"
	"time"
)

func AppendFile(filePath string,content string) (err error) {
	if dao.UploadSeconds>600{
		dao.UploadSeconds=time.Now().Unix()-dao.UploadSeconds
	}
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		logger.Error(filePath+"文件打开失败 --",zap.Error(err))
	}
	//及时关闭file句柄
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			logger.Error(filePath+"文件关闭失败 --",zap.Error(err))
		}
	}(file)
	//写入文件时，使用带缓存的 *Writer
	write := bufio.NewWriter(file)
	_, err = write.WriteString(strconv.FormatInt(measure.TsFlag,10)+"\t"+strconv.FormatInt(dao.UploadSeconds,10)+"\t"+strconv.Itoa(dao.InsertTableNums)+"\t"+strconv.Itoa(dao.RetryTime)+"\n")
	if err != nil {
		logger.Error(filePath+"文件写入缓存失败 --",zap.Error(err))
		return
	}
	//Flush将缓存的文件真正写入到文件中
	err = write.Flush()
	if err != nil {
		logger.Error(filePath+"文件真正写入失败 --",zap.Error(err))
		return
	}
	return
}
