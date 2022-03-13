package n9e

import (
	"ICMPSimply/mylog"
	"bytes"
	"encoding/json"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"net/http"
)
var logger                = mylog.GetLogger()
func PushData(Data []*MetricValue)(err error){
	url:="http://106.3.133.5/api/transfer/push"
	jsonItems,err:=json.Marshal(Data)
	if err!=nil {
		return
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonItems))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err!=nil {
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)
	body, _ := ioutil.ReadAll(resp.Body)
	var pushResponse PushResponse
	err=json.Unmarshal(body, &pushResponse)
	if err!=nil {
		return
	}
	logger.Info("this time of push finished:",zap.String(pushResponse.Dat,pushResponse.Err))
	return
}
