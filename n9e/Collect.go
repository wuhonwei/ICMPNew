package n9e

func Collect(endPoint string,ValueUntyped float64,UnixTime int64,Step int64,metric string) (err error) {
	var DataMetric []*MetricValue
	items:=make([]MetricValue,1)
	DataMetric=append(DataMetric,&items[0])
	DataMetric[0].Metric=metric
	DataMetric[0].Step=Step
	DataMetric[0].Endpoint=endPoint
	DataMetric[0].Timestamp=UnixTime

	DataMetric[0].ValueUntyped=ValueUntyped
	err = PushData(DataMetric)
	if err != nil {
		err = PushData(DataMetric)
		if err != nil {
			err = PushData(DataMetric)
			if err != nil {

				return
			}
		}
	}
	return
}