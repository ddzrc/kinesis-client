package kinesis_client


import (
	"context"
	"testing"
)

func StartupConsumer() (err error) {
	//启动projectDb 消费
	ticketIndexBinLog := NewKinesisConsumer("conf.Conf.AwsKinesisAccessKeyId", "conf.Conf.AwsKinesisAccessKey").
		SetConsumerConfig("ProjectKDBConsumerStreamName"," ProjectKDBConsumerName").
		SetHandleFunc(func(ctx context.Context, record []*ShardRecord) (records []*ShardRecord, e error) {
			return nil, nil
		})

	err =  ticketIndexBinLog.Startup()

	if err != nil {
		return err
	}

	return nil
}

func TestClient(t *testing.T){
	StartupConsumer()
}
