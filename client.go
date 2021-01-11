package kinesis_client

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	"time"
)

type KinesisClient struct {
	consumer             []*Consumer
	client               *kinesis.Kinesis
	err                  error
	RecordMess           chan []*ShardRecord
	shardList            []*kinesisShard
	LastDealTime         time.Time
	ShardListenType      string
	ShardListenStartTime time.Time
	handleFunc
	saveShardSequenceNumber
	queryShardSequenceNumber
	doRecordRetryFunc
	saveRecordRetryFunc
	close         chan struct{}
	closeResponse chan struct{}
}


type Consumer struct {
	StreamName   string
	ConsumerName string
	consumerDesc *kinesis.DescribeStreamConsumerOutput
}

type kinesisShard struct {
	*kinesis.Shard
	close                  chan struct{}
	closeResponse          chan struct{}
	StartingSequenceNumber string
	ConsumerName           string
	ConsumerARN            *string
	DealSequenceNumber     string
}

type ShardSequenceNumber struct {
	Id                 int64
	ConsumerName       string
	ShardId            string
	LastSequenceNumber string
	CreateTimeUtc      time.Time
	UpdateTimeUtc      time.Time
	Flag               int32
}

type ShardRecord struct {
	No                  string
	ShardId             string
	ShardSequenceNumber string
	Data                []byte
	ConsumerName        string
	FailedRetryNum      int64
}

func (s *ShardRecord) SetNo() {
	s.No = s.ConsumerName + "&" + s.ShardId + "&" + s.ShardSequenceNumber
}

type handleFunc func(ctx context.Context, record []*ShardRecord) ([]*ShardRecord, error)

type saveShardSequenceNumber func(ctx context.Context, sequence []*ShardSequenceNumber) error

type queryShardSequenceNumber func(ctx context.Context, consumerName string) ([]*ShardSequenceNumber, error)

type doRecordRetryFunc func(ctx context.Context) error

type saveRecordRetryFunc func(ctx context.Context, record []*ShardRecord) error

func NewKinesisConsumer(AccessKeyID string, AccessKey string) *KinesisClient {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(endpoints.ApSoutheast1RegionID),
			Credentials: credentials.NewStaticCredentials(AccessKeyID, AccessKey, ""),
		},
		Profile:           "",
		SharedConfigState: session.SharedConfigDisable,
	})
	if err != nil {
		log.Panicf("new Session failed:%v", err)
	}

	return &KinesisClient{client: kinesis.New(sess), close: make(chan struct{}, 1)}
}

func (client *KinesisClient) SetConsumerConfig(streamName string, consumerName string) *KinesisClient {
	consumer := Consumer{}
	consumer.StreamName = streamName
	consumer.ConsumerName = consumerName
	client.consumer = append(client.consumer, &consumer)
	return client
}

func (client *KinesisClient) SetRetryFunc(do doRecordRetryFunc, save saveRecordRetryFunc) *KinesisClient {
	client.doRecordRetryFunc = do
	client.saveRecordRetryFunc = save
	return client

}

func (client *KinesisClient) SetShardSequenceFunc(query queryShardSequenceNumber, save saveShardSequenceNumber) *KinesisClient {
	client.queryShardSequenceNumber = query
	client.saveShardSequenceNumber = save
	return client
}

func (client *KinesisClient) SetHandleFunc(handleFunc handleFunc) *KinesisClient {
	if handleFunc == nil {
		client.err = fmt.Errorf("handleFunc为空")
		return client
	}

	client.handleFunc = func(ctx context.Context, record []*ShardRecord) ([]*ShardRecord, error) {
		//var defErr error
		//defer func() {
		//	if rec := recover(); rec != nil {
		//		requestID := utils.RequestIDFromContext(ctx)
		//		const size = 10 * 1024
		//		buf := make([]byte, size)
		//		buf = buf[:runtime.Stack(buf, false)]
		//		logger.Errorf("%s consumer handleFunc panic err:%v", requestID, defErr)
		//	}
		//}()
		records, err := handleFunc(ctx, record)
		if err != nil {
			//defErr = err
			return records, err
		}
		return records, nil
	}
	return client
}

const (
	DefaultConcurrentNum = 1000
)

func (client *KinesisClient) Startup() error {

	if client.handleFunc == nil {
		return fmt.Errorf("[Startup] handleFunc is null")
	}

	if client.doRecordRetryFunc == nil {
		return fmt.Errorf("[Startup] queryRecordRetryFunc is null")
	}
	if client.saveRecordRetryFunc == nil {
		return fmt.Errorf("[Startup] handleFunc is null")
	}

	if client.queryShardSequenceNumber == nil {
		return fmt.Errorf("[Startup] queryShardSequenceNumber is null")
	}
	if client.saveShardSequenceNumber == nil {
		return fmt.Errorf("[Startup] saveShardSequenceNumber is null")
	}
	//初始化并发控制
	client.LastDealTime = time.Now()

	for _, v := range client.consumer {
		err := GetShard(client, v)
		if err != nil {
			return err
		}
	}

	//监听消息
	client.listen()
	//重试错误
	client.retry()

	//开始处理消息
	client.handle()

	return nil

}

func GetShard(client *KinesisClient, c *Consumer) error {
	if client == nil || client.client == nil || c == nil {
		return errors.New("client == nil || client.Client == nil || c == nil")
	}
	desc, err := client.client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(c.ConsumerName),
	})
	if err != nil {
		return fmt.Errorf("descriStream failed, %s, %v", c.StreamName, err)
	}
	fmt.Printf("get DescribeStream success, %v", desc.String())
	descParams := &kinesis.DescribeStreamConsumerInput{
		StreamARN:    desc.StreamDescription.StreamARN,
		ConsumerName: aws.String(c.ConsumerName),
	}
	descConsumer, err := client.client.DescribeStreamConsumer(descParams)
	if err, ok := err.(awserr.Error); ok && err.Code() == kinesis.ErrCodeResourceNotFoundException {
		register, err := client.client.RegisterStreamConsumer(
			&kinesis.RegisterStreamConsumerInput{
				ConsumerName: aws.String(c.ConsumerName),
				StreamARN:    desc.StreamDescription.StreamARN,
			},
		)
		if err != nil {
			return fmt.Errorf("RegisterStreamConsumer failed  %s, %v",
				c.ConsumerName, err)
		}
		fmt.Printf("register DescribeStream success, %v", register.String())
	} else if err != nil {
		return fmt.Errorf("DescribeStreamConsumer failed %s, %v",
			c.ConsumerName, err)
	}

	//连接是否是通的
	for i := 0; i < 10; i++ {
		descConsumer, err := client.client.DescribeStreamConsumer(descParams)
		if err != nil || aws.StringValue(descConsumer.ConsumerDescription.ConsumerStatus) != kinesis.ConsumerStatusActive {
			time.Sleep(time.Second * 1)
			continue
		}
		if descConsumer.ConsumerDescription == nil {
			return fmt.Errorf("descConsumer.ConsumerDescription == nil")
		}
		if i == 10 {
			return fmt.Errorf("DescribeStreamConsumer times exceed 10, %v, %v",
				awsutil.StringValue(descParams.StreamARN), awsutil.StringValue(descParams.ConsumerName))
		}
		break
	}

	c.consumerDesc = descConsumer

	shards, err := client.client.ListShards(&kinesis.ListShardsInput{StreamName: aws.String(c.StreamName)})
	if err != nil || shards == nil {
		return fmt.Errorf("get list shards failed, err:%v", err)
	}

	shardSequenceNums, err := client.queryShardSequenceNumber(context.Background(), c.ConsumerName)
	if err != nil {
		return err
	}
	for _, v := range shards.Shards {
		var shard kinesisShard
		shard.Shard = v
		for _, sequenceNum := range shardSequenceNums {
			if v.ShardId != nil {
				if *v.ShardId == sequenceNum.ShardId {
					shard.StartingSequenceNumber = sequenceNum.LastSequenceNumber
				}
			}
		}
		shard.close = make(chan struct{}, 1)
		shard.closeResponse = make(chan struct{}, 1)
		shard.ConsumerName = c.ConsumerName
		if descConsumer.ConsumerDescription != nil {
			shard.ConsumerARN = descConsumer.ConsumerDescription.ConsumerARN
		}
		client.shardList = append(client.shardList, &shard)
	}

	return nil
}

const (
	recordThresholdLen  = 1000
	recordThresholdTime = time.Microsecond * 300
)

func (client *KinesisClient) listenClose() {
	if client.close == nil {
		client.close = make(chan struct{}, 1)
	}
	for _, v := range client.shardList {
		v.close <- struct{}{}
	}

	for _, v := range client.shardList {
		if v.closeResponse == nil {
			v.closeResponse = make(chan struct{}, 1)
		}
	}
	//等待完全关闭
	for _, v := range client.shardList {
		<-v.closeResponse
	}

}

func (client *KinesisClient) listen() error {
	err := client.listenShard()
	if err != nil {
		return err
	}
	var ticker = time.NewTicker(3 * time.Minute)
	go func() {
		for {
			select {
			case <-client.close:
				client.saveSequenceNum()
				ticker.Stop()
				client.listenClose()
				client.closeResponse <- struct{}{}
			case <-ticker.C:
				log.Print("re listen")
				client.saveSequenceNum()
				//先关闭上次的监听
				client.listenClose()
				err := client.listenShard()
				if err != nil {
					log.Print("[retry] query failed")
				}
			}
		}
	}()
	return nil
}

func (client *KinesisClient) listenShardClose() {
	if client.close == nil {
		client.close = make(chan struct{}, 1)
	}
	if client.closeResponse == nil {
		client.closeResponse = make(chan struct{}, 1)
	}

	for _, v := range client.shardList {
		v.close <- struct{}{}
	}

	for _, v := range client.shardList {
		if v.closeResponse == nil {
			v.closeResponse = make(chan struct{}, 1)
		}
	}
	for _, v := range client.shardList {
		<-v.closeResponse
	}
}

func (client *KinesisClient) GetListenParam(s *kinesisShard) kinesis.SubscribeToShardInput {
	var startType string
	var startSequenceNumber *string

	var startTimesStamp *time.Time
	if s.StartingSequenceNumber != "" {
		startType = kinesis.ShardIteratorTypeAfterSequenceNumber
		startSequenceNumber = aws.String(s.StartingSequenceNumber)
	} else {
		startType = kinesis.ShardIteratorTypeAtTimestamp
		now := time.Now()
		startTimesStamp = &now
	}

	if client.ShardListenType == kinesis.ShardIteratorTypeAtTimestamp {
		startType = kinesis.ShardIteratorTypeAtTimestamp
		if client.ShardListenStartTime.IsZero() {
			now := time.Now()
			startTimesStamp = &now
		} else {
			startTimesStamp = &client.ShardListenStartTime
		}
	}

	return kinesis.SubscribeToShardInput{
		ConsumerARN: s.ConsumerARN,
		StartingPosition: &kinesis.StartingPosition{
			SequenceNumber: startSequenceNumber,
			Type:           aws.String(startType),
			Timestamp:      startTimesStamp,
		},
		ShardId: s.ShardId,
	}
}

func (client *KinesisClient) listenShard() error {
	ctx := context.Background()

	if client.RecordMess == nil {
		client.RecordMess = make(chan []*ShardRecord, 1000)
	}

	f := func(idx int, s *kinesisShard) {
		var shardId string
		if s.ShardId != nil {
			shardId = *s.ShardId
		}

		params := client.GetListenParam(s)
		sub, err := client.client.SubscribeToShardWithContext(ctx, &params)
		if err != nil {
			return
		}

		defer sub.EventStream.Close()

		for {
			select {
			case <-s.close:
				s.closeResponse <- struct{}{}
				err := sub.EventStream.StreamCloser.Close()
				if err != nil {
					fmt.Printf("[%v] [sub.EventStream.StreamCloser.Close] failed, err:%v", ctx, err)
				}
				return
			case event := <-sub.EventStream.Events():
				{
					switch e := event.(type) {
					case *kinesis.SubscribeToShardEvent:
						records := make([]*ShardRecord, 0)
						if len(e.Records) == 0 {
							continue
						}
						for _, record := range e.Records {
							sequenceNum := ""
							if record.SequenceNumber != nil {
								sequenceNum = *record.SequenceNumber
							}
							shardRecord := &ShardRecord{ConsumerName: s.ConsumerName, ShardId: shardId, ShardSequenceNumber: sequenceNum, Data: record.Data}
							shardRecord.SetNo()
							records = append(records, shardRecord)
							s.StartingSequenceNumber = sequenceNum
						}

						client.RecordMess <- records
					}
				}
			}
		}

		if err := sub.EventStream.Err(); err != nil {
			return
		}
	}

	for i, shard := range client.shardList {
		go func(idx int, s *kinesisShard) {
			f(idx, s)
		}(i, shard)
	}

	return nil
}

func (client *KinesisClient) retryClose() {
	if client.close == nil {
		client.close = make(chan struct{}, 1)
	}
	if client.closeResponse == nil {
		client.closeResponse = make(chan struct{}, 1)
	}
	client.close <- struct{}{}
	<-client.closeResponse
}

func (client *KinesisClient) retry() {
	if client.doRecordRetryFunc == nil {
		return
	}

	var ticker = time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-client.close:
				ticker.Stop()
				return
			case <-ticker.C:
				ctx := context.Background()
				err := client.doRecordRetryFunc(ctx)
				if err != nil {
					fmt.Printf("[retry] query failed, err:%v", err)
				}

			}
		}
	}()

}

func (client *KinesisClient) saveSequenceClose() {
	if client.close == nil {
		client.close = make(chan struct{}, 1)
	}
	if client.closeResponse == nil {
		client.closeResponse = make(chan struct{}, 1)
	}
	client.close <- struct{}{}
	<-client.closeResponse
}

func (client *KinesisClient) saveSequenceNum() {
	if client.saveShardSequenceNumber == nil {
		return
	}

	ctx := context.Background()
	sequences := make([]*ShardSequenceNumber, 0)
	for _, v := range client.shardList {
		shardId := ""
		if v.ShardId != nil {
			shardId = *v.ShardId
		}

		if v.DealSequenceNumber == "" {
			continue
		}
		sequence := ShardSequenceNumber{ConsumerName: v.ConsumerName, LastSequenceNumber: v.DealSequenceNumber, ShardId: shardId}
		sequences = append(sequences, &sequence)
	}

	if len(sequences) == 0 {
		return
	}
	err := client.saveShardSequenceNumber(ctx, sequences)
	if err != nil {
		log.Printf("[retry] saveShardSequenceNumber failed, err:%v", err)
	}

}

func (client *KinesisClient) handle() {
	//开始处理
	if client.doRecordRetryFunc == nil {
		return
	}
	go func() {
		totalRecords := make([]*ShardRecord, 0)
		handleTime := time.Now()
		for {
			select {
			case <-client.close:
				return
			case records := <-client.RecordMess:
				totalRecords = append(totalRecords, records...)
				if time.Now().Add(- time.Millisecond*200).Sub(handleTime) < 0 {
					continue
				}
				ctx := context.Background()
				failedRecords, err := client.handleFunc(ctx, totalRecords)
				//有错误继续跑，错误后面通过全量同步
				if err != nil {
					if failedRecords != nil && len(failedRecords) > 0 {
						saveRetryErr := client.saveRecordRetryFunc(ctx, failedRecords)
						if saveRetryErr != nil {
							log.Printf("[%v] [handleFunc] saveRetryErr failed, err:%v", saveRetryErr)
						}
					}
					log.Printf("[%v] [handleFunc] failed, err:%v", ctx, err)
				}
				for _, record := range totalRecords {
					for _, shard := range client.shardList {
						if shard.ConsumerName != record.ConsumerName || shard.ShardId == nil || *shard.ShardId != record.ShardId {
							continue
						}
						shard.DealSequenceNumber = record.ShardSequenceNumber
					}
				}
				totalRecords = make([]*ShardRecord, 0)
			}
		}

	}()

}

func (client *KinesisClient) Stop() {
	ctx := context.Background()

	//消息队列关闭前处理的序列号保存
	sequences := make([]*ShardSequenceNumber, 0)
	for _, v := range client.shardList {
		shardId := ""
		if v.ShardId != nil {
			shardId = *v.ShardId
		}
		sequence := ShardSequenceNumber{ConsumerName: v.ConsumerName, LastSequenceNumber: v.StartingSequenceNumber, ShardId: shardId}
		sequences = append(sequences, &sequence)
	}

	if sequences != nil && len(sequences) != 0 {
		err := client.saveShardSequenceNumber(ctx, sequences)
		if err != nil {
			log.Printf("[%v] [handleFunc] saveShardSequenceNumber failed, err:%v", err)
		}
	}

	client.retryClose()
	client.listenShardClose()
	client.saveSequenceClose()
	if client.close != nil {
		close(client.close)
	}
	if client.closeResponse != nil {
		close(client.closeResponse)
	}
	for _, v := range client.shardList {
		close(v.close)
		close(v.closeResponse)
	}

}
