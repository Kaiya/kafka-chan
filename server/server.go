package server

import (
	"bytes"
	"compress/gzip"
	"context"

	"fmt"
	"io"
	"strings"

	"github.com/Kaiya/kafka-chan/kafkapb"
	lru "github.com/hashicorp/golang-lru"
	"github.com/hoveychen/kafka-go"
	"github.com/pkg/errors"
	"gitlab.momoso.com/mms2/utils/kafkautil"
	"gitlab.momoso.com/mms2/utils/lg"
	"gitlab.momoso.com/mms2/utils/service"
)

type Server struct {
	consumerId  string
	kafkaClient *kafka.Client
	msgCache    *lru.Cache
	brokers     []string
}

func NewServer(consumerId string, brokers []string) *Server {
	cache, err := lru.New(102400)
	if err != nil {
		lg.Error("error creating memory cache: ", err)
	}
	return &Server{
		consumerId:  consumerId,
		kafkaClient: service.DialKafkaClient("kafka"),
		msgCache:    cache,
		brokers:     brokers,
	}
}

func (s *Server) MakeMemoryLRUCache(ctx context.Context, in *kafkapb.MakeMemoryLRUCacheRequest) (*kafkapb.MakeMemoryLRUCacheReply, error) {
	return nil, nil
}

func (s *Server) QueryMsgByKeyword(ctx context.Context, in *kafkapb.QueryMsgByKeywordRequest) (*kafkapb.QueryMsgByKeywordReply, error) {
	/*
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"kafka"},
			GroupID:   s.consumerId,
			Topic:     in.GetKafkaTopic(),
			Partition: int(in.GetPartition()),
		})
	*/
	// cache, key: topic-keyword, value: gzip compressed msgValue
	resp := new(kafkapb.QueryMsgByKeywordReply)
	cacheKey := fmt.Sprintf("%s-%s", in.GetKafkaTopic(), in.GetKeyword())
	//get from cache
	if s.msgCache.Contains(cacheKey) {
		lg.Info("got msg in cache")
		compressedValue, _ := s.msgCache.Get(cacheKey)
		value, err := decompress(compressedValue.([]byte))
		if err != nil {
			return nil, errors.Wrap(err, "decompresse")
		}
		resp.MsgJson = string(value)
		return resp, nil
	}
	reader := service.DialKafkaReader("kafka", in.GetKafkaTopic(), s.consumerId)
	for {
		/*
			lag, err := kafkautil.GetLag(ctx, s.kafkaClient, in.GetKafkaTopic(), s.consumerId)
			if err != nil {
				return nil, errors.Wrap(err, "kafka get lag")
			}
			if lag == 0 {
				lg.Info("iterate done, not found")
				return nil, errors.New("not found on earth")
			}
		*/
		msg, commitFn, err := kafkautil.Fetch(ctx, reader)
		if err != nil {
			return nil, errors.Wrap(err, "kafka fetch msg error")
		}
		lg.Infof("finding keyword:%s in offset:%d", in.GetKeyword(), msg.Offset)
		switch in.GetKeywordFrom() {
		case kafkapb.KeywordFromType_KAFKA_MSG_KEY:
			msgKey := string(msg.Key)
			if strings.Contains(msgKey, in.GetKeyword()) {
				r, err := gzip.NewReader(bytes.NewReader(msg.Value))
				if err != nil {
					lg.Errorf("new gzip decoder: %s", errors.Wrap(err, ""))
					return nil, errors.Wrap(err, "new gzip decoder")
				}
				//store to cache
				s.msgCache.Add(cacheKey, msg.Value)
				//decompresse
				uncompressed, err := io.ReadAll(r)
				if err != nil {
					lg.Errorf("decode gzip", errors.Wrap(err, ""))
					return nil, errors.Wrap(err, "decode gzip")
				}
				resp.MsgJson = string(uncompressed)
				return resp, nil
			}
		case kafkapb.KeywordFromType_KAFKA_MSG_VALUE:
			if !kafkautil.IsGzipCompressed(msg.Value) {
				return nil, errors.New("kafka msg is not gzip compressed")
			} else {
				if ok, str := existsKeywordInBytes(msg.Value, in.GetKeyword()); ok {
					// store to cache
					s.msgCache.Add(cacheKey, msg.Value)
					resp.MsgJson = str
					return resp, nil
				}
			}
		}
		if err := commitFn(ctx); err != nil {
			return nil, fmt.Errorf("fetch msg at offset:%d commit error:%s", msg.Offset, err)
		}
	}
}

func (s *Server) ProduceMsgToTopic(ctx context.Context, in *kafkapb.ProduceMsgToTopicRequest) (*kafkapb.ProduceMsgToTopicReply, error) {
	writer := service.DialKafkaWriter("kafka", in.GetKafkaTopic())
	// kafkautil.WriteGzipJSONContext(ctx, writer, "", in.GetMsgJson())
	compressed, err := compress([]byte(in.GetMsgJson()))
	if err != nil {
		return nil, errors.Wrap(err, "compress")
	}
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(in.GetKey()),
		Value: compressed,
	})
	if err != nil {
		return nil, errors.Wrap(err, "write messagee")
	}
	return &kafkapb.ProduceMsgToTopicReply{Ok: true}, nil
}

func compress(original []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(original); err != nil {
		return nil, errors.Wrap(err, "Encode json to gzip")
	}
	if err := w.Close(); err != nil {
		return nil, errors.Wrap(err, "Encoder close")
	}
	return buf.Bytes(), nil
}

func existsKeywordInBytes(decompressedMsg []byte, keyword string) (bool, string) {
	str := string(decompressedMsg)
	if !strings.Contains(str, keyword) {
		return false, ""
	}
	return true, str
}

func decompress(ziped []byte) ([]byte, error) {
	defer lg.TimeFuncDebug("decompress")()
	r, err := gzip.NewReader(bytes.NewReader(ziped))
	if err != nil {
		lg.Errorf("new gzip decoder: %s", errors.Wrap(err, ""))
		return nil, errors.Wrap(err, "new gzip decoder")
	}
	uncompressed, err := io.ReadAll(r)
	if err != nil {
		lg.Errorf("decode gzip", errors.Wrap(err, ""))
		return nil, errors.Wrap(err, "decode gziped msg")
	}
	return uncompressed, nil
}
