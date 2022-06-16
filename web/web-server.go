package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Kaiya/kafka-chan/kafkapb"
	"github.com/Kaiya/kafka-chan/server"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type WebServer struct {
	*mux.Router
	rpcSrv server.Server
}

func NewWebServer(rpcServer server.Server) *WebServer {
	s := &WebServer{rpcSrv: rpcServer}

	s.Router = mux.NewRouter()

	routes := []struct {
		Name    string
		Method  string
		Path    string
		Handler http.HandlerFunc
	}{
		{"QueryMsg", "POST", "/msg", s.queryMsgHandler},
		{"ProduceMsg", "POST", "/produce_msg", s.produceMsgHandler},
		{"ProduceQueryMsg", "POST", "/produce_query_msg", s.produceQueryMsgHandler},
		{"ReplayMsgGet", "GET", "/msg/replay", s.replayMsgGet},
		{"ReplayMsgPost", "POST", "/msg/replay", s.replayMsgPost},
	}
	for _, r := range routes {
		s.Router.PathPrefix("/").Methods(r.Method).Path(r.Path).Name(r.Name).Handler(r.Handler)
	}
	return s
}
func (s *WebServer) replayMsgGet(w http.ResponseWriter, r *http.Request) {
	topic := r.FormValue("topic")
	partition := r.FormValue("partition")
	offset := r.FormValue("offset")
	key := r.FormValue("key")
	if topic == "" || partition == "" || offset == "" || key == "" {
		http.Error(w, "topic, partition, offset, or key is empty", http.StatusBadRequest)
	}
	ok, err := s.replayMsg(topic, partition, offset, key)
	if err != nil {
		http.Error(w, "call replay msg", http.StatusBadRequest)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(strconv.FormatBool(ok)))
}
func (s *WebServer) replayMsgPost(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Topic     string `json:"topic"`
		Partition string `json:"partition"`
		Offset    string `json:"offset"`
		Key       string `json:"key"`
	}{}
	if data.Topic == "" || data.Partition == "" || data.Offset == "" || data.Key == "" {
		http.Error(w, "topic, partition, offset, or key is empty", http.StatusBadRequest)
	}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "decode request", http.StatusBadRequest)
		return
	}
	ok, err := s.replayMsg(data.Topic, data.Partition, data.Offset, data.Key)
	if err != nil {
		http.Error(w, "call replay msg", http.StatusBadRequest)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(strconv.FormatBool(ok)))

}

func (s *WebServer) replayMsg(topic, partition, offset, key string) (bool, error) {
	p, perr := strconv.Atoi(partition)
	o, oerr := strconv.Atoi(offset)
	if perr != nil || oerr != nil {
		return false, errors.New("parse string error")
	}
	ctx := context.Background()
	reply, err := s.rpcSrv.QueryMsgByOffset(ctx, &kafkapb.QueryMsgByOffsetRequest{
		KafkaTopic: topic,
		Partition:  int64(p),
		Offset:     int64(o),
	})
	if err != nil {
		return false, errors.Wrap(err, "call query msg by offset")
	}
	resp, err := s.rpcSrv.ProduceMsgToTopic(ctx, &kafkapb.ProduceMsgToTopicRequest{
		KafkaTopic: topic,
		Partition:  int32(p),
		MsgJson:    reply.GetMsgJson(),
		Key:        key,
	})
	if err != nil {
		return false, errors.Wrap(err, "call produce msg to topic")
	}
	return resp.GetOk(), nil
}

func (s *WebServer) produceQueryMsgHandler(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Topic       string `json:"kafkaTopic"`
		Partition   string `json:"partition"`
		Key         string `json:"key"`
		Keyword     string `json:"keyword"`
		KeywordFrom string `json:"keywordFrom"`
	}{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "decode request", http.StatusBadRequest)
		return
	}
	msg, err := s.queryMsg(data.Topic, data.Partition, data.Keyword, data.KeywordFrom)
	if err != nil {
		http.Error(w, "call query msg", http.StatusInternalServerError)
		return
	}
	ok, err := s.produceMsg(data.Topic, data.Partition, data.Key, msg)
	if err != nil {
		http.Error(w, "call produce msg", http.StatusInternalServerError)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(strconv.FormatBool(ok)))
}

func (s *WebServer) produceMsgHandler(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Topic     string `json:"kafkaTopic"`
		Partition string `json:"partition"`
		Key       string `json:"key"`
		MsgJson   string `json:"msgJson"`
	}{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "decode request", http.StatusBadRequest)
		return
	}
	ok, err := s.produceMsg(data.Topic, data.Partition, data.Key, data.MsgJson)
	if err != nil {
		http.Error(w, "call produce msg", http.StatusInternalServerError)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(strconv.FormatBool(ok)))
}

func (s *WebServer) produceMsg(topic, partition, key, msgJson string) (bool, error) {
	reply, err := s.rpcSrv.ProduceMsgToTopic(context.Background(), &kafkapb.ProduceMsgToTopicRequest{
		KafkaTopic: topic,
		Partition: func() int32 {
			if p, err := strconv.ParseInt(partition, 10, 32); err == nil {
				return int32(p)
			}
			return 0
		}(),
		Key:     key,
		MsgJson: msgJson,
	})
	if err != nil {
		return false, errors.Wrap(err, "call rpc")
	}
	return reply.GetOk(), nil
}

func (s *WebServer) queryMsgHandler(w http.ResponseWriter, r *http.Request) {
	data := struct {
		KafkaTopic  string `json:"kafkaTopic"`
		Keyword     string `json:"keyword"`
		KeywordFrom string `json:"keywordFrom"`
		Partition   string `json:"partition"`
	}{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "json decode request error", http.StatusBadRequest)
		return
	}
	if data.KafkaTopic == "" || data.Keyword == "" || data.KeywordFrom == "" {
		http.Error(w, "missing topic or keyword or keywordFrom", http.StatusBadRequest)
		return
	}
	msg, err := s.queryMsg(data.KafkaTopic, data.Partition, data.Keyword, data.KeywordFrom)
	if err != nil {
		http.Error(w, fmt.Sprintf("call query msg error:%s", err), http.StatusInternalServerError)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(msg))
}

func (s *WebServer) queryMsg(topic, partition, keyword, keywordFrom string) (string, error) {
	reply, err := s.rpcSrv.QueryMsgByKeyword(context.Background(), &kafkapb.QueryMsgByKeywordRequest{
		KafkaTopic: topic,
		Partition: func() int32 {
			if res, err := strconv.ParseInt(partition, 10, 32); err == nil {
				return int32(res)
			}
			return 0
		}(),
		Keyword: keyword,
		KeywordFrom: func() kafkapb.KeywordFromType {
			if keywordFrom == "value" {
				return kafkapb.KeywordFromType_KAFKA_MSG_VALUE
			}
			return kafkapb.KeywordFromType_KAFKA_MSG_KEY
		}(),
	})
	if err != nil {
		return "", errors.Wrap(err, "call rpc")
	}
	return reply.MsgJson, nil
}
