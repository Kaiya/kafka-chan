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
	}
	for _, r := range routes {
		s.Router.PathPrefix("/").Methods(r.Method).Path(r.Path).Name(r.Name).Handler(r.Handler)
	}
	return s
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
