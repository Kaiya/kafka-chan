package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Kaiya/kafka-chan/kafkapb"
	"github.com/Kaiya/kafka-chan/server"
	"github.com/gorilla/mux"
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
		{"QueryMsg", "POST", "/msg", s.queryMsg},
	}
	for _, r := range routes {
		s.Router.PathPrefix("/").Methods(r.Method).Path(r.Path).Name(r.Name).Handler(r.Handler)
	}
	return s
}

func (s *WebServer) queryMsg(w http.ResponseWriter, r *http.Request) {
	topic := r.FormValue("kafkaTopic")
	keyword := r.FormValue("keyword")
	keywordFrom := r.FormValue("keywordFrom")
	if topic == "" || keyword == "" || keywordFrom == "" {
		http.Error(w, "missing topic or keyword or keywordFrom", 400)
		return
	}
	reply, err := s.rpcSrv.QueryMsgByKeyword(context.Background(), &kafkapb.QueryMsgByKeywordRequest{
		KafkaTopic: r.FormValue("kafkaTopic"),
		Partition: func() int32 {
			if res, err := strconv.ParseInt(r.FormValue("partition"), 10, 32); err == nil {
				return int32(res)
			}
			return 0
		}(),
		Keyword: r.FormValue("keyword"),
		KeywordFrom: func() kafkapb.KeywordFromType {
			if r.FormValue("keywordFrom") == "value" {
				return kafkapb.KeywordFromType_KAFKA_MSG_VALUE
			}
			return kafkapb.KeywordFromType_KAFKA_MSG_KEY
		}(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("call rpc error:%s", err), 500)
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(reply.MsgJson))
}
