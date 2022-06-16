package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"gitlab.momoso.com/mms2/utils/service"
)

var (
	cacheResolved = cache.New(time.Second*30, time.Second*10)
	lock          sync.Mutex
)

type consulKafkaResolver struct {
	serviceName string
}

func NewConsulKafkaResolver(serviceName string) *consulKafkaResolver {
	return &consulKafkaResolver{serviceName: serviceName}
}

func (r *consulKafkaResolver) LookupHost(ctx context.Context, addr string) (addrs []string, err error) {
	lock.Lock()
	defer lock.Unlock()

	if ret, exists := cacheResolved.Get(addr); exists {
		return ret.([]string), nil
	}
	defer func() {
		if err == nil {
			cacheResolved.SetDefault(addr, addrs)
		}
	}()
	if addr == r.serviceName {
		// Initial lookup. Directly return broker addresses.
		addrs := service.GetAllAddress(r.serviceName, "")
		if len(addrs) == 0 {
			return nil, errors.New("Failed to lookup address")
		}
		sort.Strings(addrs)
		return addrs, nil
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = "9092"
		addr = fmt.Sprintf("%s:%s", host, port)
	}

	mapAddr, err := service.GetConsulClient().GetMappedAddress(addr)
	if err != nil {
		return []string{addr}, nil
	}
	return []string{mapAddr}, nil
}
