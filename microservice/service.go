package main

import (
	context "context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net"
	"regexp"
	"sync"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// если хочется, то для красоты можно разнести логику по разным файликам

type SubPubStat struct {
	subscribers []Subscriber
	mu          *sync.RWMutex
}

type Subscriber struct {
	st     *Stat
	du     uint64
	stream Admin_StatisticsServer
}

func (sub *SubPubStat) Subscribe(s Subscriber) int {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.subscribers = append(sub.subscribers, s)
	return len(sub.subscribers) - 1
}

func (sub *SubPubStat) Publish(index int) {
	sub.mu.RLock()
	duration := sub.subscribers[index].du
	sub.mu.RUnlock()
	for {
		<-time.Tick(time.Second * time.Duration(duration))
		sub.mu.Lock()
		err := sub.subscribers[index].stream.Send(sub.subscribers[index].st)
		if err != nil {
			sub.subscribers[index] = Subscriber{}
			sub.mu.Unlock()
			return
		}
		sub.subscribers[index].st = &Stat{ByMethod: map[string]uint64{}, ByConsumer: map[string]uint64{}}
		sub.mu.Unlock()
	}
}

type SubPubLog struct {
	subscribers []Admin_LoggingServer
	event       chan *Event
	mu          *sync.RWMutex
}

func (sp *SubPubLog) Subscribe(sub Admin_LoggingServer) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.subscribers = append(sp.subscribers, sub)
}

func (sp *SubPubLog) SendWithPossibleUnsub(ev *Event, stream Admin_LoggingServer, i int) {
	err := stream.Send(ev)
	if err != nil {
		sp.subscribers[i] = sp.subscribers[len(sp.subscribers)-1]
		sp.subscribers = sp.subscribers[:len(sp.subscribers)-1]
		sp.SendWithPossibleUnsub(ev, stream, i)
	}
}

func (sp *SubPubLog) Publish(ev *Event) {
	for i, stream := range sp.subscribers {
		sp.SendWithPossibleUnsub(ev, stream, i)
	}
}

type Service struct {
	UnimplementedAdminServer
	UnimplementedBizServer
	mu         *sync.RWMutex
	acl        map[string][]string
	ctx        context.Context
	logging    SubPubLog
	statistics SubPubStat
}

func (srv *Service) Logging(in *Nothing, as Admin_LoggingServer) error {
	srv.logging.Subscribe(as)
	select {
	case <-as.Context().Done():
		return as.Context().Err()
	case <-srv.ctx.Done():
		return srv.ctx.Err()
	}
}

func (srv *Service) Statistics(statInt *StatInterval, client Admin_StatisticsServer) error {
	sub := Subscriber{
		st: &Stat{
			ByMethod:   map[string]uint64{},
			ByConsumer: map[string]uint64{},
		},
		du:     statInt.IntervalSeconds,
		stream: client,
	}
	index := srv.statistics.Subscribe(sub)
	go srv.statistics.Publish(index)
	select {
	case <-srv.ctx.Done():
		return srv.ctx.Err()
	case <-client.Context().Done():
		return client.Context().Err()
	}
}

func (srv *Service) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (srv *Service) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (srv *Service) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (serv *Service) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	var errNoValue interface{}
	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md.Get("consumer")
	if consumer == nil {
		return errNoValue, status.Error(codes.Unauthenticated, "the consumer field is missing")
	}
	serv.mu.RLock()
	availableMethods, isExists := serv.acl[consumer[0]]
	serv.mu.RUnlock()
	if !isExists {
		return errNoValue, status.Error(codes.Unauthenticated, "there's no such consumer")
	}
	for _, curMethod := range availableMethods {
		matched, err := regexp.MatchString(curMethod, info.FullMethod)
		if err != nil {
			return errNoValue, status.Error(codes.Unauthenticated, "Error compare strings")
		}
		if !matched {
			continue
		}

		serv.mu.Lock()
		for i := range serv.statistics.subscribers {
			serv.statistics.subscribers[i].st.ByConsumer[consumer[0]] += 1
			serv.statistics.subscribers[i].st.ByMethod[info.FullMethod] += 1
		}
		client, _ := peer.FromContext(ctx)
		serv.logging.event <- &Event{Consumer: consumer[0], Method: info.FullMethod, Timestamp: time.Now().UnixNano(), Host: client.Addr.String()}
		serv.mu.Unlock()
		return handler(ctx, req)
	}
	return errNoValue, status.Error(codes.Unauthenticated, "no access")
}

func (serv *Service) StreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	md, _ := metadata.FromIncomingContext(ss.Context())
	consumer := md.Get("consumer")
	if consumer == nil {
		return status.Error(codes.Unauthenticated, "the consumer field is missing")
	}
	serv.mu.RLock()
	avaliableMethods, isExists := serv.acl[consumer[0]]
	serv.mu.RUnlock()
	if !isExists {
		return status.Error(codes.Unauthenticated, "there's no such consumer")
	}

	for _, curMethod := range avaliableMethods {
		matched, err := regexp.MatchString(curMethod, info.FullMethod)
		if err != nil {
			return status.Error(codes.Unauthenticated, "Error compare strings")
		}
		if !matched {
			continue
		}

		serv.mu.Lock()
		for i := range serv.statistics.subscribers {
			serv.statistics.subscribers[i].st.ByConsumer[consumer[0]] += 1
			serv.statistics.subscribers[i].st.ByMethod[info.FullMethod] += 1
		}
		client, _ := peer.FromContext(ss.Context())
		serv.logging.Publish(&Event{Consumer: consumer[0], Method: info.FullMethod, Host: client.Addr.String(), Timestamp: time.Now().UnixNano()})
		serv.mu.Unlock()
		return handler(srv, ss)
	}
	return status.Error(codes.Unauthenticated, "no access")
}

func NewServiceServer() *Service {
	mutex := sync.RWMutex{}
	return &Service{
		logging: SubPubLog{
			event:       make(chan *Event),
			subscribers: []Admin_LoggingServer{},
			mu:          &mutex,
		},
		statistics: SubPubStat{
			subscribers: []Subscriber{},
			mu:          &mutex,
		},
		mu: &mutex,
	}
}

func ACLParse(acl string) (map[string][]string, error) {
	aclParse := make(map[string][]string)
	err := json.Unmarshal([]byte(acl), &aclParse)
	return aclParse, err
}

func StartMyMicroservice(ctx context.Context, addr string, acl string) error {
	aclParse, err := ACLParse(acl)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	service := NewServiceServer()
	service.ctx = ctx
	service.acl = aclParse

	server := grpc.NewServer(
		grpc.UnaryInterceptor(service.UnaryInterceptor),
		grpc.StreamInterceptor(service.StreamInterceptor),
	)

	RegisterAdminServer(server, service)
	RegisterBizServer(server, service)
	go func() {
		err = server.Serve(listener)
		if err != nil {
			return
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				server.Stop()
				close(service.logging.event)
				return
			case ev := <-service.logging.event:
				service.mu.Lock()
				service.logging.Publish(ev)
				service.mu.Unlock()
			}
		}
	}()
	return nil
}
