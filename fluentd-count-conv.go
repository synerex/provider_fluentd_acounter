package main

// simple conversion file from fluentd into people are counter format.

import (
	"context"
	//	"encoding/json"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	fluentd "github.com/synerex/proto_fluentd"
	pcounter "github.com/synerex/proto_pcounter"

	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	//	"io/ioutil"

	"log"
	"sync"
)

var (
	nodesrv  = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local    = flag.String("local", "", "Local Synerex Server")
	mu       sync.Mutex
	version  = "0.01"
	areaChan chan *pcounter.ACounter
)

func supplyFluentdCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	//	ac := &pcounter.ACounter{}
	fd := &fluentd.FluentdRecord{}

	err := proto.Unmarshal(sp.Cdata.Entity, fd)

	if err == nil {
		//		fmt.Printf("%#v ::", sp.SupplyName) // "RS Notify"
		fmt.Printf("C:%#v", fd)
		fmt.Printf("%s\n", ptypes.TimestampString(fd.Time))

		ac := &pcounter.ACounter{
			Ts:       fd.Time,
			AreaName: "Shop",
			AreaId:   0,
			Count:    int32(fd.Record[0]),
		}
		areaChan <- ac
	}

}

func subscribeFluentdSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyFluentdCallback)
	log.Fatal("Error on subscribe")
}

func supplyChannelAcounter(client *sxutil.SXServiceClient, areaChan chan *pcounter.ACounter) {

	for {
		ac := <-areaChan

		out, _ := proto.Marshal(ac)
		cont := api.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "ACounter:0",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		if nerr != nil {
			log.Printf("Send Fail!\n", nerr) //			client.Reconnect() we need to reconsider error ...

		} else {
			log.Printf("Sent OK! %#v\n", *ac)
		}

	}

}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()

	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	channelTypes := []uint32{pbase.FLUENTD_SERVICE, pbase.AREA_COUNTER_SVC}
	srv, rerr := sxutil.RegisterNode(*nodesrv, "fd2ac_conv", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		srv = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(srv)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connected with SynerexServer")
	}

	fd_client := sxutil.NewSXServiceClient(client, pbase.FLUENTD_SERVICE, "{Client:FDconv}")

	wg.Add(1)
	log.Print("Subscribe Supply Fluentd Channel")
	areaChan = make(chan *pcounter.ACounter)

	ac_client := sxutil.NewSXServiceClient(client, pbase.AREA_COUNTER_SVC, "{Client:FDconvAC}")
	go supplyChannelAcounter(ac_client, areaChan)

	go subscribeFluentdSupply(fd_client)

	wg.Wait()

}
