package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
	"github.com/fabrikiot/wsmqttrt/wsmqttrtpusher"
	"github.com/golang/snappy"
)

var (
	logger *log.Logger

	msgch = make(chan PublishMsg, 10000)
)

type QdevRawData struct {
	LUptime  int64  `json:"luptime"`
	LUTCtime int64  `json:"lutctime"`
	ModHex   string `json:"modhex"`
	STime    int64  `json:"stime"`
}

type PublishMsg struct {
	deviceid string
	payload  *DataToPublish
}

type DataToPublish struct {
	SDeviceId string  `json:"sdeviceid"`
	Lat       float64 `json:"lat"`
	Long      float64 `json:"long"`
	LUTCtime  int64   `json:"lutctime"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run filename.go <topic>")
		os.Exit(1)
	}

	topic := strings.TrimSpace(os.Args[1])

	if topic == "" {
		fmt.Println("Error: None of the arguments can be empty.")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	}

	wg := &sync.WaitGroup{}
	startWg := &sync.WaitGroup{}
	wspulleropts := wsmqttrtpuller.NewWsMqttRtPullerOpts("qdev1.buzzenergy.in", 11884)

	wspusheropts := wsmqttrtpusher.NewWsMqttRtPusherOpts("qdev1.buzzenergy.in", 11884)
	wspusheropts.PublishCacheSize = 10 * 1024

	stoppedflag := uint32(0)
	var once sync.Once

	statecallback := &wsmqttrtpuller.WsMqttRtPullerStateCallback{
		Started: func() {
			logger.Println("Puller started")
		},
		Stopped: func() {
			once.Do(func() {
				atomic.StoreUint32(&stoppedflag, 1)
				logger.Println("Puller stopped")
			})
		},
	}

	pusherstatecallback := &wsmqttrtpusher.WsMqttRtPusherStateCallback{
		Started: func() {
			logger.Println("Pusher Started")
			startWg.Done()
		},
		Stopped: func() {
			atomic.StoreUint32(&stoppedflag, 1)
		},
	}

	wspusher := wsmqttrtpusher.NewWsMqttRtPusher(wspusheropts, pusherstatecallback)
	startWg.Add(1)
	wspusher.Start()
	startWg.Wait()

	timestart := time.Now().UnixMicro()
	npublished := uint32(0)
	nacked := uint32(0)

	wg.Add(1)
	go func() {
		for atomic.LoadUint32(&stoppedflag) != 1 {
			// currPublished := atomic.LoadUint32(&npublished)
			// currAcked := atomic.LoadUint32(&nacked)
			// // log.Println("NPublished:", currPublished, ",NAcked:", currAcked)
			wspusherstats := wspusher.GetStats()
			wspusherstatsstr, _ := json.Marshal(&wspusherstats)
			log.Println(string(wspusherstatsstr))

			time.Sleep(time.Minute * 1)
		}
		wg.Done()
	}()

	wspusherpubcallback := func(topic []byte, payload []byte, context interface{}, isok bool) {
		// t.Log("Publish callback:", time.Now().UnixMicro(), ":Topic:", string(topic), ",Payload:", string(payload), ",Context:", context, ",Isok?", isok)
		currAcked := atomic.AddUint32(&nacked, 1)
		if currAcked == atomic.LoadUint32(&npublished) {
			timetaken := time.Now().UnixMicro() - timestart
			// log.Println("Time taken:", timetaken, ", micro secs,", currAcked, atomic.LoadUint32(&npublished))
			if timetaken > 0 {
				// log.Println("Throughput:", 1000*1000*int64(atomic.LoadUint32(&npublished))/timetaken)
			}
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case newmsg, ok := <-msgch:
				if !ok {
					return
				}
				// unlimited publish; no upper bound on number of messages
				fullTopic := "/laf/blescan/" + newmsg.deviceid
				topic := []byte(fullTopic)
				payload, err := json.Marshal(newmsg.payload)
				if err != nil {
					log.Println("Error Marshalling the Data")
					continue
				}
				atomic.AddUint32(&npublished, 1)
				publishisok := wspusher.Publish(topic, payload, nil, wspusherpubcallback)
				for !publishisok {
					publishisok = wspusher.Publish(topic, payload, nil, wspusherpubcallback)
					time.Sleep(time.Microsecond * 1000)
				}
				// logger.Printf("Published message for deviceid:%s", newmsg.deviceid)
			}

		}
	}()

	subscribecallback := func(topic []byte, issubscribe bool, isok bool) {
		if isok {
			logger.Printf("Subscribed to topic: %s\n", string(topic))
		} else {
			logger.Printf("Failed to subscribe to topic: %s\n", string(topic))
		}
	}

	msgcallback := func(topic []byte, payload []byte) {
		topics := strings.Split(string(topic), "/")
		if len(topics) < 3 {
			return
		}
		sourceDeviceId := strings.TrimSpace(topics[2])

		decompressed, err := snappy.Decode(nil, payload)
		if err != nil {
			logger.Println("Snappy decompression error:", err)
			return
		}

		var rawData QdevRawData
		err = json.Unmarshal(decompressed, &rawData)
		if err != nil {
			logger.Println("Error unmarshalling payload:", err)
			return
		}

		parsed, err := ParseHexData(rawData.ModHex)
		if err != nil {
			// logger.Println("ParseHexData error:", err)
			return
		}

		if parsed == nil {
			logger.Println("ParseHexData returned nil without error")
			return
		}

		data := &DataToPublish{
			SDeviceId: sourceDeviceId,
			Lat:       parsed.Lat,
			Long:      parsed.Lng,
			LUTCtime:  rawData.LUTCtime,
		}

		for _, eachDevice := range parsed.Devices {
			deviceid := eachDevice.DeviceID
			publishmsg := PublishMsg{
				deviceid: deviceid,
				payload:  data,
			}
			msgch <- publishmsg
		}

	}

	wspuller := wsmqttrtpuller.NewWsMqttRtPuller(wspulleropts, statecallback, msgcallback)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wspuller.Start()
		for atomic.LoadUint32(&stoppedflag) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	fullTopic := "/lafraw/+/" + topic
	wspuller.Subscribe([]byte(fullTopic), subscribecallback)

	ossigch := make(chan os.Signal, 1)
	signal.Notify(ossigch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	<-ossigch
	fmt.Println("Interrupt signal received, stopping...")

	// wspusherstats := wspusher.GetStats()
	// wspusherstatsstr, _ := json.Marshal(&wspusherstats)
	// log.Println(string(wspusherstatsstr))

	cancel()
	wspuller.Stop()
	wspusher.Stop()
	wg.Wait()

}
