package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/cascades-fbp/cascades/components/utils"
	"github.com/cascades-fbp/cascades/runtime"
	influxdb "github.com/influxdb/influxdb/client"
	zmq "github.com/pebbe/zmq4"
)

var (
	// Flags
	inputEndpoint   = flag.String("port.in", "", "Component's input port endpoint")
	optionsEndpoint = flag.String("port.options", "", "Component's input port endpoint")
	errorEndpoint   = flag.String("port.err", "", "Component's output port endpoint")
	jsonFlag        = flag.Bool("json", false, "Print component documentation in JSON")
	debug           = flag.Bool("debug", false, "Enable debug mode")

	// Internal
	inPort, optionsPort, errPort *zmq.Socket
	inCh, errCh                  chan bool
	err                          error
)

func validateArgs() {
	if *optionsEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *inputEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func openPorts() {
	optionsPort, err = utils.CreateInputPort("influxdb/write.options", *optionsEndpoint, nil)
	utils.AssertError(err)

	inPort, err = utils.CreateInputPort("influxdb/write.in", *inputEndpoint, inCh)
	utils.AssertError(err)

	if *errorEndpoint != "" {
		errPort, err = utils.CreateOutputPort("influxdb/write.err", *errorEndpoint, errCh)
		utils.AssertError(err)
	}
}

func closePorts() {
	optionsPort.Close()
	inPort.Close()
	if errPort != nil {
		errPort.Close()
	}
	zmq.Term()
}

func main() {
	flag.Parse()

	if *jsonFlag {
		doc, _ := registryEntry.JSON()
		fmt.Println(string(doc))
		os.Exit(0)
	}

	log.SetFlags(0)
	if *debug {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	validateArgs()

	ch := utils.HandleInterruption()
	inCh = make(chan bool)
	errCh = make(chan bool)

	openPorts()
	defer closePorts()

	ports := 1
	if errPort != nil {
		ports++
	}

	waitCh := make(chan bool)
	go func(num int) {
		total := 0
		for {
			select {
			case v := <-inCh:
				if !v {
					log.Println("IN port is closed. Interrupting execution")
					ch <- syscall.SIGTERM
				} else {
					total++
				}
			case v := <-errCh:
				if !v {
					log.Println("ERR port is closed. Interrupting execution")
					ch <- syscall.SIGTERM
				} else {
					total++
				}
			}
			if total >= num && waitCh != nil {
				waitCh <- true
			}
		}
	}(ports)

	log.Println("Waiting for port connections to establish... ")
	select {
	case <-waitCh:
		log.Println("Ports connected")
		waitCh = nil
	case <-time.Tick(30 * time.Second):
		log.Println("Timeout: port connections were not established within provided interval")
		os.Exit(1)
	}

	log.Println("Waiting for options to arrive...")
	var (
		influxHost, influxUsername, influxPassword, influxDB, tmpStr string
		parts, kv                                                    []string
		ip                                                           [][]byte
	)
	for {
		ip, err = optionsPort.RecvMessageBytes(0)
		if err != nil {
			log.Println("Error receiving IP:", err.Error())
			continue
		}
		if !runtime.IsValidIP(ip) || !runtime.IsPacket(ip) {
			continue
		}
		tmpStr = string(ip[1])
		parts = strings.Split(tmpStr, ",")
		for _, p := range parts {
			kv = strings.Split(p, "=")
			if len(kv) != 2 {
				continue
			}
			switch kv[0] {
			case "host":
				influxHost = kv[1]
			case "user":
				influxUsername = kv[1]
			case "pass":
				influxPassword = kv[1]
			case "db":
				influxDB = kv[1]
			}
		}
		optionsPort.Close()
		break
	}

	var (
		config *influxdb.ClientConfig
		client *influxdb.Client
	)
	config = &influxdb.ClientConfig{
		Host:     influxHost,
		Username: influxUsername,
		Password: influxPassword,
		Database: influxDB,
	}
	log.Printf("Using ClientConfig = %#v", config)
	client, err := influxdb.NewClient(config)
	if err != nil {
		fmt.Println("Error creating InfluxDB client:", err.Error())
		os.Exit(1)
	}

	log.Println("Started...")
	var series *influxdb.Series
	for {
		ip, err = inPort.RecvMessageBytes(0)
		if err != nil {
			log.Println("Error receiving message:", err.Error())
			continue
		}
		if !runtime.IsValidIP(ip) {
			continue
		}
		err = json.Unmarshal(ip[1], &series)
		if err != nil {
			log.Println("Failed to decode incoming series:", err.Error())
			continue
		}

		if err = client.WriteSeries([]*influxdb.Series{series}); err != nil {
			log.Println("Error writing series:", err.Error())
			if errPort != nil {
				errPort.SendMessage(runtime.NewPacket([]byte(err.Error())))
			}
			continue
		}
	}
}
