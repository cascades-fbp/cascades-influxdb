package main

import (
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/cascades-fbp/cascades/components/utils"
	"github.com/cascades-fbp/cascades/runtime"
	influxdb "github.com/influxdb/influxdb/client"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	// Flags
	inputEndpoint   = flag.String("port.in", "", "Component's input port endpoint")
	optionsEndpoint = flag.String("port.options", "", "Component's input port endpoint")
	errorEndpoint   = flag.String("port.err", "", "Component's output port endpoint")
	jsonFlag        = flag.Bool("json", false, "Print component documentation in JSON")
	debug           = flag.Bool("debug", false, "Enable debug mode")

	// Internal
	context                      *zmq.Context
	inPort, optionsPort, errPort *zmq.Socket
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
	context, err = zmq.NewContext()
	utils.AssertError(err)

	optionsPort, err = utils.CreateInputPort(context, *optionsEndpoint)
	utils.AssertError(err)

	inPort, err = utils.CreateInputPort(context, *inputEndpoint)
	utils.AssertError(err)

	if *errorEndpoint != "" {
		errPort, err = utils.CreateOutputPort(context, *errorEndpoint)
		utils.AssertError(err)
	}
}

func closePorts() {
	optionsPort.Close()
	inPort.Close()
	if errPort != nil {
		errPort.Close()
	}
	context.Close()
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

	openPorts()
	defer closePorts()

	ch := utils.HandleInterruption()
	err = runtime.SetupShutdownByDisconnect(context, inPort, "influx-write.in", ch)
	utils.AssertError(err)

	log.Println("Waiting for options to arrive...")
	var (
		influxHost, influxUsername, influxPassword, influxDB, tmpStr string
		parts, kv                                                    []string
		ip                                                           [][]byte
	)
	for {
		ip, err = optionsPort.RecvMultipart(0)
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
		ip, err = inPort.RecvMultipart(0)
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
				errPort.SendMultipart(runtime.NewPacket([]byte(err.Error())), zmq.NOBLOCK)
			}
			continue
		}
	}
}
