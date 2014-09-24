package main

import (
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/cascades-fbp/cascades/runtime"
	influxdb "github.com/influxdb/influxdb/client"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	// Flags
	inputEndpoint   = flag.String("port.in", "", "Component's input port endpoint")
	optionsEndpoint = flag.String("port.options", "", "Component's input port endpoint")
	outputEndpoint  = flag.String("port.err", "", "Component's output port endpoint")
	jsonFlag        = flag.Bool("json", false, "Print component documentation in JSON")
	debug           = flag.Bool("debug", false, "Enable debug mode")

	// Internal vars
	err           error
	context       *zmq.Context
	receiver      *zmq.Socket
	optionsSocket *zmq.Socket
	sender        *zmq.Socket
)

func main() {
	flag.Parse()

	if *jsonFlag {
		doc, _ := registryEntry.JSON()
		fmt.Println(string(doc))
		os.Exit(0)
	}

	if *inputEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *optionsEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.SetFlags(0)
	if *debug {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	context, _ = zmq.NewContext()
	defer context.Close()

	//  Socket to receive messages on
	receiver, err = context.NewSocket(zmq.PULL)
	if err != nil {
		fmt.Println("Error creating socket:", err.Error())
		os.Exit(1)
	}
	defer receiver.Close()
	err = receiver.Bind(*inputEndpoint)
	if err != nil {
		fmt.Println("Error binding socket:", err.Error())
		os.Exit(1)
	}

	// Options port socket
	optionsSocket, err = context.NewSocket(zmq.PULL)
	if err != nil {
		fmt.Println("Error creating socket:", err.Error())
		os.Exit(1)
	}
	defer optionsSocket.Close()
	err = optionsSocket.Bind(*optionsEndpoint)
	if err != nil {
		fmt.Println("Error binding socket:", err.Error())
		os.Exit(1)
	}

	//  Socket to send errors to (optional)
	if *outputEndpoint != "" {
		sender, err = context.NewSocket(zmq.PUSH)
		if err != nil {
			fmt.Println("Error creating socket:", err.Error())
			os.Exit(1)
		}
		defer sender.Close()
		err = sender.Connect(*outputEndpoint)
		if err != nil {
			fmt.Println("Error connecting to socket:", err.Error())
			os.Exit(1)
		}
	}

	// Ctrl+C handling
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		for _ = range ch {
			log.Println("Give 0MQ time to deliver before stopping...")
			time.Sleep(1e9)
			log.Println("Stopped")
			os.Exit(0)
		}
	}()

	// Monitoring setup
	err = runtime.SetupShutdownByDisconnect(context, receiver, "influx-write.in", ch)
	if err != nil {
		log.Println("Failed to setup monitoring:", err.Error())
		os.Exit(1)
	}

	log.Println("Waiting for options to arrive...")
	var (
		influxHost, influxUsername, influxPassword, influxDB, tmpStr string
		parts, kv                                                    []string
		ip                                                           [][]byte
	)
	for {
		ip, err = optionsSocket.RecvMultipart(0)
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
		break
	}

	var client *influxdb.Client
	client, err := influxdb.NewClient(&influxdb.ClientConfig{
		Host:     influxHost,
		Username: influxUsername,
		Password: influxPassword,
		Database: influxDB,
	})
	if err != nil {
		fmt.Println("Error connecting to influxDB:", err.Error())
		os.Exit(1)
	}

	log.Println("Starting to handle data...")
	var series *influxdb.Series
	for {
		ip, err = receiver.RecvMultipart(0)
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
			if sender != nil {
				sender.SendMultipart(runtime.NewPacket([]byte(err.Error())), zmq.NOBLOCK)
			}
			continue
		}
	}
}
