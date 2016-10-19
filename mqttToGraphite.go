// mqttToGraphite
//  take data from a mqtt server as a subscriber, and place them in graphite

package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/marpaia/graphite-golang"
	//"github.com/gr3yw0lf/graphite-golang"
	"log"
	"bytes"
	"flag"
	"time"
	"crypto/tls"
	"io/ioutil"
	"crypto/x509"
	"os"
	"strings"
	"strconv"
	"collectd.org/api"
)

////////////////
// Main
func main() {

	var mqttServerURL string
	var qos int
	var requireCerts bool
	var graphiteServer string

	flag.StringVar(&mqttServerURL, "host", "tcp://172.17.0.12:1883", "server address")
	flag.IntVar(&qos, "qos", 1, "qos level")
	flag.BoolVar(&requireCerts, "tls", false, "TLS Certificates required")
	flag.StringVar(&graphiteServer, "g", "172.17.0.5", "Graphite Server")
	typesDbFile := flag.String("t", "/usr/share/collectd/types.db", "The location of the collectd types.db file")
	flag.Parse()

	log.Printf("Start: pid:%d", os.Getpid())
	clientId := fmt.Sprintf("mqtt-client_%d", os.Getpid())

	log.Printf("Connecting to mqtt: %s\n", mqttServerURL)

	mqttOpts := mqtt.NewClientOptions().AddBroker(mqttServerURL).SetClientID(clientId).SetCleanSession(true)

	if requireCerts {
			certFile := "./client1.crt"
			keyFile := "./client1.key"
			cacertFile := "./ca.crt"
			cert, err := tls.LoadX509KeyPair(certFile,keyFile)
			if err != nil {
				fmt.Printf("cert or key loading failure\n")
				log.Fatal(err)
			}
					
			validationCert, err := ioutil.ReadFile(cacertFile)
			if err != nil {
				fmt.Println("Error loading validation certificate. ",err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(validationCert) {
				fmt.Println("Error installing validation certificate.")
			}
			tlsConfig := &tls.Config{
				Certificates:   []tls.Certificate{cert},
				RootCAs:        pool,
			}
			mqttOpts.SetTLSConfig(tlsConfig)
	}
	// mqttOpts := mqtt.NewClientOptions().AddBroker(mqttServerURL).SetClientID(clientId).SetCleanSession(true).SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(mqttOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// ///////////
	// Load the typesDB

	file, err := os.Open(*typesDbFile)
	if err != nil {
		log.Fatalf("Can't open types.db file %s", typesDbFile)
	}

	typesDB, err := api.NewTypesDB(file)
	if err != nil {
		log.Fatalf("Error in parsing types.db file %s", typesDbFile)
	}
	file.Close()

	///////////////////////
	// connect to the graphite server (carbon port)

	log.Printf("Connecting to graphite: %s\n", graphiteServer)
	graphite, _ := graphite.NewGraphite(graphiteServer, 2003)
	// TODO: Err check this connection
	log.Printf("Loaded Graphite connection: %#v", graphite)

	// create the context
	//
	context := &GraphiteContext{
			graphiteServer: graphite,
			typesDB: typesDB,
			mqttClient: client,
	}

	// subscribe to the mqtt messages

	subscribeTopic := "collectd/#"
	//subscribeTopic := "collectd/oak.tree.local/load/#"
	//subscribeTopic := "collectd/oak.tree.local/memory/#"

	fmt.Printf("Subscribing to %s\n", subscribeTopic)
	// pass an instance to the context pointer
	if token := client.Subscribe(subscribeTopic, byte(qos), context.MessageHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	for {
		//log.Println("waiting for signal")
		time.Sleep(30 * time.Second)
	}

	log.Println("end")
}

// end of Main
/////////////////

type GraphiteContext struct {
	graphiteServer *graphite.Graphite
	typesDB *api.TypesDB
	mqttClient mqtt.Client
}

// building off of: https://elithrar.github.io/article/custom-handlers-avoiding-globals/

// type MessageHandler func(Client, Message)
func (g GraphiteContext) MessageHandler(client mqtt.Client, message mqtt.Message) {
	//fmt.Printf("Received message on topic: %+v Message: %s\n", message.Topic(), message.Payload())
	// make sure all host names have .'s replaced
	topic := strings.Replace(message.Topic(), ".", "_", -1)

	//>>> topic,value: collectd/oak_tree_local/interface-lo/if_octets  payload: =1476648925.261:638.098817688229:638.098817688229
	topics := strings.Split(topic, "/")
	var payloadStrings string
	n := bytes.IndexByte(message.Payload(), 0)
	if n > 0 {
		// kill the damn zero byte at the end
		payloadStrings = string(message.Payload()[:n])
	} else {
		payloadStrings = string(message.Payload()[:])
	}
	payload := strings.Split(payloadStrings, ":")
	if len(payload) <2 {
		fmt.Printf("payload error: length < 2 for %s\n", strings.Join(topics,"."))
		return
	}
	timestamp, _ := strconv.ParseFloat(payload[0],64)
	metrics := make([]graphite.Metric,0)

	var dataSet *api.DataSet
	dataSet, found := g.typesDB.DataSet(topics[len(topics)-1])
	//&{Name:if_packets Sources:[{Name:rx Type:api.Derive Min:0 Max:NaN} {Name:tx Type:api.Derive Min:0 Max:NaN}]} 
	if found {
		//fmt.Printf("%+v\n", dataSet)
		for i, source := range dataSet.Sources {
			//fmt.Printf(">>%d %s\n",i, source.Name)
			metric :=  graphite.NewMetric(
				fmt.Sprintf("%s.%s",
					strings.Join(topics,"."),
					source.Name,
				),
				payload[i+1],
				int64(timestamp),
			)
			metrics = append(metrics,metric)
		}
	} else {
		// Not found in typesDB
		if len(payload) > 2 {
			fmt.Printf("Not in TypesDB: %s\n", strings.Join(topics,"."))
			return
		}

		// should be a single metric
		metric :=  graphite.NewMetric(
			fmt.Sprintf("%s",
				strings.Join(topics,"."),
			),
			payload[1],
			int64(timestamp),
		)
		metrics = append(metrics,metric)
	}
	
	//fmt.Printf(">>> %+v\n", metrics)
	g.graphiteServer.SendMetrics(metrics)
}


