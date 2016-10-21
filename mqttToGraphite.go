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

const (
		DEFAULT_DEBUG = true
		DEFAULT_QOS	= 1
		DEFAULT_SUBSCRIPTION = "collectd/+/load/#"
		DEFAULT_MAXAGE = 100
)

var debug bool

////////////////
// Main
func main() {

	var mqttServerURL string
	var qos int
	var requireCerts bool
	var graphiteServer string

	flag.StringVar(&mqttServerURL, "host", "tcp://172.17.0.12:1883", "server address")
	flag.IntVar(&qos, "qos", DEFAULT_QOS, "qos level")
	flag.BoolVar(&requireCerts, "tls", false, "TLS Certificates required")
	flag.BoolVar(&debug, "debug", DEFAULT_DEBUG, "Debug messages")
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


	// create the metricQueue
	metricQueue := NewMetricQueue()

	// create the server shared items
	//
	server := &MqttToGraphite{
			graphiteServer: graphite,
			typesDB: typesDB,
			mqttClient: client,
			debug: debug,
			count: 0,
			queue: metricQueue,
	}

	// subscribe to the mqtt messages

	subscribeTopic := DEFAULT_SUBSCRIPTION
	//subscribeTopic := "collectd/+/load/#"
	//subscribeTopic := "collectd/oak.tree.local/memory/#"

	fmt.Printf("Subscribing to %s\n", subscribeTopic)
	// pass an instance to the context pointer
	if token := client.Subscribe(subscribeTopic, byte(qos), server.MessageHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// process values periodically
	processTimer := time.NewTicker(time.Second * 10).C
	for {
		select {
		case <-processTimer:
			server.count = 0
			server.Send()
			log.Printf("Count: %d", server.count)
		}
	}

	log.Println("end")
}

// end of Main
/////////////////


//////////////////////////////////////////////
// GraphiteItem Object
//
type GraphiteItem struct {
	topic string
	metrics []graphite.Metric
}
func NewGraphiteItem(topicString string, topicMetrics []graphite.Metric) (*GraphiteItem) {
	item := &GraphiteItem{
		topic: topicString,
		metrics: topicMetrics,
	}
	return item
}

//////////////////////////////////////////////
// MetricQueue Object
//
type MetricQueue struct {
	topics map[string]*GraphiteItem
	lastSent int64
}

func NewMetricQueue() (*MetricQueue) {
	mq := &MetricQueue{
		topics: make(map[string]*GraphiteItem),
		lastSent: 0,
	}
	return mq
}

func (mq *MetricQueue) AddTopic(topicString string, topicMetrics []graphite.Metric) {
	if _, ok := mq.topics[topicString]; !ok {
		// debug:
		if debug {
				log.Printf("metricQueue.AddTopic creating %s\n", topicString)
		}
		// create the graphiteItem
		newItem := NewGraphiteItem(topicString,topicMetrics)
		mq.topics[topicString] = newItem
	} else {
		// debug:
		if debug {
				log.Printf("metricQueue.AddTopic found %s\n", topicString)
		}
		mq.topics[topicString].metrics = topicMetrics
	}
	// debug:
	if debug {
			fmt.Printf(">>>> mq: %+v\n ",mq)
	}
}

//////////////////////////////////////////////
// MqttToGraphite Server Object
//
type MqttToGraphite struct {
	graphiteServer *graphite.Graphite
	typesDB *api.TypesDB
	mqttClient mqtt.Client
	debug bool
	count int64
	queue *MetricQueue
}

// MessageHandler - required callback for the mqtt subscribe function
//  from: github.com/eclipse/paho.mqtt.golang : type MessageHandler func(Client, Message)
//  
func (g *MqttToGraphite) MessageHandler(client mqtt.Client, message mqtt.Message) {
	//fmt.Printf("Received message on topic: %+v Message: %s\n", message.Topic(), message.Payload())

	// make sure all host names have .'s replaced
	topic := strings.Replace(message.Topic(), ".", "_", -1)

	topicPrefix := "test."
	//>>> topic,value: collectd/oak_tree_local/interface-lo/if_octets  payload: =1476648925.261:638.098817688229:638.098817688229
	topics := strings.Split(topic, "/")
	var payloadStrings string
	n := bytes.IndexByte(message.Payload(), 0)
	if n > 0 {
		// kill the damn zero byte at the end
		payloadStrings = string(message.Payload()[:n])
	} else {
		fmt.Printf("Payload didnt end with a zero byte\n")
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
				fmt.Sprintf("%s%s.%s",
					topicPrefix,
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
			fmt.Sprintf("%s%s",
				topicPrefix,
				strings.Join(topics,"."),
			),
			payload[1],
			int64(timestamp),
		)
		metrics = append(metrics,metric)
	}
	
	g.queue.AddTopic(topic,metrics)
	//fmt.Printf(">>> %+v\n", metrics)
	//g.graphiteServer.SendMetrics(metrics)
}

func (g *MqttToGraphite) Send() {
	// debug:
	if debug {
			fmt.Printf("mq.Send: topic count = %d\n", len(g.queue.topics))
	}

	allMetrics := make([]graphite.Metric,0)

	// collect up all the individual metrics
	for _, item := range g.queue.topics {
		// find a list of items that are stale, and exclude them from the sending
		metricTime := time.Unix(item.metrics[0].Timestamp,0)
		maxValidity := metricTime.Add(time.Second*DEFAULT_MAXAGE)
		if maxValidity.Before(time.Now()) {
			fmt.Printf("Max Validity in the past, now=%+v, maxValidity=%+v\n", time.Now(), maxValidity)
			fmt.Printf("delete %+v\n", item)
			delete(g.queue.topics,item.topic)
		}
		for _, metric := range item.metrics {
			if debug {
				fmt.Printf("metric = %+v\n", metric)
			}
			allMetrics = append(allMetrics, metric)
		}
	}
	// debug:
	if debug {
			fmt.Printf("len allMetrics = %d, allMetrics = %p\n", len(allMetrics), allMetrics)
	}
	g.count = g.count +int64(len(allMetrics))
	g.graphiteServer.SendMetrics(allMetrics)

}

