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
	"sync"
)

const (
		DEFAULT_DEBUG = false
		DEFAULT_QOS	= 1
		DEFAULT_SUBSCRIPTION = "collectd/+/load/#"
		DEFAULT_MAXAGE = 40
		DEFAULT_GRAPHITE_SEND = 10
		DEFAULT_PREFIX = "test."
		DEFAULT_MQTT_SERVER = "tcp://172.17.0.12:1883"
		DEFAULT_GRAPHITE_SERVER = "172.17.0.5:2003"
		DEFAULT_TYPESDB = "/usr/share/collectd/types.db"
)

var debug bool

////////////////
// Main
func main() {

	log.SetFlags(log.Lshortfile | log.LstdFlags )
	var mqttServerURL string
	var qos int
	var requireCerts bool
	var graphiteServer string

	flag.StringVar(&mqttServerURL, "mqtt-server", DEFAULT_MQTT_SERVER, "mqtt server method, address, and port")
	flag.IntVar(&qos, "qos", DEFAULT_QOS, "mqtt qos level")
	flag.BoolVar(&requireCerts, "tls", false, "TLS Certificates required")
	flag.BoolVar(&debug, "debug", DEFAULT_DEBUG, "Debug messages")
	flag.StringVar(&graphiteServer, "graphite-server", DEFAULT_GRAPHITE_SERVER, "Graphite Server address, and port")
	typesDbFile := flag.String("typesdb", DEFAULT_TYPESDB, "The location of the collectd types.db file")
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
	graphiteConnection := strings.Split(graphiteServer,":")
	graphitePort, _ := strconv.ParseInt(graphiteConnection[1],10,32)
	graphite, _ := graphite.NewGraphite(graphiteConnection[0],int(graphitePort))
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
			graphitePrefix: DEFAULT_PREFIX,
	}

	// subscribe to the mqtt messages

	subscribeTopic := DEFAULT_SUBSCRIPTION

	log.Printf("Subscribing to %s\n", subscribeTopic)
	// pass an instance to the context pointer
	if token := client.Subscribe(subscribeTopic, byte(qos), server.MessageHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// process values periodically
	processTimer := time.NewTicker(time.Second * DEFAULT_GRAPHITE_SEND).C
	for {
		select {
		case <-processTimer:
			//server.count = 0
			server.Send()
			if debug {
				log.Printf("total Count sent: %d", server.count)
			}
		}
	}

	log.Println("end")
}

// end of Main
/////////////////

//////////////////////////////////////////////
// MetricQueue Object
//
type MetricQueue struct {
	topics map[string][]graphite.Metric
	lastSeen time.Time
	lock *sync.Mutex
}

// create a new MetricQueue 
func NewMetricQueue() (*MetricQueue) {
	mq := &MetricQueue{
		topics: make(map[string][]graphite.Metric),
		lock: &sync.Mutex{},
	}
	return mq
}

// add into the MetricQueue
func (mq *MetricQueue) AddTopic(topicString string, topicMetrics []graphite.Metric) {
	mq.lock.Lock()
	mq.topics[topicString] = topicMetrics
	mq.lastSeen = time.Unix(topicMetrics[0].Timestamp,0)
	mq.lock.Unlock()
	if debug {
			log.Printf("+ %s\n",topicString)
	}
}

// return all the valid metrics from the map of topics and
//  the count of the amount of topics processed
func (mq *MetricQueue) GetAll() ([]graphite.Metric, int64) {

	allMetrics := make([]graphite.Metric,0)
	var count int64

	// collect up all the individual metrics
	mq.lock.Lock()
	for key, item := range mq.topics {
		// check if metric is stale, and exclude them 
		//  (only check one of the timstamps in the metric, as all metrics should have the same timestamp)
		lastSeen := time.Unix(item[0].Timestamp,0)
		maxValidity := lastSeen.Add(time.Second*DEFAULT_MAXAGE)
		if maxValidity.Before(time.Now()) {
			log.Printf("%s: Max Validity in the past, now=%+v, maxValidity=%+v\n", key, time.Now(), maxValidity)
			delete(mq.topics,key)
		} else {
			// compile all metrics within the topic
			for _, metric := range item {
				if debug {
					log.Printf("metric = %+v\n", metric)
				}
				allMetrics = append(allMetrics, metric)
			}
		}
		if debug {
			log.Printf("maxValidity = %v\n", maxValidity)
		}
		count++
	}
	mq.lock.Unlock()
	return allMetrics, count
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
	graphitePrefix string
}

// MessageHandler - required callback for the mqtt subscribe function
//  from: github.com/eclipse/paho.mqtt.golang : type MessageHandler func(Client, Message)
//  
func (g *MqttToGraphite) MessageHandler(client mqtt.Client, message mqtt.Message) {

	// make sure all host names have .'s replaced
	topic := strings.Replace(message.Topic(), ".", "_", -1)

	//>>> topic,value: collectd/oak_tree_local/interface-lo/if_octets  payload: =1476648925.261:638.098817688229:638.098817688229
	topics := strings.Split(topic, "/")

	var payloadStrings string
	// cope with payload being zero byte terminated (the way collectd adds into mqtt)
	n := bytes.IndexByte(message.Payload(), 0)
	if n > 0 {
		// kill it
		payloadStrings = string(message.Payload()[:n])
	} else {
		log.Printf("Payload didnt end with a zero byte\n")
		payloadStrings = string(message.Payload()[:])
	}
	payload := strings.Split(payloadStrings, ":")
	if len(payload) <2 {
		log.Printf("payload error: length < 2 for %s. Ignoring\n", strings.Join(topics,"."))
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
					g.graphitePrefix,
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
			log.Printf("Not in TypesDB: %s\n", strings.Join(topics,"."))
			return
		}

		// should be a single metric
		metric :=  graphite.NewMetric(
			fmt.Sprintf("%s%s",
				g.graphitePrefix,
				strings.Join(topics,"."),
			),
			payload[1],
			int64(timestamp),
		)
		metrics = append(metrics,metric)
	}
	
	g.queue.AddTopic(topic,metrics)
}

// Send all metrics of all topics to graphite
func (g *MqttToGraphite) Send() {

	allMetrics, count := g.queue.GetAll()
	g.count = g.count +int64(len(allMetrics))

	if debug {
		log.Printf("topic count = %d, len allMetrics = %d\n", count, len(allMetrics))
	}

	// no point trying to send if there are no items
	if len(allMetrics) > 0 {
		g.graphiteServer.SendMetrics(allMetrics)
	}

}

