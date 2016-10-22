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
	"gopkg.in/natefinch/lumberjack.v2"
	"os/signal"
	"syscall"
)

const (
		DEFAULT_DEBUG = false
		DEFAULT_MQTT_SERVER = "tcp://172.17.0.12:1883"
		DEFAULT_MQTT_QOS	= 1
		DEFAULT_MQTT_SUBSCRIPTION = "collectd/#"
		DEFAULT_GRAPHITE_SEND = 10
		DEFAULT_GRAPHITE_PREFIX = ""
		DEFAULT_GRAPHITE_SERVER = "172.17.0.5:2003"
		DEFAULT_TYPESDB = "/usr/share/collectd/types.db"
		DEFAULT_MAXAGE = 120	// two minutes
		PROGRAM = "mqttToGraphite_v1"
		DEFAULT_LOGFILE = "./mqttToGraphite.log"
		DEFAULT_LOG_COUNTS = false
)

var Debug bool
var logger *log.Logger

////////////////
// Main
func main() {
	var mqttServerURL string
	var qos int
	var requireCerts bool
	var logCounts bool
	var logFile string
	var graphiteServer string
	var graphitePrefix string
	var subscribeTopic string
	runChannel := make(chan os.Signal, 1)
	signal.Notify(runChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-runChannel
		logger.Println("signal received, exiting")
		os.Exit(0)
	}()

	flag.StringVar(&mqttServerURL, "mqtt-server", DEFAULT_MQTT_SERVER, "mqtt server method, address, and port")
	flag.StringVar(&logFile, "logfile", DEFAULT_LOGFILE, "log file to log details to")
	flag.StringVar(&subscribeTopic, "mqtt-subscription", DEFAULT_MQTT_SUBSCRIPTION, "mqtt server subscription (eg: collectd/+/load/#)")
	flag.IntVar(&qos, "qos", DEFAULT_MQTT_QOS, "mqtt qos level")
	flag.BoolVar(&requireCerts, "tls", false, "TLS Certificates required")
	flag.BoolVar(&Debug, "debug", DEFAULT_DEBUG, "Debug messages")
	flag.BoolVar(&logCounts, "log-counts", DEFAULT_LOG_COUNTS, "Log the sever counts")
	flag.StringVar(&graphiteServer, "graphite-server", DEFAULT_GRAPHITE_SERVER, "Graphite Server address, and port")
	flag.StringVar(&graphitePrefix, "graphite-prefix", DEFAULT_GRAPHITE_PREFIX, "prefix to use when sending to graphite")
	typesDbFile := flag.String("typesdb", DEFAULT_TYPESDB, "The location of the collectd types.db file")
	flag.Parse()

	// Log to a rotating file
	e, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	logger = log.New(e, "" ,log.LstdFlags|log.Lshortfile)
	logger.SetOutput(&lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    1,  // megabytes after which new file is created
		MaxBackups: 3,  // number of backups
		MaxAge:     30, //days
	})

	logger.Printf("Start:%s pid:%d", PROGRAM, os.Getpid())
	clientId := fmt.Sprintf("%s_%d", PROGRAM, os.Getpid())

	logger.Printf("Connecting to mqtt: %s\n", mqttServerURL)

	mqttOpts := mqtt.NewClientOptions().AddBroker(mqttServerURL).SetClientID(clientId).SetCleanSession(true)

	if requireCerts {
			certFile := "./client1.crt"
			keyFile := "./client1.key"
			cacertFile := "./ca.crt"
			cert, err := tls.LoadX509KeyPair(certFile,keyFile)
			if err != nil {
				logger.Printf("cert or key loading failure\n")
				logger.Fatal(err)
			}
					
			validationCert, err := ioutil.ReadFile(cacertFile)
			if err != nil {
				logger.Println("Error loading validation certificate. ",err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(validationCert) {
				logger.Println("Error installing validation certificate.")
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
		logger.Fatalf("Can't open types.db file %s", typesDbFile)
	}

	typesDB, err := api.NewTypesDB(file)
	if err != nil {
		logger.Fatalf("Error in parsing types.db file %s", typesDbFile)
	}
	file.Close()

	///////////////////////
	// connect to the graphite server (carbon port)

	logger.Printf("Connecting to graphite: %s\n", graphiteServer)
	graphiteConnection := strings.Split(graphiteServer,":")
	graphitePort, _ := strconv.ParseInt(graphiteConnection[1],10,32)
	graphite, _ := graphite.NewGraphite(graphiteConnection[0],int(graphitePort))
	// TODO: Err check this connection
	logger.Printf("Loaded Graphite connection: %#v", graphite)


	// create the graphiteStore
	//
	graphiteStore := NewGraphiteStore()

	// create the server shared items
	//
	server := &MqttToGraphite{
			graphiteServer: graphite,
			typesDB: typesDB,
			mqttClient: client,
			debug: Debug,
			count: 0,
			graphiteStore: graphiteStore,
			graphitePrefix: graphitePrefix,
	}

	// mqtt subscription (will start to process messages sat in the server)
	//
	logger.Printf("Subscribing to %s\n", subscribeTopic)
	if token := client.Subscribe(subscribeTopic, byte(qos), server.MessageHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// process values periodically
	//
	processTimer := time.NewTicker(time.Second * DEFAULT_GRAPHITE_SEND).C
	for {
		select {
		case <-processTimer:
			//server.count = 0
			server.Send()
			server.StatsToMqtt()
			if logCounts {
				logger.Printf("total Count sent: %d", server.count)
			}
		}
	}

	logger.Println("end")
}

// end of Main
/////////////////

//////////////////////////////////////////////
// MqttToGraphite Server Object
//
type MqttToGraphite struct {
	graphiteServer *graphite.Graphite
	typesDB *api.TypesDB
	mqttClient mqtt.Client
	debug bool
	count int64
	graphiteStore *GraphiteStore
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
		logger.Printf("Payload didnt end with a zero byte\n")
		payloadStrings = string(message.Payload()[:])
	}
	payload := strings.Split(payloadStrings, ":")
	if len(payload) <2 {
		logger.Printf("payload error: length < 2 for %s. Ignoring\n", strings.Join(topics,"."))
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
			logger.Printf("Not in TypesDB: %s\n", strings.Join(topics,"."))
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
	
	g.graphiteStore.AddTopic(topic,metrics)
}

// Send all metrics of all topics to graphite
func (g *MqttToGraphite) Send() {

	allMetrics, count := g.graphiteStore.GetAll()
	g.count = g.count +int64(len(allMetrics))

	if g.debug {
		logger.Printf("topic count = %d, len allMetrics = %d\n", count, len(allMetrics))
	}

	// no point trying to send if there are no items
	if len(allMetrics) > 0 {
		g.graphiteServer.SendMetrics(allMetrics)
	}

}

// send stats to local mqtt
func (g *MqttToGraphite) StatsToMqtt() {
	topicPrefix := "mqttToGraphite"

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	g.SendStats(topicPrefix, hostname, "count", fmt.Sprintf("%d",g.count))
	g.SendStats(topicPrefix, hostname, "lastUpdate", fmt.Sprintf("%d",time.Now().Unix()))
}

func (g *MqttToGraphite) SendStats(prefix string, host string, suffix string, message string) {
	topic := strings.Join([]string{prefix, host, suffix},"/")
	qos := 1
	if token := g.mqttClient.Publish(topic,byte(qos), true, message); token.Wait() && token.Error() != nil {
		logger.Printf("mqtt publish error to %s:%v\n", topic, token.Error())
	}
}

