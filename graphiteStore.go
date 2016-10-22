
package main

import (
	"github.com/marpaia/graphite-golang"
	//"github.com/gr3yw0lf/graphite-golang"
	"log"
	"time"
	"sync"
)


//////////////////////////////////////////////
// GraphiteStore Object
//
type GraphiteStore struct {
	topics map[string][]graphite.Metric
	lastSeen time.Time
	lock *sync.Mutex
}

// create a new MetricQueue 
func NewGraphiteStore() (*GraphiteStore) {
	store := &GraphiteStore{
		topics: make(map[string][]graphite.Metric),
		lock: &sync.Mutex{},
	}
	return store
}

// add into the GraphiteStore
func (store *GraphiteStore) AddTopic(topicString string, topicMetrics []graphite.Metric) {
	store.lock.Lock()
	store.topics[topicString] = topicMetrics
	store.lastSeen = time.Unix(topicMetrics[0].Timestamp,0)
	store.lock.Unlock()
	if Debug {
			log.Printf("+ %s\n",topicString)
	}
}

// return all the valid metrics from the map of topics and
//  the count of the amount of topics processed
func (store *GraphiteStore) GetAll() ([]graphite.Metric, int64) {

	allMetrics := make([]graphite.Metric,0)
	var count int64

	// collect up all the individual metrics
	store.lock.Lock()
	for key, item := range store.topics {
		// check if metric is stale, and exclude them 
		//  (only check one of the timstamps in the metric, as all metrics should have the same timestamp)
		lastSeen := time.Unix(item[0].Timestamp,0)
		maxValidity := lastSeen.Add(time.Second*DEFAULT_MAXAGE)
		if maxValidity.Before(time.Now()) {
			log.Printf("%s: Max Validity in the past, now=%+v, maxValidity=%+v\n", key, time.Now(), maxValidity)
			delete(store.topics,key)
		} else {
			// compile all metrics within the topic
			for _, metric := range item {
				if Debug {
					log.Printf("metric = %+v\n", metric)
				}
				allMetrics = append(allMetrics, metric)
			}
		}
		if Debug {
			log.Printf("maxValidity = %v\n", maxValidity)
		}
		count++
	}
	store.lock.Unlock()
	return allMetrics, count
}



