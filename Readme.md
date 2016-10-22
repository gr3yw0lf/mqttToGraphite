# mqttToGraphite

Go program to collect details from a mosquitto server and pass them to a graphite server.
Specifically made for collectd mqtt plugin

### help:

$ ./mqttToGraphite --help

```Usage of ./mqttToGraphite:
  -debug
        Debug messages
  -graphite-prefix string
        prefix to use when sending to graphite
  -graphite-server string
        Graphite Server address, and port (default "172.17.0.5:2003")
  -log-counts
        Log the sever counts
  -logfile string
        log file to log details to (default "./mqttToGraphite.log")
  -mqtt-server string
        mqtt server method, address, and port (default "tcp://172.17.0.12:1883")
  -mqtt-subscription string
        mqtt server subscription (eg: collectd/+/load/#) (default "collectd/#")
  -qos int
        mqtt qos level (default 1)
  -tls
        TLS Certificates required
  -typesdb string
        The location of the collectd types.db file (default "/usr/share/collectd/types.db")
```
### Example run:

$ ./mqttToGraphite -debug -graphite-prefix "test." -mqtt-subscription "collectd/+/load/#" -logfile ./test.log



