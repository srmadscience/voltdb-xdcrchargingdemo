SECS=$1
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.14 100 0 1 TRANSACTIONS 100 $SECS 5 1000 100 &
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.16 100 0 1 TRANSACTIONS 100 $SECS 5 1000 100
wait
