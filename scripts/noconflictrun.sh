SECS=$1
USERCOUNT=100
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.14 $USERCOUNT 0          1 TRANSACTIONS 100 $SECS 5 1000 10 
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.16 $USERCOUNT $USERCOUNT 1 TRANSACTIONS 100 $SECS 5 1000 10 &
wait
