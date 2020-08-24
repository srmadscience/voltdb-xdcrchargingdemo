#!/bin/sh

#
# Create DB schema
#
#for i in 14 15 16; do sqlcmd --servers=192.168.0.$i < db.sql ; done
for i in 14 15 16; do sqlcmd --servers=192.168.0.$i < classes.sql ; done

#
# Enable tasks
#
#for i in 14 15 16; do sqlcmd --servers=192.168.0.$i < euc.sql ; done
#for i in 14 15 16; do sqlcmd --servers=192.168.0.$i < ccb.sql ; done

#
# Create 300K usees
#
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.16 300000 0 1 USERS 100 300 10 50000 10

sleep 5


SECS=1800
USERCOUNT=100000
#
# Run 1K TPS on 100,000 transactions
java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.14 $USERCOUNT 0          1 TRANSACTIONS 100 $SECS 5 10000 5 > 14.lst &

# If you uncomment this line a different set of users will be run on 16. It will crash as well...
#java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.16 $USERCOUNT $USERCOUNT 1 TRANSACTIONS 100 $SECS 5 1000 5 > 16.lst &
#java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.15 $USERCOUNT 0 1 TRANSACTIONS 100 $SECS 5 10000 5 > 15.lst &
#java -jar ../jars/voltdb-chargingdemo-client.jar 192.168.0.16 $USERCOUNT 0 1 TRANSACTIONS 100 $SECS 5 10000 5 > 16.lst &

# sleep 20
# 
# CT=1
# 
# while 
# 	[ "$CT" -lt 5 ]
# do
# 	#
# 	# Reload same JAR file we used earlier
# 	#
# 	for i in 14 15 16; do sqlcmd --servers=192.168.0.$i < classes.sql ; done
# 	sleep 5
# 	CT=`expr $CT + 1`
# done

wait
echo 14
cat 14.lst

echo 16
cat 16.lst

