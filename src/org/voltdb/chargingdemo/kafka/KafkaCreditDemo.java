package org.voltdb.chargingdemo.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaCreditDemo {

	public static void main(String[] args) throws UnknownHostException {

		msg("Parameters:" + Arrays.toString(args));

		if (args.length != 6) {
			msg("Usage: kafkaserverplusport recordcount offset tpms durationseconds maxamount");
			System.exit(1);
		}

		String kafkaserverplusport = args[0];
		int recordCount = 0;
		int offset = 0;
		int tpms = 0;
		int durationseconds = 0;
		int maxamount = 0;
		Random r = new Random();

		try {
			recordCount = Integer.parseInt(args[1]);
			offset = Integer.parseInt(args[2]);
			tpms = Integer.parseInt(args[3]);
			durationseconds = Integer.parseInt(args[4]);
			maxamount = Integer.parseInt(args[5]);

		} catch (NumberFormatException e) {
			msg("Value should be a number:" + e.getMessage());
			System.exit(1);
		}
		
		
		Properties config = new Properties();
		config.put("client.id", InetAddress.getLocalHost().getHostName());
		config.put("bootstrap.servers", kafkaserverplusport);
		config.put("acks", "all");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String> (config);

		long endtimeMs = System.currentTimeMillis() + (1000 * durationseconds);
		// How many transactions we've done...
		
		int tranCount = 0;

		int tpThisMs = 0;
		long currentMs = System.currentTimeMillis();
		
		while (endtimeMs > System.currentTimeMillis()) {
			
			tranCount++;

			if (tpThisMs++ > tpms) {

				while (currentMs == System.currentTimeMillis()) {
					try {
						Thread.sleep(0, 50000);
					} catch (InterruptedException e) {
					}

				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}
			
			int userId = r.nextInt(recordCount) + offset;
			int amount = r.nextInt(maxamount);
			String txnId = "Kafka_" + tranCount + "_" + currentMs;
			String request = "\"" + userId + "\",\"" + amount + "\",\"" +txnId +  "\"" ;
			
			
			
			ProducerRecord<String, String> newrec = new ProducerRecord<String, String>("ADDCREDIT",
					request);
			
			producer.send(newrec);

		}
		
		producer.close();
		
	}

	/**
	 * Print a formatted message.
	 * 
	 * @param message
	 */
	public static void msg(String message) {

		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = new Date();
		String strDate = sdfDate.format(now);
		System.out.println(strDate + ":" + message);

	}

}
