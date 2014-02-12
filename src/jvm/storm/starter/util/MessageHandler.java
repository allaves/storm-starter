package storm.starter.util;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/*
 * Singleton handler to manage the messaging between bolts and RabbitMQ
 */
public class MessageHandler {
	 private final static String EXCHANGE = "HSLTracking";
	 
	 private static Logger log = Logger.getLogger(MessageHandler.class);
	 
	 private static MessageHandler instance;
	 
	 private static Connection connection;
	 
	 private MessageHandler() {}
	 
	 public static MessageHandler getMessageHandler() {
		 if (instance == null) {
			 instance = new MessageHandler();
		 }
		 return instance;
	 }
	 
	 public static void initConnection() throws IOException {
		 ConnectionFactory factory = new ConnectionFactory();
		 factory.setHost("localhost");
		 //factory.setPort(15672);
		 connection = factory.newConnection();
		 log.info("*** RabbitMQ connection ready! ***");
	 }
	 
	 public static Channel createChannel() throws IOException {
		 return connection.createChannel();
	 }
	 
	 public static void push(Channel channel, String msg) throws IOException {
		 if (channel != null) {
			 channel.basicPublish(EXCHANGE, "", null, msg.getBytes());
			 log.info(" [x] Sent '" + msg + "'");
			 channel.close();
		 }
		 else {
			 throw new IOException("Null channel!");
		 }
	 }
	 
	 
	 public static void closeConnection() throws IOException {
		 connection.close();
	 }
	 
	 public Object clone() throws CloneNotSupportedException {
		 throw new CloneNotSupportedException();
	 }
	 
	 

}
