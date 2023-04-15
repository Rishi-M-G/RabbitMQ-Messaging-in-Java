package coen6371.as3.consumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;

public class Recv {
	
	private final static String QUEUE_NAME = "hello";
	private final static String EXCHANGE_NAME = "topic_mongodb";

	public static void main(String[] args) throws IOException, TimeoutException {
		// ***** SETTING CONNECTION FOR RABBITMQ *****
		ConnectionFactory factory = new ConnectionFactory();
		
		// setting up IP address for the rabbitmq server.. It is hosted in oracle cloud instance
		factory.setHost("155.248.234.61");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		
		//creating exchange interface to handle topics, here the data transferring method is "topic"
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		
		//creating queues.. queues will be assigned with random names by the rabbitmq server
		String queueName = "";
		for(String qn:args)
		{
			queueName = channel.queueDeclare(qn,true,false,false,null).getQueue();
		}
		
		
		if(args.length < 1)
		{
			System.err.println("Usage: Arguments not passed properly. THE APPLICATION WILL EXIT NOW");
			System.exit(1);
		}
		
		for(String bindingKey:args)
		{
			
			channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
		}
		
		
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    
	    
	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	        String message = new String(delivery.getBody(), "UTF-8");
	        System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey()+"':'"+message + "'");
	    };
	    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

	}

}
