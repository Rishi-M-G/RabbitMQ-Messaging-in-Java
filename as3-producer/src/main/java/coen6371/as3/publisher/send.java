package coen6371.as3.publisher;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class send {
	
	private final static String EXCHANGE_NAME = "topic_mongodb";

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		
		// ***** SETTING CONNECTION FOR RABBITMQ ***** 
		ConnectionFactory factory = new ConnectionFactory();
		
		// setting up IP address for the rabbimq server.. It is hosted in oracle cloud instance
		factory.setHost("155.248.234.61");
		try(Connection connection = factory.newConnection();
				Channel channel = connection.createChannel()){
			
			
			//creating exchange interface to handle topics, here the data transferring method is "topic"
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			
			// *** CREATING MONGODB CLIENT ***
			// establishing connection to my mongodb instance
			ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
			MongoClientSettings settings = MongoClientSettings.builder()
			        .applyConnectionString(connectionString)
			        .build();
			MongoClient mongoClient = MongoClients.create(settings);
			
			// Access the MongoDB database
			MongoDatabase database = mongoClient.getDatabase("MyDatabase");
			Thread.sleep(1000);
			
			// FOR TOPIC ONE **************************************************************************************
			//Access MongoDB collection
			MongoCollection<Document> collection_one = database.getCollection("SampleEduCostStatQueryOne");
			
			//Retrieve documents from the MongoDB collection
			Document query_one = new Document("State", "Alabama");
			MongoCursor<Document> cursor_one = collection_one.find(query_one).iterator();
			
			while(cursor_one.hasNext())
			{
				Document doc = cursor_one.next();
				String routingKey = createRoutingKey_one(doc);
				String message = getMessage_one(doc);
				
				//Publish the message to exchange
				channel.basicPublish(EXCHANGE_NAME,routingKey, null, message.getBytes("UTF-8"));
				System.out.println("[x] Sent '"+routingKey+"':'"+message+"'");
				
			}
			
			// *****************************************************************************************************
			
			
			//FOR TOPIC TWO **************************************************************************************
			//Access MongoDB Collection
			MongoCollection<Document> collection_two = database.getCollection("SampleEduCostStatQueryTwo");
			
			//Retrieve documents from the the MongoDB collection
			MongoCursor<Document> cursor_two = collection_two.find().sort(Sorts.ascending("_id")).iterator();
			
			
			while(cursor_two.hasNext())
			{
				Document doc = cursor_two.next();
				String routingKey = createRoutingKey_two(doc);
				String message = getMessage_two(doc);
				
				//Publish the message to the exchange
				channel.basicPublish(EXCHANGE_NAME,routingKey, null, message.getBytes("UTF-8"));
				System.out.println("[x] Sent '"+routingKey+"':'"+message+"'");
				
			}
			
			// *****************************************************************************************************
			
			//FOR TOPIC THREE **************************************************************************************
			//Access MongoDB Collection
			MongoCollection<Document> collection_three = database.getCollection("SampleEduCostStatQueryThree");
			
			//Retrieve documents from the the MongoDB collection
			MongoCursor<Document> cursor_three = collection_three.find().sort(Sorts.ascending("_id")).iterator();
			
			
			while(cursor_three.hasNext())
			{
				Document doc = cursor_three.next();
				String routingKey = createRoutingKey_three(doc);
				String message = getMessage_three(doc);
				
				//Publish the message to the exchange
				channel.basicPublish(EXCHANGE_NAME,routingKey, null, message.getBytes("UTF-8"));
				System.out.println("[x] Sent '"+routingKey+"':'"+message+"'");
				
			}
			
			// *****************************************************************************************************
			
			//FOR TOPIC FOUR **************************************************************************************
			//Access MongoDB Collection
			MongoCollection<Document> collection_four = database.getCollection("SampleEduCostStatQueryFour");
			
			//Retrieve documents from the the MongoDB collection
			MongoCursor<Document> cursor_four = collection_four.find().sort(Sorts.ascending("Growth_Rate_3_Years")).iterator();
			
			while(cursor_four.hasNext()) 
			{
				Document doc = cursor_four.next();
				String routingKey = createRoutingKey_four(doc);
				String message = getMessage_four(doc);
				
				//Publish the message to the exchange
				channel.basicPublish(EXCHANGE_NAME,routingKey, null, message.getBytes("UTF-8"));
				System.out.println("[x] Sent '"+routingKey+"':'"+message+"'");
			}
			
			// *****************************************************************************************************
			
			//FOR TOPIC FOUR **************************************************************************************
			//Access MongoDB Collection
			MongoCollection<Document> collection_five = database.getCollection("SampleEduCostStatQueryFive");
			
			//Retrieve documents from the the MongoDB collection
			MongoCursor<Document> cursor_five = collection_five.find().sort(Sorts.ascending("total_value")).iterator();
			
			while(cursor_five.hasNext()) 
			{
				Document doc = cursor_five.next();
				String routingKey = createRoutingKey_five(doc);
				String message = getMessage_five(doc);
				
				//Publish the message to the exchange
				channel.basicPublish(EXCHANGE_NAME,routingKey, null, message.getBytes("UTF-8"));
				System.out.println("[x] Sent '"+routingKey+"':'"+message+"'");
			}
			
		}	
	}
	
	private static String createRoutingKey_one(Document doc)
	{
		//Extracting the required fields from the document
		int Year = doc.getInteger("Year");
		String State = doc.getString("State");
		String Type = doc.getString("Type");
		String Length = doc.getString("Length");
		
		//create routing key using the required fields
		String routingKey = "Cost-"+Year+"-"+State+"-"+Type+"-"+Length;
		
		return routingKey;
	}
	
	private static String getMessage_one(Document doc)
	{
		//Extracting the required result value from the document
		int Value = doc.getInteger("Value");
		String Expense = doc.getString("Expense");
		
		String message = "The Result for the Query Parameters are: Value: " +Integer.toString(Value) +"For the expense type : "+Expense;
		
		return message;
	}
	
	private static String createRoutingKey_two(Document doc)
	{
		//Extracting the required fields from the document
		int Year = doc.getInteger("Year");
		String Type = doc.getString("Type");
		String Length = doc.getString("Length");
		//create routing key using the required fields
		String routingKey = "Top5-Expensive-"+Year+"-"+Type+"-"+Length;
		
		return routingKey;
		
	}
	
	private static String getMessage_two(Document doc)
	{
		//Extracting the required result value from the document
		String State = doc.getString("_id");
		int Overall_Expense = doc.getInteger("Overall Expense");
		
		String message = "State : "+State+" Overall Expense: "+Overall_Expense;
		
		return message;

	}
	
	private static String createRoutingKey_three(Document doc)
	{
		//Extracting the required fields from the document
		int Year = doc.getInteger("Year");
		String Type = doc.getString("Type");
		String Length = doc.getString("Length");
		//create routing key using the required fields
		String routingKey = "Top5-Economic-"+Year+"-"+Type+"-"+Length;
		
		return routingKey;
		
	}
	
	private static String getMessage_three(Document doc)
	{
		//Extracting the required result value from the document
		String State = doc.getString("_id");
		int Overall_Expense = doc.getInteger("Overall Expense");
		
		String message = "State : "+State+" Overall Expense: "+Overall_Expense;
		
		return message;

	}
	
	
	private static String createRoutingKey_four(Document doc)
	{
		//Extracting the required fields from the document
		
		String Years = doc.getString("Years");
		//create routing key using the required fields
		String routingKey = "Top5-HighestGrowth-"+Years;
		
		return routingKey;
		
	}
	
	private static String getMessage_four(Document doc)
	{
		//Extracting the required result value from the document
		String State = doc.getString("State");
		double growth = doc.getDouble("Growth_Rate_3_Years");
		
		String message = "State : "+State+" Growth Rate over 3 years: "+growth;
		
		return message;

	}
	
	private static String createRoutingKey_five(Document doc)
	{
		//Extracting the required fields from the document
		
		int Year = doc.getInteger("Year");
		String Type = doc.getString("Type");
		String Length = doc.getString("Length");
		//create routing key using the required fields
		String routingKey = "AverageExpense-"+Year+"-"+Type+"-"+Length;
		
		return routingKey;
		
	}
	
	private static String getMessage_five(Document doc)
	{
		//Extracting the required result value from the document
		String Region = doc.getString("_id");
		int Value = doc.getInteger("total_value");
		
		String message = "Region : "+Region+" Region value: "+Value;
		
		return message;

	}


}
