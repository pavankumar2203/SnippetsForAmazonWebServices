/**
 * @author Pavan kumar Sunder
 * @email psunder@andrew.cmu.edu
 * 
 * This creates a dynamoDB and loads data into it
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

public class DynamoDB {
	

	
	public static void main(String[] args) throws Exception {

		//get the credentials
		Properties properties = new Properties();
		properties.load(CreateInstance.class
				.getResourceAsStream("/AwsCredentials.properties"));

		BasicAWSCredentials credentials = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));

		
		try {
			//create a dynamodb client
			AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(credentials);

			//table name
			String table = "Caltech256";

			// Create a table in dynamodb with category and picture attributes
			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(table)
					.withKeySchema(
							new KeySchemaElement()
									.withAttributeName("Category").withKeyType(
											KeyType.HASH))
					.withAttributeDefinitions(
							new AttributeDefinition().withAttributeName(
									"Category").withAttributeType(
									ScalarAttributeType.S))
					.withKeySchema(
							new KeySchemaElement().withAttributeName("Picture")
									.withKeyType(KeyType.RANGE))
					.withAttributeDefinitions(
							new AttributeDefinition().withAttributeName(
									"Picture").withAttributeType(
									ScalarAttributeType.N))
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(
									1L).withWriteCapacityUnits(300L));
			TableDescription createdTableDescription = dynamoDB.createTable(
					createTableRequest).getTableDescription();
			System.out.println("created table " + createdTableDescription);

			// Wait till table is active
			System.out.println("waiting for " + table + " to become ACTIVE");
			while (true) {
				try {
					Thread.sleep(1000 * 10);
				} catch (Exception e) {
				}
				try {
					//check for table activation status
					DescribeTableRequest request = new DescribeTableRequest()
							.withTableName(table);
					TableDescription tableDescription = dynamoDB.describeTable(
							request).getTable();
					String tableStatus = tableDescription.getTableStatus();
					System.out.println(tableStatus);
					if (tableStatus.equals(TableStatus.ACTIVE.toString()))
						break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			DescribeTableRequest describeTableRequest = new DescribeTableRequest()
					.withTableName(table);
			TableDescription tableDescription = dynamoDB.describeTable(
					describeTableRequest).getTable();
			System.out.println("Table created is  " + tableDescription);

			// load data into the table from CSV
			long startTime1 = System.currentTimeMillis();
			File f = new File("C:/Users/Pavan/workspace/LoadTest/src/caltech-256.csv");
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = reader.readLine();
			int count = 0;
			while ((line = reader.readLine()) != null) {
				String[] s = line.split(",");
				// create a row and put it into the table
				
				Map<String, AttributeValue> row = new HashMap<String, AttributeValue>();
				row.put("Category", new AttributeValue(s[0]));
				row.put("Picture", new AttributeValue().withN(s[1]));
				row.put("S3URL", new AttributeValue().withS(s[2]));
				
				PutItemRequest putItemRequest = new PutItemRequest(table,
						row);
				dynamoDB.putItem(putItemRequest);
				System.out.println((count++));

			}
			long endTime1 = System.currentTimeMillis();
			System.out.println(endTime1 - startTime1);
			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}


	
}