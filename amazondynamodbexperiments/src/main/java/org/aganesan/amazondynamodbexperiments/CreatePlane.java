package org.aganesan.amazondynamodbexperiments;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * Demonstrates the ability to execute create plane commands
 * 
 * @author agane
 *
 */
public class CreatePlane {

	static DynamoDB dynamoDB = null;

	static String tableName = "cars";

	private static Logger logger = LogManager.getLogger(CreatePlane.class);

	public static void main(String args[]) {
		logger.debug("Demonstrating the create plane");
		init();
		createTable();
		describeTable();
		updateTable();
		describeTable();
		listAllTables();
		deleteTable();
	}

	/**
	 * Updates Provisioned throughput of the table
	 */
	private static void updateTable() {
		Table table = dynamoDB.getTable(tableName);
		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(15L)
				.withWriteCapacityUnits(12L);
		table.updateTable(provisionedThroughput);
		logger.debug("Update provisioned throughput");
	}

	/**
	 * list all tables
	 */
	private static void listAllTables() {
		TableCollection<ListTablesResult> tables = dynamoDB.listTables();
		Iterator<Table> iterator = tables.iterator();

		while (iterator.hasNext()) {
			Table table = iterator.next();
			logger.debug("Table Name: {} ", table.getTableName());
		}
	}

	/**
	 * Delete table
	 */
	private static void deleteTable() {
		Table table = dynamoDB.getTable(tableName);
		table.delete();
		try {
			table.waitForDelete();
		} catch (InterruptedException e) {
			logger.error("Exception", e);
		}
		logger.debug("Deleted table {} ", tableName);
	}

	// describes a table
	private static void describeTable() {
		TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
		logger.debug("Table Description: {}", tableDescription);

	}

	/**
	 * creates a table
	 * 
	 * @param dbClient
	 */
	private static void createTable() {

		// Uses the builder pattern
		// -http://minborgsjavapot.blogspot.com/2014/08/creating-objects-using-builder-pattern.html
		// create a table with id as the partition key or the hash key, and the
		// year as the range key or the sort key
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
				.withKeySchema(new KeySchemaElement().withAttributeName("year").withKeyType(KeyType.RANGE))
				.withAttributeDefinitions(
						new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.N))
				.withAttributeDefinitions(
						new AttributeDefinition().withAttributeName("year").withAttributeType(ScalarAttributeType.N))
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

		Table table = dynamoDB.createTable(createTableRequest);

		try {
			table.waitForActive();
		} catch (InterruptedException e) {
			logger.error("Exception", e);
		}
		logger.debug("Table created successfully with tablename {}", tableName);
	}

	private static void init() {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (C:\\Users\\agane\\.aws\\credentials), and is in valid format.", e);
		}
		AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		dynamoDBClient.setRegion(usEast1);

		dynamoDB = new DynamoDB(dynamoDBClient);

	}

}
