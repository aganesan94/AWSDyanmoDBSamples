package org.aganesan.amazondynamodbexperiments.dataplane;

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
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public abstract class BaseItem {

	protected DynamoDB dynamoDB = null;

	protected String tableName = "person";

	protected Logger logger = LogManager.getLogger(this.getClass());

	public BaseItem() {
		super();
		logger.debug("Demonstrating the create plane");
		init();
		createTable();
	}

	/**
	 * Delete table
	 */
	private void deleteTable() {
		Table table = dynamoDB.getTable(tableName);
		table.delete();
		try {
			table.waitForDelete();
		} catch (InterruptedException e) {
			logger.error("Exception", e);
		}
		logger.debug("Deleted table {} ", tableName);
	}

	/**
	 * creates a table
	 * 
	 * @param dbClient
	 */
	private void createTable() {

		boolean tableExists = true;
		try {
			dynamoDB.getTable(tableName).describe();
		} catch (Exception e) {
			logger.warn("Table does not exist with tablename {}, exception is {}", tableName, e.getMessage());
			tableExists = false;
		}

		if (tableExists) {
			logger.warn("Table Exists");
			return;
		}

		// Uses the builder pattern
		// -http://minborgsjavapot.blogspot.com/2014/08/creating-objects-using-builder-pattern.html
		// create a table with id as the partition key or the hash key, and the
		// year as the range key or the sort key
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName("personId").withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName("personId")
						.withAttributeType(ScalarAttributeType.N))
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(2L).withWriteCapacityUnits(2L));

		Table table = dynamoDB.createTable(createTableRequest);

		try {
			table.waitForActive();
		} catch (InterruptedException e) {
			logger.error("Exception", e);
		}
		logger.debug("Table created successfully with tablename {}", tableName);
	}

	private void init() {

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
