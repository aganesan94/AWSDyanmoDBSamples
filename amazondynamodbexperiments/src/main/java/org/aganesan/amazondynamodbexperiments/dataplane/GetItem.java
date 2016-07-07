package org.aganesan.amazondynamodbexperiments.dataplane;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

/**
 * Demonstrates retrieving an item based on a primary key. This also
 * demonstrates getting all the parameters of an item or only selected
 * parameters.
 * 
 * @author agane
 *
 */
public class GetItem extends BaseItem {

	public GetItem() {
		super();
	}

	@Test
	// Shows getting an item based on a primary key
	// Run InsertItem.case1 before executing this query
	public void case1() {

		try {
			Table table = dynamoDB.getTable(tableName);
			Item item = table.getItem(new PrimaryKey("personId", 1));
			logger.debug("item : {} ", item);
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	@Test
	// Note that the following query selects only specific attributes of
	// the item. This also demonstrates how to pretty print the JSON.
	public void case2() {
		try {
			Table table = dynamoDB.getTable(tableName);
			GetItemSpec spec = new GetItemSpec().withPrimaryKey("personId", 1)
					.withProjectionExpression("firstName, lastName, favoriteBeverages.cold, sampleJson").withConsistentRead(true);

			Item item = table.getItem(spec);

			logger.debug("item : {} ", item);
			logger.debug("item in JSON pretty : {} ", item.toJSONPretty());
			logger.debug("item in JSON pretty : {} ", item.getJSONPretty("sampleJson"));
		} catch (Exception e) {
			printStackTrace(e);
		}
	}
	
	
	@Test
	// Note that the following query selects the complete json document
	public void case3() {
		try {
			Table table = dynamoDB.getTable(tableName);
			GetItemSpec spec = new GetItemSpec().withPrimaryKey("personId", 1)
					.withProjectionExpression("sampleJson");

			Item item = table.getItem(spec);

			logger.debug("item : {} ", item.toJSON());
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

}
