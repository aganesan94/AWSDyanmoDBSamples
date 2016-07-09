package org.aganesan.amazondynamodbexperiments.dataplane;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;

/**
 * Demonstrates update items into dynamodb
 * 
 * @author agane
 *
 *
 *         If the item does not exist, create it if the item exists, update it.
 *         It is also possible to update the item with the putItem method though
 *         it is not recommended. The difference between updating via putItem
 *         and updateItem method is that the put Item method removes all the
 *         attributes which are not specified in the request.
 */
public class UpdateItem extends BaseItem {

	public UpdateItem() {
		super();
	}

	@Test
	// note that the InsertItem.case1 will need to be run prior to executing
	// this statement.
	public void case1() {

		// Goal
		// 1. Update first Name to Mary
		// 2. Remove attribute colorPencils
		// 3. Add attribute books. (Note it

		try {
			Table table = dynamoDB.getTable(tableName);

			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
			expressionAttributeNames.put("#fName", "firstName");
			expressionAttributeNames.put("#colorPencils", "colorPencils");
			expressionAttributeNames.put("#books", "books");

			List<String> books = Arrays.asList("Alice in wonderland", "Famous Five", "Autobiography of a Yogi");

			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			expressionAttributeValues.put(":val1", "Mary");
			expressionAttributeValues.put(":val2", new HashSet<String>(books)); // Price

			UpdateItemOutcome outcome = table.updateItem("personId", // key
																		// attribute
																		// name
					1, // key attribute value
					"set #fName = :val1 add #books :val2 remove #colorPencils", // UpdateExpression
					expressionAttributeNames, expressionAttributeValues);
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	@Test
	/**
	 * Add remove or update attributes based on a primary key.
	 */
	public void case2() {

		try {
			Table table = dynamoDB.getTable(tableName);

			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
			expressionAttributeNames.put("#fName", "firstName");
			expressionAttributeNames.put("#colorPencils", "colorPencils");
			expressionAttributeNames.put("#books", "books");

			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			expressionAttributeValues.put(":val1", "Gary");
			expressionAttributeValues.put(":val2", new HashSet<String>(Arrays.asList("Red", "Green", "Blue"))); // Price

			UpdateItemOutcome outcome = table.updateItem("personId", // key
																		// attribute
																		// name
					1, // key attribute value
					"set #fName = :val1 remove #books add #colorPencils :val2", // UpdateExpression
					expressionAttributeNames, expressionAttributeValues);
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	/**
	 * Revert it back to original state so as to execute case 2
	 */
	@Test
	public void case3() {
		try {
			Table table = dynamoDB.getTable(tableName);

			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
			expressionAttributeNames.put("#fName", "firstName");
			expressionAttributeNames.put("#colorPencils", "colorPencils");
			expressionAttributeNames.put("#books", "books");

			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			expressionAttributeValues.put(":val1", "Gary");
			expressionAttributeValues.put(":val2", new HashSet<String>(Arrays.asList("Red", "Green", "Blue"))); // Price

			UpdateItemOutcome outcome = table.updateItem("personId", // key
																		// attribute
																		// name
					1, // key attribute value
					"set #fName = :val1 remove #books add #colorPencils :val2", // UpdateExpression
					expressionAttributeNames, expressionAttributeValues);
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	@Test
	/***
	 * Demonstrates executing an update only if there is a condition.
	 * 
	 * update table fname with Mary only if the fname is Gary.
	 * 
	 */
	public void case4() {
		try {
			Table table = dynamoDB.getTable(tableName);

			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
			expressionAttributeNames.put("#age", "age");

			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			expressionAttributeValues.put(":val1", 55);
			expressionAttributeValues.put(":val2", 45);

			UpdateItemOutcome outcome = table.updateItem("personId", // key
					1, // key attribute value
					"set #age = :val1 ", // UpdateExpression
					"#age =  :val2", // conditionexpression
					expressionAttributeNames, expressionAttributeValues);
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

}
