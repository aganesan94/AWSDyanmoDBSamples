package org.aganesan.amazondynamodbexperiments.dataplane;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

/**
 * Demonstrates inserting items into dynamodb.
 * 
 * @author agane
 *
 */
public class DataPlane extends BaseItem {

	public DataPlane() {
		super();
	}

	@Test
	public void case1() {

		// Person -> firstname, lastname, age, male/female?, spouseName
		// Person -> Address (line1, line2, city, state, zipcode)
		// Person -> Set<String> colorPencils
		// Person -> List<String> cars
		// Person -> Map<String, List<String>> beverages both cold and hot.
		// Storing a JSON string
		// Storing an optional parameter.

		try {
			String json = readJson();
			logger.debug("json : {} ", json);

			deleteIfExists();

			// notice the builder pattern again
			// Build the item
			Item item = new Item()
					// notice how the primary key is declared
					.withPrimaryKey(new PrimaryKey("personId", 1))
					// notice how Strings are declared
					.withString("firstName", "don").withString("lastName", "Quixote")
					// notice how number is declared
					.withNumber("age", 21)
					// notice how boolean is declared
					.withBoolean("isFemale", true)
					// Notice how null is declared
					.withNull("spouseName")
					// notice declaration of a set of strings. Also note you can
					// use implementations of the "Set Interface" as well.
					.withStringSet("colorPencils", new HashSet<String>(Arrays.asList("Red", "Green", "Blue")))
					.withList("cars", getListOfCars())
					// map of hot and cold beverages
					.withMap("favoriteBeverages", getMapOfBeverages()).
					// notice use of JSONS
					withJSON("sampleJson", readJson());

			Table table = dynamoDB.getTable(tableName);

			// Write the item to the table
			PutItemOutcome outcome = table.putItem(item);
			logger.info("Item for case 1 inserted successfully ");
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	@Test
	// Note that case 1 needs to be run before this method is executed
	// The goal of this method is to demonstrate that an item with the same primary key can be updated if a condition
	// specified in met
	public void case2() {

		// Person -> firstname, lastname, age, male/female?, spouseName
		// Person -> Address (line1, line2, city, state, zipcode)
		// Person -> Set<String> colorPencils
		// Person -> List<String> cars
		// Person -> Map<String, List<String>> beverages both cold and hot.
		// Storing a JSON string
		// Storing an optional parameter.

		try {
			String json = readJson();
			logger.debug("json : {} ", json);
			
			
			//deleteIfExists();

			// notice the builder pattern again
			// Build the item
			Item item = new Item()
					// notice how the primary key is declared
					.withPrimaryKey(new PrimaryKey("personId", 1))
					// notice how Strings are declared
					.withString("firstName", "Gary").withString("lastName", "Driscoll")
					// notice how number is declared
					.withNumber("age", 45)
					// notice how boolean is declared
					.withBoolean("isFemale", false)
					// Notice how null is declared
					.withNull("spouseName")
					// notice declaration of a set of strings
					.withStringSet("colorPencils", new HashSet<String>(Arrays.asList("Red", "Green", "Blue")))
					.withList("cars", getListOfCars())
					// map of hot and cold beverages
					.withMap("favoriteBeverages", getMapOfBeverages()).
					// notice use of JSONS
					withJSON("sampleJson", readJson());

			Table table = dynamoDB.getTable(tableName);

			// Write the item to the table
			// Case 1 had age specified as 21.
			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			expressionAttributeValues.put(":age", 21);

			PutItemOutcome outcome = table.putItem(
			    item, 
			    "age = :age", // ConditionExpression parameter
			    null,          // ExpressionAttributeNames parameter - we're not using it for this example
			    expressionAttributeValues);
			logger.info("Item for case 2 inserted successfully ");
		} catch (Exception e) {
			printStackTrace(e);
		}
	}

	private List<String> getFavoriteSnacks() {
		List<String> snacks = new ArrayList<String>();
		snacks.add("chips");
		snacks.add("candies");
		snacks.add("chocolates");
		return snacks;
	}

	/**
	 * returns a map of beverages.
	 * 
	 * @return
	 */
	private Map<String, List<String>> getMapOfBeverages() {
		Map<String, List<String>> mapOfBeverages = new HashMap<>();

		List<String> coldBeverages = new ArrayList<>();
		coldBeverages.add("COKE");
		coldBeverages.add("PEPSI");
		coldBeverages.add("SPRITE");

		List<String> hotBeverages = new ArrayList<>();
		coldBeverages.add("TEA");
		coldBeverages.add("COFFEE");
		coldBeverages.add("CAPPACINO");

		mapOfBeverages.put("cold", coldBeverages);
		mapOfBeverages.put("hot", hotBeverages);

		return mapOfBeverages;

	}

	/**
	 * returns a list of cars.
	 * 
	 * @return
	 */
	private List<String> getListOfCars() {
		List<String> cars = new ArrayList<>();
		cars.add("Honda");
		cars.add("Toyota");
		cars.add("BMW");
		return cars;
	}

	/**
	 * For the sake of executing this program again and again deleteIfExists
	 * will delete any items with the given id
	 * 
	 */
	private void deleteIfExists() {
		Table table = dynamoDB.getTable(tableName);
		DeleteItemOutcome outcome1 = table.deleteItem("personId", 1);
		DeleteItemOutcome outcome2 = table.deleteItem("personId", 2);
		DeleteItemOutcome outcome3 = table.deleteItem("personId", 3);
		logger.debug("Deleted person Objects");
	}

	/**
	 * read a json from the resources directory.
	 * 
	 * @return
	 */
	private String readJson() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("test.json").getFile());
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			String everything = sb.toString();
			return everything;
		} catch (FileNotFoundException e) {
			logger.error("File Not Found exception ", e);
		} catch (IOException e) {
			logger.error("File Not Found exception ", e);
		}
		return null;
	}

}
