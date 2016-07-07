package org.aganesan.amazondynamodbexperiments.dataplane;

import org.junit.Test;

/**
 * Demonstrates inserting items into dynamodb.
 * 
 * @author agane
 *
 */
public class InsertItem extends BaseItem {

	public InsertItem() {
		super();
	}

	@Test
	public void execute(){
		
		
		// Person -> firstname, lastname, age
		// Person -> Address (line1, line2, city, state, zipcode)
		// Person -> Set<String> colorPencils
		// Person -> List<String> cars
		// Person -> Map<String, List<String>> beverages both cold and hot.
		// Storing a JSON string
		// Storing an optional parameter.
		
		
	}

}
