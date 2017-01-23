package com.rest.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class CustomerResourceClient_P3 {
	public static void main(String[] args) {
		try {
			System.out.println("\n*** Create a new Customer ***");
			// Create a new customer
			String newCustomer = "<customer>" + "<first-name>Bill</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";

			URL postUrl = new URL("http://localhost:8090/rest/customers");
			HttpURLConnection connection = (HttpURLConnection) postUrl.openConnection();
			System.out.println("opened connection...");
			connection.setDoOutput(true);
			connection.setInstanceFollowRedirects(false);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/xml");
			OutputStream os = connection.getOutputStream();
			os.write(newCustomer.getBytes());
			os.flush();
			System.out.println(HttpURLConnection.HTTP_CREATED);
			System.out.println(connection.getResponseCode());
			System.out.println("Location: " + connection.getHeaderField("Location"));
			connection.disconnect();

			// Get the new customer
			System.out.println("\n*** GET Created Customer ***");
			URL getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();

			// Update the new customer. Change Bill's name to William
			String updateCustomer = "<customer>" + "<first-name>William</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setDoOutput(true);
			connection.setRequestMethod("PUT");
			connection.setRequestProperty("Content-Type", "application/xml");
			os = connection.getOutputStream();
			os.write(updateCustomer.getBytes());
			os.flush();
			System.out.println(HttpURLConnection.HTTP_NO_CONTENT);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
			
			// Get the updated customer
			System.out.println("\n*** GET Updated Customer ***");
			getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();

			// Create four new customers
			System.out.println("\n*** Create four new Customers ***");
			ArrayList<String> newCustomers = new ArrayList<String>();
			newCustomers.add(new String("<customer>" + "<first-name>Tom</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>"));
			newCustomers.add(new String("<customer>" + "<first-name>Jason</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>248 Clarendon Street</street>" + "<city>Cambridge</city>" + "<state>MA</state>"
					+ "<zip>02478</zip>" + "<country>USA</country>" + "</customer>"));
			newCustomers.add(new String("<customer>" + "<first-name>Richael</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>123 Clarendon Street</street>" + "<city>Newton</city>" + "<state>MA</state>"
					+ "<zip>01827</zip>" + "<country>USA</country>" + "</customer>"));
			newCustomers.add(new String("<customer>" + "<first-name>Jane</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>234 Clarendon Street</street>" + "<city>Watertown</city>" + "<state>MA</state>"
					+ "<zip>02837</zip>" + "<country>USA</country>" + "</customer>"));
			
			for (String customer : newCustomers) {
				postUrl = new URL("http://localhost:8090/rest/customers");
				connection = (HttpURLConnection) postUrl.openConnection();
				System.out.println("opened connection...");
				connection.setDoOutput(true);
				connection.setInstanceFollowRedirects(false);
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Content-Type", "application/xml");
				os = connection.getOutputStream();
				os.write(customer.getBytes());
				os.flush();
				System.out.println(HttpURLConnection.HTTP_CREATED);
				System.out.println(connection.getResponseCode());
				System.out.println("Location: " + connection.getHeaderField("Location"));
				connection.disconnect();			
			}
			
			// Get all the customers
			System.out.println("\n*** GET All the Customers ***");
			getUrl = new URL("http://localhost:8090/rest/customers");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
			
			// Get specified customers
			System.out.println("\n*** GET Specified Customers **");
			int startingId = 2;
			int numOfCustomersToReturn = 3;
			getUrl = new URL("http://localhost:8090/rest/customers/" + startingId + "/" + numOfCustomersToReturn);
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
			
			// Delete one particular customer
			System.out.println("\n*** Delete One Particular Customer ***");
			getUrl = new URL("http://localhost:8090/rest/customers/3");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("DELETE");
			System.out.println("Content-Type: " + connection.getContentType());
			System.out.println(HttpURLConnection.HTTP_NO_CONTENT);
			System.out.println(connection.getResponseCode());
			connection.disconnect();			
			
			// Get all the customers
			System.out.println("\n*** GET All the Customers ***");
			getUrl = new URL("http://localhost:8090/rest/customers");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();		
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
