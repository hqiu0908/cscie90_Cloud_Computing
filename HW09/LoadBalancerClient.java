package com.loadbalancer.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

public class LoadBalancerClient {
	public static void main(String[] args) {
		
		HashMap<String, Integer> counter = new HashMap<String, Integer>();
		
		try {
			for (int i = 0; i < 1000; i++) {
				System.out.println("\n*** Send HTTP GET Request ***");
				URL getUrl = new URL("http://apacheserverloadbalancer-1308527199.us-east-1.elb.amazonaws.com/");
				HttpURLConnection connection = (HttpURLConnection) getUrl.openConnection();
				connection.setRequestMethod("GET");
				System.out.println("Content-Type: " + connection.getContentType());

				BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

				String line = reader.readLine();
				while (line != null) {
					System.out.println(line);
					// Record the times received for each IP address
					if (counter.containsKey(line)) {
						counter.put(line, counter.get(line) + 1);
					} else {
						counter.put(line, 1);
					}					
					line = reader.readLine();
				}
		
				connection.disconnect();	
			}
			
			System.out.println("\nThe times to print each IP address are:\n");
			for (String ip : counter.keySet()) {
				System.out.println("IP address: " + ip + ", times: " + counter.get(ip));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
