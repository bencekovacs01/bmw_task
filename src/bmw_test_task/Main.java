package bmw_test_task;

import java.io.IOException;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Main {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger("MyLog");
		logger.setUseParentHandlers(false);
		FileHandler fileHandler;
		String urlJson = "https://jsonplaceholder.typicode.com/users";

		try {
			URL jsonUrl = new URL(urlJson);
			HttpURLConnection connection = (HttpURLConnection) jsonUrl.openConnection();
			connection.setRequestMethod("GET");
			connection.connect();

			Connection databaseConnection = null;

			String databaseUrl = "jdbc:mysql://localhost:3306/bmw_test_task";
			String username = "root";
			String password = "";

			Class.forName("com.mysql.cj.jdbc.Driver");
			databaseConnection = DriverManager.getConnection(databaseUrl, username, password);
			
			Statement constraintCheckOn = databaseConnection.createStatement();
			constraintCheckOn.executeUpdate("set foreign_key_checks = 0");
			Statement truncateCompanies = databaseConnection.createStatement();
			truncateCompanies.executeUpdate("truncate companies");
			Statement truncateAddresses = databaseConnection.createStatement();
			truncateAddresses.executeUpdate("truncate addresses");
			Statement truncateUsers = databaseConnection.createStatement();
			truncateUsers.executeUpdate("truncate users");
			Statement constraintCheckOff = databaseConnection.createStatement();
			constraintCheckOff.executeUpdate("set foreign_key_checks = 1");

			logger.info("Connected to database!");

			fileHandler = new FileHandler("logfile.txt", true);
			logger.addHandler(fileHandler);
			SimpleFormatter formatter = new SimpleFormatter();
			fileHandler.setFormatter(formatter);
			logger.info("Trying to connect to: " + urlJson);
			
			int companiesStatus = 0;
			int addressesStatus = 0;
			int usersStatus = 0;

			PreparedStatement fillCompanies = databaseConnection.prepareStatement("insert into companies values (?,?,?,?)");
			PreparedStatement fillAddresses = databaseConnection.prepareStatement("insert into addresses values (?,?,?,?,?,?,?)");
			PreparedStatement fillUsers = databaseConnection.prepareStatement("insert into users values (?,?,?,?,?,?,?,?)");

			int responseCode = connection.getResponseCode();
			if (responseCode != 200) {
				logger.severe("Cannot access url! Response code: " + responseCode);
				throw new RuntimeException("Response code: " + responseCode);
			} else {
				logger.info("Url accessed successfully!");
				Scanner scanner = new Scanner(jsonUrl.openStream());
				String line = "";
				while (scanner.hasNext()) {
					line += scanner.nextLine();
				}
				scanner.close();

				JSONParser parser = new JSONParser();
				JSONArray usersArray = (JSONArray) parser.parse(line);

				int companiesId = 1;
				int addressesId = 1;
				int usersId = 1;
				logger.info("Saving the data into the database...");

				for (int i = 0; i < usersArray.size(); ++i) {
					JSONObject user = (JSONObject) usersArray.get(i);
					JSONObject company = (JSONObject) user.get("company");
					JSONObject address = (JSONObject) user.get("address");

					String name = (String) company.get("name");
					String catchPhrase = (String) company.get("catchPhrase");
					String bs = (String) company.get("bs");
					
					String street = (String) address.get("street");
					String suite = (String) address.get("suite");
					String city = (String) address.get("city");
					String zipcode= (String) address.get("zipcode");

					JSONObject geo = (JSONObject) address.get("geo");
					String geoLat = (String) geo.get("lat");
					String geoLng = (String) geo.get("lng");
					
					fillAddresses.setInt(1,addressesId++);
					fillAddresses.setString(2, street);
					fillAddresses.setString(3, suite);
					fillAddresses.setString(4, city);
					fillAddresses.setString(5, zipcode);
					fillAddresses.setString(6, geoLat);
					fillAddresses.setString(7, geoLng);

					fillCompanies.setInt(1, companiesId++);
					fillCompanies.setString(2, name);
					fillCompanies.setString(3, catchPhrase);
					fillCompanies.setString(4, bs);
					
					String userName = (String) user.get("name");
					String userUsername = (String) user.get("username");
					int userAddress = usersId;
					String userEmail = (String) user.get("email");
					String userPhone = (String) user.get("phone");
					String userWebsite = (String) user.get("website");
					int userCompany = usersId;
					
					fillUsers.setInt(1,usersId++);
					fillUsers.setString(2,userName);
					fillUsers.setString(3,userUsername);
					fillUsers.setString(4, userEmail);
					fillUsers.setInt(5,userAddress);
					fillUsers.setString(6,userPhone);
					fillUsers.setString(7,userWebsite);
					fillUsers.setInt(8,userCompany);

					addressesStatus = fillAddresses.executeUpdate();
					companiesStatus = fillCompanies.executeUpdate();
					usersStatus = fillUsers.executeUpdate();
					
					String email = user.get("email").toString();
					validateEmail(email,logger);
				}
				logger.info("Database filled successfully!");
			}
			fillCompanies.close();
			fillAddresses.close();
			databaseConnection.close();
			connection.disconnect();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void validateEmail(String email,Logger logger) {
		String regex = "^(.+)@(.+)$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(email);
		if (!matcher.matches()) {
			logger.info("Email address format invalid for: " + email);
		}
	}
	
	
}
