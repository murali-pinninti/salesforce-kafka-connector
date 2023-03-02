package com.salesforce.emp.connector.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.json.JSONObject;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Util {
	public static String DB_URL = "jdbc:oracle:thin:@//camb-ocmdev01.forrester.com:4443/BKP";
	public static String USER_NAME = "sirius";
	public static String PASS = "xibsT3F@/cE8AT#S?[b~";
	Connection conn;
	public static void sql(String query) {
		try { 
            String url = "jdbc:oracle:thin:@//camb-ocmdev01.forrester.com:4443/BKP"; 
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection(DB_URL,USER_NAME,PASS); 
            Statement st = conn.createStatement(); 
            st.executeUpdate(query); 
            System.out.println("QUery ran");
            //st.executeUpdate("INSERT INTO Customers " + "VALUES (1002, 'McBeal', 'Ms.', 'Boston', 2004)"); 
            //st.executeUpdate("INSERT INTO Customers " + "VALUES (1003, 'Flinstone', 'Mr.', 'Bedrock', 2003)"); 
            
            conn.close(); 
        } catch (Exception e) { 
            System.err.println("Got an exception! "); 
            System.err.println(e.getMessage());
            e.printStackTrace();
        } 
	}
	//{"event":{"createdDate":"2022-01-04T17:59:25.357Z","replayId":1009,"type":"created"},
	//"sobject":{"Email":"annapravda010422@gmail.com","AccountId":"001a000001AEq0NAAT",
	     //"FirstName":"Anna","LastName":"Pravda","Id":"00377000005s5blAAA"}}

	public static void contactJsonParse(String json) {
		JSONObject obj = new JSONObject(json);
		String type = obj.getJSONObject("event").getJSONObject("type").toString();
		System.out.println("PARSED data - "+obj.toString());
		System.out.println(type);
		if("created".equalsIgnoreCase(type)) {
			JSONObject feed = obj.getJSONObject("event");
			String id = feed.getJSONObject("Id").toString();
			String accountId = feed.getJSONObject("AccountId").toString();
			String firstName = feed.getJSONObject("FirstName").toString();
			String lastName = feed.getJSONObject("LastName").toString();
			String email = feed.getJSONObject("Email").toString();
			String query = "INSERT INTO BKP.CONTACTS_KAFKA VALUES('"+id+"','"+accountId+"','"
						+firstName+"','"+lastName+"','"+email+"')";
			System.out.println("Query formed \n"+query);
		}
	}
	
	public void createConnection()
	{
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}  
		try {

			this.conn=DriverManager.getConnection(DB_URL,USER_NAME,PASS);
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}

	public void executeStatement(String s)
	{
		Statement stmt;
		ResultSet rs;
		try {
			stmt = conn.createStatement();
			stmt.execute(s);
			stmt.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	public void executeStatement(String s,List<String> values,String dateFormat,int index)
	{
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(s);
			int i;
			for( i=0;i<values.size();i++)
			{
				String value = values.get(i);
				if(i==index)
				{
					SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
					Timestamp t = new Timestamp(sdf.parse(value).getTime());
					stmt.setTimestamp(i+1, t);
				}
				else
				{
					stmt.setString (i+1, value);
				}
			}
			stmt.execute();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}  
		finally {
			try {
				if(stmt !=null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void closeConnection() {
		if (conn !=null)
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally {
			System.out.println("connection closed");
		}
	}
}
