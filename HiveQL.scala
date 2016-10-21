package main;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveQLWhere {
   private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
   
   public static void main(String[] args) throws SQLException, ClassNotFoundException {
   
      // Register driver and create driver instance
      Class.forName(driverName);
      
      // get connection
      Connection con = DriverManager.getConnection("jdbc:hive2://sldifrdwbhn01.fr.intranet:10001/eag-fxticks;transportMode=http;httpPath=cliservice;principal=hive/_HOST@CIB.NET");
      
      // create statement
      Statement stmt = con.createStatement();
      
      // execute statement
      ResultSet res = stmt.executeQuery("SELECT * FROM mydata;");
      
      System.out.println("Result:");
      //System.out.println(" ID \t Name \t Salary \t Designation \t Dept ");
      
      while (res.next()) {
         System.out.println(res.getInt(1) + " " + res.getString(2) + " " + res.getDouble(3) + " " + res.getString(4) + " " + res.getString(5));
      }
      con.close();
   }
}
