package jdbc_demo;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.Scanner;
import java.sql.PreparedStatement;

class MySqlCon {
	public static void main(String args[]) {
		

        String filePath = "/Users/tirth/JuniorSpring2021/CS480/cs480_JDBC_Demo/src/jdbc_demo/transfile";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			//Initially please create a database Homework3 in your MySQL by the statement : create database Homework3
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/Homework3", "root", "password");
			Statement stmt = con.createStatement();
			
			String dropCoursePrereq = "DROP TABLE IF EXISTS course,prereq ";
			 
		      stmt.executeUpdate(dropCoursePrereq);
		      
		      
			  String createCourse = "CREATE TABLE course " +
	                   "(cid Varchar(255) not NULL, " +
	                   " title VARCHAR(255), " + 
	                   " credits INTEGER, " + 
	                   " dname VARCHAR(255), " + 
	                   " PRIMARY KEY ( cid ))";
			  
			  String createPrereq = "CREATE TABLE prereq " +
	                   "(cid VARCHAR(255) not NULL, " +
	                   " pid VARCHAR(255)) " ; 
			  
			  stmt.executeUpdate(createCourse);
			  stmt.executeUpdate(createPrereq);
			  
			    // create a reader instance
			    BufferedReader br = new BufferedReader(new FileReader(filePath));

			    // read until end of file
			    String line;
			  
			   
			    
			    String sql_5 = "SELECT count(*) FROM course WHERE cid=? ";
			    String def = null;
			    while ((line = br.readLine()) != null) {
			    	String[] words = line.split("\\s+"); // split word by one or more spaces
			    	
			    	
			    	if(line.charAt(0) == '2') {
			    		  String sql = "INSERT INTO course values (?, ?, ?, ?)";
			    		PreparedStatement pStmt = con.prepareStatement(sql);
			    		PreparedStatement pStmt5 = con.prepareStatement(sql_5);

			    		
			    		pStmt.setString(1, words[1]);

			    		pStmt.setString(2, words[2]);
			    		
			    		if(words[3] != null) {
			    		pStmt.setInt(3, Integer.parseInt(words[3]));
			    		}
			    		
//			    		if(words.length > 4 && words[4] != null ) {
			    		pStmt.setString(4, words[4]);

			    		// if course ha multiple prereqs 
			    		if(words.length > 5 && words[5] != null)
			    		{
			    			int i = 5 ; 
			    			// add all prereqs for a course 
			    			while(i < words.length) {
			    				pStmt5.setString( 1,words[i]);
			    				ResultSet rs = pStmt5.executeQuery();
					    		rs.next();
					    		
			    			// check whether a pid is cid of an existing course 
			    				if(rs.getInt(1) > 0) {
			    				String sql2 = "INSERT INTO prereq values (?, ?)";
			    				PreparedStatement pStmt2 = con.prepareStatement(sql2);
				    			pStmt2.setString(1, words[1]);
				    			pStmt2.setString(2, words[i++]); // add multiple prereqs
				    			pStmt2.executeUpdate();
				    			
				    			}
			    				else {
			    					System.out.println("Course cid " + words[i] + " does not exist");
			    					i++;   // continue to next prereq if any 
			    					
			    				}	
			    			}
			    		}
			    					    		
			    		pStmt.executeUpdate();
			    	}
		    	
			    	else if (line.charAt(0) == '1')
			    	{
			    		String sql3 = "DELETE FROM course WHERE cid=?";  // delete the cid from course
					    String sql4 = "DELETE FROM prereq WHERE cid=?" + " OR pid=?";  // delete tuple from prereq if cid or pid is specified
			    		PreparedStatement pStmt3 = con.prepareStatement(sql3);
			    		PreparedStatement pStmt4 = con.prepareStatement(sql4);
			    		PreparedStatement pStmt5 = con.prepareStatement("SELECT count(*) from course where cid=?");
			    		pStmt5.setString(1,words[1]);
			    		ResultSet r = pStmt5.executeQuery();
			    		r.next();
			    		if(r.getInt(1) > 0 ) // if cid exist in the course table
			    		{
			    		pStmt3.setString(1, words[1]);
			    		pStmt4.setString(1, words[1]);
			    		pStmt4.setString(2, words[1]);
			    		pStmt3.executeUpdate();
			    		pStmt4.executeUpdate();
			    		}
			    		else {
			    			System.out.println("Course cid "+ words[1] +" does not exist in the course table");
			    		}

			    	}

//			    	// delete pre requite for a course 
			    	else if (line.charAt(0) == '3')
			    	{
			    		String sql5 = "SELECT count(*) FROM course WHERE cid=? "; // check if cid exists in course
			    		
			    		String sql6 = "DELETE FROM prereq WHERE cid=? AND pid=?"; // check direct prereq
			    		String sql7 = "SELECT count(*) FROM course WHERE cid=?";  //checl if pid exists in course
			    		PreparedStatement pStmt4 = con.prepareStatement(sql6);
			      		PreparedStatement pStmt5 = con.prepareStatement(sql5);
			      		PreparedStatement pStmt6 = con.prepareStatement(sql7);
			      		pStmt5.setString(1, words[1]);   // course id
			      		pStmt6.setString(1, words[2]);
			      		ResultSet rs=	pStmt5.executeQuery();
			      		rs.next();
			      		ResultSet rs2=	pStmt6.executeQuery();
			      		rs2.next();
			    		if(rs.getInt(1) > 0  && rs2.getInt(1) > 0)   // delete only when cid already exists 
			    		{
			    		
			    		pStmt4.setString(1, words[1]);
			    		pStmt4.setString(2, words[2]);
			    		pStmt4.executeUpdate();
			    		}
			    		else {
			    			System.out.println("cid does not exist in course table for cid: " + words[1]);
			    			System.out.println("OR pid does not exist in course table for pid: " + words[2]);
			    		}

			    	
			    	}		    		
			    	else if (line.charAt(0) == '4')
			    	{
			    		String sql7 = "SELECT count(*) FROM course WHERE cid=?";
			    		String sql8 = "SELECT count(*) FROM prereq WHERE cid=? AND pid=?"; // check if pid is not already a prereq for cid
			    		String sql9 = "INSERT INTO prereq values(?, ?)";
			    		
			    		
			    		PreparedStatement pStmt5 = con.prepareStatement(sql7);
			    		PreparedStatement pStmt6 = con.prepareStatement(sql8);
			    		PreparedStatement pStmt7 = con.prepareStatement(sql9);
			    		PreparedStatement pStmt8 = con.prepareStatement(sql7);
			    		pStmt5.setString(1, words[1]);   // course id
			    		pStmt6.setString(1, words[1]);
			    		pStmt6.setString(2, words[2]);
			    		pStmt8.setString(1, words[2]);
			    		
			      		
			      		ResultSet rs=	pStmt5.executeQuery();
			      		ResultSet rs2=	pStmt6.executeQuery();
			      		ResultSet rs3 = pStmt8.executeQuery();
			      		rs.next();
			      		rs2.next();
			      		rs3.next();
			      		// if cid is present in the course table and pid is not already a prereq for this cid
			      		if(rs.getInt(1) > 0  && rs2.getInt(1) == 0 && rs3.getInt(1) > 0)
			      		{
			      			pStmt7.setString(1,words[1]);
			      			pStmt7.setString(2, words[2]);
			      			pStmt7.executeUpdate();
			      		}
			      		else { 
			      			// three possibilities
			      			System.out.println("pid " + words[2] + " is already a prereq for cid " + words[1]);
			      			System.out.println("OR Course cid " + words[1] + " does not exits in course table");
			      			System.out.println("OR Course prereq pid "+ words[2] + " is not an existing course in course table");
			      		}
			    	} 
			    		
			    	else if (line.charAt(0) == '6')
			    	{
			    		String sql1 = "SELECT cid,title from course where dname=?";
			    		PreparedStatement pStmt = con.prepareStatement(sql1);
			    		
			    		pStmt.setString(1, words[1]);
			    		
			    		ResultSet rs = pStmt.executeQuery();
			    		while (rs.next())
							System.out.println(rs.getString(1) + "  " + rs.getString(2) );
			    
			    	}
//			    	
			    	else if (line.charAt(0) == '5')
			    	{
			    		String sql1 = "SELECT count(*) from course where cid=?";
			    		
			    		// get the cid and title of direct prerequisites 
			    		String sql10 = "SELECT c.cid,c.title from course c where c.cid IN (SELECT p.pid FROM prereq p WHERE p.cid=?) ";
			    		
			    		// get the cid and title of indirect prerequisites 
			    	String sql11 = "SELECT c.cid,c.title from course c where c.cid IN (SELECT p.pid from prereq as p INNER JOIN prereq as p1 ON p1.pid = p.cid WHERE"
			    			+ " p1.cid=? AND p.pid NOT IN (SELECT pid from prereq WHERE cid = ?))";
			    	
			    		PreparedStatement pStmt1 = con.prepareStatement(sql1);
			    		PreparedStatement pStmt2 = con.prepareStatement(sql10);
			    		PreparedStatement pStmt3 = con.prepareStatement(sql11);
			    		pStmt1.setString(1 ,words[1]);
			    		pStmt2.setString(1, words[1]);   
			    		pStmt3.setString(1, words[1]);   
			    		pStmt3.setString(2, words[1]); 
			    		ResultSet rs=	pStmt1.executeQuery();
			    		rs.next();
			    		ResultSet rs2=	pStmt2.executeQuery();
			    		ResultSet rs3=	pStmt3.executeQuery();

			    		// if cid is present 
			    		if(rs.getInt(1) > 0) 
			    		{
			    			while (rs2.next()) // print direct pre requisites 
								System.out.println(rs2.getString(1) + "  "+ rs2.getString(2) );
			    			
			    			while (rs3.next()) // print indirect pre requisites 
								System.out.println(rs3.getString(1) + "  "+ rs3.getString(2) );
			    		}
			    		else {
			    			System.out.println("No existing course with cid: "+ words[1]);
			    		}
			    		
			    	}
			    	Arrays.fill(words, null); // clear the words array after each iteration of line 
			    
			    }//  end while

			    
			    // drop all tables
			    
			    String dropTables = "DROP TABLE IF EXISTS course,prereq ";
			    stmt.executeUpdate(dropTables);
			    // close the reader
			    br.close();
			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}	
	}
}
