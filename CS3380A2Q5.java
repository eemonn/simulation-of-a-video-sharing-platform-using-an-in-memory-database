import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.Scanner;


public class CS3380A2Q5 {
    static Connection connection;

    public static void main(String[] args) throws Exception {
	
		// startup sequence
		MyDatabase db = new MyDatabase();		
		doStuff(db);

		
		System.out.println("Exiting...");
	}
	
	public static void doStuff(MyDatabase db){

		String name = "Snowball Grottobow";
		String link = "bz4bnJ77um";
		try{
			Scanner sc = new Scanner(System.in);
			System.out.println("Gimme an Elf name: ");
			String maybeName = sc.nextLine();
			System.out.println("Gimme a CheerTube link");
			String maybeLink = sc.nextLine();

			if (maybeName.length() > 0)
				name = maybeName;
			if (maybeLink.length() > 0)
				link = maybeLink;
			sc.close();
		}
		catch(Exception e){
			System.out.println("Using defaults, loser.");
		}

		
		int elfAccountID = db.getAccountForElfName(name);
		db.getBills(elfAccountID, name);
		db.getLink(link);
		db.getInfo(name);
		db.getView();
		db.onlyView();
		db.longerPlayedvid();
	}


}

class MyDatabase{
	private Connection connection;
	private final String accountsTXT = "accounts.txt";
	private final String videosTXT = "videos.txt";
	private final String viewsTXT = "views.txt";

	public MyDatabase(){
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			// creates an in-memory database
			connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");

			createTables();
			readInData();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	
	private void createTables(){
		// To be completed
		String accounts = "create table accounts ( "+
			" accountID integer,"+
            " billingAddress VARCHAR(100),"+
            " primary key(accountID)" +
			")";
		try {
		   
			connection.createStatement().executeUpdate(accounts);

            String bills = "create table bills ("
                + " billID integer," 
                + " amount integer,"
                + " accountID integer, "
                + " primary key(billID),"
                + " foreign key (accountID) references accounts ON DELETE CASCADE);";

			connection.createStatement().executeUpdate(bills);
			
			
			
			String userInfo = "create table userInfo ("
                + " userID integer IDENTITY," 
                + " username VARCHAR(100),"
                + " accountID integer, "
                + " primary key(userID),"
                + " foreign key (accountID) references accounts ON DELETE CASCADE);";

			connection.createStatement().executeUpdate(userInfo);
			
			
			
			
			
			String videoDetails = "create table videoDetails ("
                + " link VARCHAR(100),"
                + " videoname VARCHAR(100),"
                + " vlength integer,"
                + " cID integer, "
                + " primary key(link),"
                + " foreign key (cID) references userInfo (userID) ON DELETE CASCADE);";

			connection.createStatement().executeUpdate(videoDetails);
			
		
			String viewInfo = "create table viewInfo ("
                + " vID integer IDENTITY," 
                + " date integer,"
                + " videolink VARCHAR(100), "
                + " viewerID integer, "
                + " primary key(vID),"
                + " foreign key (videolink) references videoDetails (link) ON DELETE CASCADE,"
                + " foreign key (viewerID) references userInfo (userID));";

			connection.createStatement().executeUpdate(viewInfo);

			
			String vTemp = "create table vTemp ("
                + " accountID integer," 
                + " viewername VARCHAR(100),"
                + " link VARCHAR(100),"
                + " date integer);";

			connection.createStatement().executeUpdate(vTemp);

			String vidTemp = "create table vidTemp ("
				+ " creatorname VARCHAR(100),"
                + " videoname VARCHAR(100),"
                + " link VARCHAR(100),"
                + " vlength integer);";

			connection.createStatement().executeUpdate(vidTemp);
			
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}

	}



	public int getAccountForElfName(String elfName){
		/*
		 * To be CORRECTED and completed. Just an example of how this can work. You will have to add more tables to the FROM statement
		 */
		int aID = -1;
		System.out.println("Q1 - account for " + elfName);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select * from userInfo where username=?;"
			);
			pstmt.setString(1, elfName);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getInt("accountID");
				System.out.println(elfName + " is associated with account " + aID);
			}

			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return aID;
	}//getAccountForElfName

	public void getBills(int elfID, String elfName){
		
		System.out.println("Q2 - Bills for " + elfName);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select * from bills where accountID=?;"
			);
			pstmt.setInt(1, elfID);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println(elfName + " has bill " + resultSet.getInt("billID") + " which is for " + resultSet.getInt("amount"));
			}

			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//getBills

	public void getLink(String link){
		
		System.out.println("Q3 - Views for video with link " + link);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select videoDetails.videoname, videoDetails.link, videoviews.totalviews " + 
				"from videoDetails left join (" + 
				"select viewInfo.videolink, count(viewInfo.vID) as totalviews " + 
				"from viewInfo group by viewInfo.videolink having viewInfo.videolink = ?) as videoviews " + 
				"on videoDetails.link = videoviews.videolink " + 
				"where videoDetails.link=?;"
			);
			pstmt.setString(1, link);
			pstmt.setString(2, link);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println(resultSet.getString("videoname") + "/" + resultSet.getString("link") + " has " + resultSet.getInt("totalviews") + " viewInfo");
			}

			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//getLink

	public void getInfo(String elfName){
		
		System.out.println("Q4 - videoDetails for " + elfName + " and number of viewInfo");
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"SELECT videolink, videoname, count(vID) as totalviews " +
				"from viewInfo " +
				"right join (select link, videoname from videoDetails " +
				"left join userInfo on cID = userID " +
				"where username = ?) as elfvideos " +
				"on videolink = link group by videolink, videoname;"
			);
			pstmt.setString(1, elfName);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println(elfName + "'s video " +resultSet.getString("videoname") + " has " + resultSet.getInt("totalviews") + " viewInfo");
			}

			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//getInfo

	public void getView(){
		
		System.out.println("Q5 - viewInfo of videoDetails with no other viewInfo than the creator");
		try{

			String s = "SELECT videoname " +
			"from videoDetails " +
			"left join viewInfo on videolink = link " +
			"where cID = viewerID AND link in (select vw.videolink from viewInfo vw " +
			"group by vw.videolink " +
			"having count(vw.vID) = 1) ;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(s);

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println("Video " + resultSet.getString("videoname") + " has no other viewInfo");
			}

			statement.close();
			resultSet.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//getView
	
	public void onlyView(){
		
		System.out.println("Q6 - Viewers who are the only viewer of a video that is NOT the creator");
		try{

			String s = "SELECT videoname, username " +
			"from videoDetails " +
			"left join viewInfo on videolink = link " +
			"left join userInfo on userID = viewerID " +
			"where link in (select vw.videolink from viewInfo vw " +
			"left join videoDetails vd on vw.videolink = vd.link " +
			"where vw.viewerID != vd.cID " +
			"group by vw.videolink " +
			"having count(vw.vID) = 1) " +
			"AND viewerID != cID;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(s);

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println("Video " + resultSet.getString("videoname") + " has no other viewInfo othern than " + resultSet.getString("username"));
			}

			statement.close();
			resultSet.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//onlyView

	public void longerPlayedvid(){
		
		System.out.println("Q7 - Users with the most minutes view");
		try{

			String s = "select videoname, viewcount*vlength as totalminutes, " + 
			"username from videoDetails " + 
			"left join userInfo on userID = cID " + 
			"left join (select videolink, count(vID) as viewcount from viewInfo " + 
			"group by videolink) as totalviews on totalviews.videolink = link " + 
			"order by totalminutes desc NULLS LAST limit 5";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(s);

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists.
				System.out.println("Video " + resultSet.getString("videoname") + " by " + 
					resultSet.getString("username") + " has total time " + resultSet.getInt("totalminutes") + " minutes");
			}

			statement.close();
			resultSet.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//longerPlayedvid

	private void readInData(){

		// to be corrected and completed
		
		BufferedReader in = null;
		
		
		
		try {
			in = new BufferedReader((new FileReader(accountsTXT)));
		
			// throw away the first line - the header
			
			
			in.readLine();

			// pre-load loop
			String line = in.readLine();
			while (line != null) {
				//System.out.println(line);
				// split naively on commas
				// good enough for this dataset!
				String[] parts = line.split(",");
				if(parts.length >= 4){
					makeAccount(parts[0], parts[2]);
					
					makeBills(parts[0], parts[1], parts[3]);
				}
				// get next line
				line = in.readLine();
			}
			in.close();
		} catch (IOException e) {
			System.out.println("Error in IO accountsTXT");
			e.printStackTrace();
		}

		//Videos.txt
		try {
			in = new BufferedReader((new FileReader(videosTXT)));
		
			// throw away the first line - the header
			in.readLine();

			// pre-load loop
			String line = in.readLine();
			while (line != null) {
				// split naively on commas
				// good enough for this dataset!
				String[] parts = line.split(",");
				if(parts.length >= 3){
					PreparedStatement ptrt = connection.prepareStatement(
					"insert into vidTemp (creatorname, videoname, link, vlength) values (?, ?, ?, ?);"
					);
					ptrt.setString(1, parts[0] );
					ptrt.setString(2, parts[1] );
					ptrt.setString(3, parts[2] );
					ptrt.setInt(4, Integer.parseInt(parts[3]) );
					int numUpdated= ptrt.executeUpdate();
					//makeVideoRoot(parts[0], parts[1], parts[2], parts[3]);
				}
				// get next line
				line = in.readLine();
			}
			in.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}catch (IOException e) {
			System.out.println("Error in IO videosTXT");
			e.printStackTrace();
		}


		//Views.txt
		try {
			in = new BufferedReader((new FileReader(viewsTXT)));
		
			// throw away the first line - the header
			in.readLine();

			// pre-load loop
			String line = in.readLine();
			while (line != null) {
				// split naively on commas
				// good enough for this dataset!
				String[] parts = line.split(",");
				if(parts.length >= 3){
					PreparedStatement ptvt = connection.prepareStatement(
					"insert into vTemp (accountID, viewername, link, date) values (?, ?, ?, ?);"
					);
					ptvt.setInt(1, Integer.parseInt(parts[0]) );
					ptvt.setString(2, parts[1] );
					ptvt.setString(3, parts[2] );
					ptvt.setInt(4, Integer.parseInt(parts[3]) );
					int numUpdated= ptvt.executeUpdate();
					
				}
					
				// get next line
				line = in.readLine();
			}
			in.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}

		
         createUser();
		createVid();
		createView();
		dTable();
		
	}//readInData

	private void makeAccount(String accountID, String billingAddress){
		/*
		 * Really make or create account. Return the account ID
		 * whether it is new, or give the existing one if it already exists
		 */
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select accountID From accounts where accountID = ?;"
			);
			pstmt.setInt(1, Integer.parseInt(accountID));

			ResultSet resultSet = pstmt.executeQuery();

			if (!resultSet.next()) {
				// no record
				// make the new account
                PreparedStatement addAccount = connection.prepareStatement(
					"insert into accounts (accountID, billingAddress) values (?, ?);"
				);
                
                addAccount.setInt(1, Integer.parseInt(accountID) );
                addAccount.setString(2, billingAddress);
                int numUpdated= addAccount.executeUpdate();
                
				addAccount.close();
            
				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in makeAccount: " + accountID + " " +billingAddress);
			e.printStackTrace(System.out);
		}

	}//makeAccount

	private void makeBills(String accountID , String billID, String amount){
		
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select billID From bills where billID = ?;"
			);
			pstmt.setInt(1, Integer.parseInt(billID));

			ResultSet resultSet = pstmt.executeQuery();

			if (!resultSet.next()) {
				// no record
				// add new row
                PreparedStatement addBill = connection.prepareStatement(
					"insert into bills (billID, amount, accountID) values (?, ?, ?);"
				);
                
                addBill.setInt(1, Integer.parseInt(billID) );
				addBill.setInt(2, Integer.parseInt(amount));
				addBill.setInt(3, Integer.parseInt(accountID) );
                int numUpdated= addBill.executeUpdate();
                
				addBill.close();
            
				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in makeBills: " + billID + " " + accountID + " " + amount);
			//e.printStackTrace(System.out);
		}

	}//makeBills

	private void createUser(){
		

		try{
			String sql = "Select * from vTemp;";

			Statement statement = connection.createStatement();
			ResultSet rtSet = statement.executeQuery(sql);

			while (rtSet.next()) {
                
				try{
					PreparedStatement pstmt = connection.prepareStatement(
						"Select username From userInfo where username = ?;"
					);
					pstmt.setString(1, rtSet.getString("viewername"));
		
					ResultSet resultSet = pstmt.executeQuery();
		
					if (!resultSet.next()) {
						// no record
						// add new row
						PreparedStatement addUser = connection.prepareStatement(
							"insert into userInfo (username, accountID) values (?, ?);"
						);
						
						addUser.setString(1, rtSet.getString("viewername"));
						addUser.setInt(2, rtSet.getInt("accountID") );
						int numUpdated= addUser.executeUpdate();
						
						addUser.close();					
						resultSet.close();
					}
					pstmt.close();
				}
				catch (SQLException e) {
					
					e.printStackTrace(System.out);
				}
			}
			rtSet.close();
			statement.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		try{
			String sql = "Select * from userInfo;";

			Statement statement = connection.createStatement();
			ResultSet rtSet = statement.executeQuery(sql);

			
			rtSet.close();
			statement.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
		
	}//createUser

	private void createVid(){
		

		try{
			String sql = "Select * from vidTemp;";

			Statement statement = connection.createStatement();
			ResultSet rtSet = statement.executeQuery(sql);

			while (rtSet.next()) {
                try{
					PreparedStatement pstmt = connection.prepareStatement(
						"Select link From videoDetails where link = ?;"
					);
					pstmt.setString(1, rtSet.getString("link"));
		
					ResultSet resultSet = pstmt.executeQuery();

					if (!resultSet.next()) {
						// no record
						// add new row

						//find userID
						PreparedStatement userID = connection.prepareStatement(
							//"Select * from userInfo where userID = 22;"
						"Select * from userInfo where username = ?;"
						);

						userID.setString(1, rtSet.getString("creatorname"));
			
						ResultSet userSet = userID.executeQuery();

						if(userSet.next()){

							PreparedStatement ptvt = connection.prepareStatement(
								"insert into videoDetails (link, videoname, vlength, cID) values (?, ?, ?, ?);"
							);
							
							ptvt.setString(1, rtSet.getString("link"));
							ptvt.setString(2, rtSet.getString("videoname"));
							ptvt.setInt(3, rtSet.getInt("vlength") );
							ptvt.setInt(4, userSet.getInt("userID") );
							int numUpdated= ptvt.executeUpdate();
							
							ptvt.close();

							userSet.close();
							userID.close();
						}
					
						resultSet.close();
					}
					pstmt.close();
				}
				catch (SQLException e) {
				
					e.printStackTrace(System.out);
				}
			}
			rtSet.close();
			statement.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}//createVid

	
	private void createView(){
		

		try{
			String sql = "Select * from vTemp;";

			Statement statement = connection.createStatement();
			ResultSet rtSet = statement.executeQuery(sql);

			while (rtSet.next()) {
                try{
					//get userID
					PreparedStatement getUserID = connection.prepareStatement(
						"Select userID From userInfo where username = ?;"
					);
					getUserID.setString(1, rtSet.getString("viewername"));
					ResultSet viduID = getUserID.executeQuery();

					if(viduID.next()){

						//uniqueness
						PreparedStatement pstmt = connection.prepareStatement(
							"Select date, videolink, viewerID From viewInfo where date = ? AND videolink = ? AND viewerID = ?;"
						);
						pstmt.setInt(1, rtSet.getInt("date"));
						pstmt.setString(2, rtSet.getString("link"));
						pstmt.setInt(3, viduID.getInt("userID"));
						ResultSet resultSet = pstmt.executeQuery();
			
						if (!resultSet.next()) {
							// no record
							// add new row

							PreparedStatement atvt = connection.prepareStatement(
								"insert into viewInfo (date, videolink, viewerID) values (?, ?, ?);"
							);
							
							atvt.setInt(1, rtSet.getInt("date") );
							atvt.setString(2, rtSet.getString("link"));
							atvt.setInt(3, viduID.getInt("userID"));
							int numUpdated= atvt.executeUpdate();
							
							atvt.close();

							viduID.close();
							getUserID.close();
											
							resultSet.close();
						}
						pstmt.close();
					}
				}
				catch (SQLException e) {
					e.printStackTrace(System.out);
				}
			}
			
					
			rtSet.close();
			statement.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	
	}//createView

	
	
	private void dTable(){
		String vTemp = "DROP TABLE vTemp;";
		
		String vidTemp = "DROP TABLE vidTemp;";
		try {
			connection.createStatement().executeUpdate(vTemp);
			connection.createStatement().executeUpdate(vidTemp);
		}
		catch (SQLException e) {
			
		
			e.printStackTrace(System.out);
		}
	}//dTable




}