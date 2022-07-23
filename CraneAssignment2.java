/* 
* Gavin Crane
* 11/21/2021
* CraneAssignment2.java
*  
* This program outputs stock split data and executes a buying/selling strategy with said stock data. The user inputs 
* a company ticker and optional date range. The program will use this information to build a mysql query to request 
* the information from a database. The program will then iterate through the data outputting stock splits: How many,
* what type, and when they occurred. After that, it will execute a buying and selling strategy on the data and report
* total transactions and net cash. Input format: "ticker start end" i.e "INTC 1980.01.01 1999.12.31" no trading will
* occur if there's less than 51 days of data and the database connection will close once an empty string is entered.
* 
* Note: some of this code was taken from the provided skeleton code (mostly just gdbc and mysql connection)
*/
import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.PrintWriter;

class CraneAssignment2 {   
   /*
   * This class retrieves all the data from given ticker and date range in descending order. It uses an sql query to 
   * select the TransDate, OpenPrice, HighPrice, LowPrice, and ClosePrice of every day within the date range and 
   * puts it into a 2d array: (i)[j] where i is the day and j is the information for that day. returns the 2d array
   */
   static class StockData {	   
	    
	 String ticker;
	 // date range
	 String start;
	 String end;
	 	 
	 List<String[]> getDataT() throws SQLException {
		 
		 List<String[]> dArray = new ArrayList<>();
		 PreparedStatement pstmt;
		 
		 // no date range given
		 if (start == null && end == null) {
			 pstmt = conn.prepareStatement(
					   "select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice "+
					   "	from pricevolume "+
					   "	where Ticker = ?"+
					   "	order by TransDate DESC"
					   );
			 pstmt.setString(1, ticker);
		   }		
		 else {
			 // date and range given
			 pstmt = conn.prepareStatement(
					 	"select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice "+
						"	from pricevolume "+
						"	where TransDate Between ? and ? and Ticker = ?"+
						"	order by TransDate DESC"
					 	);
	
		     pstmt.setString(1, start);
		     pstmt.setString(2, end);
		     pstmt.setString(3, ticker);
		 }		   
		 ResultSet rs = pstmt.executeQuery();
		   
		 // populate 2d array with query results
		 while (rs.next()) {	
     		 String[] row = new String[5];
     		 for (int i = 1; i <= 5; i++) {
     			 Object obj = rs.getObject(i);
     			 row[i-1] = (obj ==null) ?null:obj.toString();    			   
     		 }
     		 dArray.add(row);     		
		 }
		 pstmt.close();
		 return dArray;		 
	 } 
   }

   static Connection conn;
   static final String prompt = "Enter ticker symbol [start/end dates]: ";
   
   public static void main(String[] args) throws Exception {
	   
	  // read database login file
      String paramsFile = "readerparams.txt";
      if (args.length >= 1) {
         paramsFile = args[0];
      }
      Properties connectprops = new Properties();
      connectprops.load(new FileInputStream(paramsFile));
      
      try {
    	 // sql gdb connection and login 
         Class.forName("com.mysql.jdbc.Driver");
         String dburl = connectprops.getProperty("dburl");
         String username = connectprops.getProperty("user");
         conn = DriverManager.getConnection(dburl, connectprops);
         System.out.printf("Database connection %s %s established.%n", dburl, username);
         
         // get user input
         Scanner in = new Scanner(System.in);
         System.out.print(prompt);
         String input = in.nextLine().trim();
         
         // parse input and check if a date range is given
         while (input.length() > 0) {
            String[] params = input.split("\\s+");
            String ticker = params[0];
            String startdate = null, enddate = null;
            // if there is a ticker save it and execute program. if there is dates, save them, otherwise dates = null
            if (getName(ticker)) {
               if (params.length >= 3) {
                  startdate = params[1];
                  enddate = params[2];                            
               }
               // get stock data, output splits, and adjust data
               List<double[]> data = getStockData(ticker, startdate, enddate);
               System.out.println();
               System.out.println("Executing investment strategy");      
               // execute buying/selling strategy on adjusted data
               doStrategy(data);                             
            }             
            System.out.println();
            System.out.print(prompt);
            input = in.nextLine().trim();
         }
         // user entered empty string, terminate connection
         System.out.println("Database connection closed.");
         conn.close();
      } catch (SQLException ex) {
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                           ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }
   }
  
   // This method takes the input company ticker and prints the company name, returns false if it doesn't exist   
   static boolean getName(String ticker) throws SQLException {	
	   PreparedStatement pstmt = conn.prepareStatement(
			   "select Name " +
			   "	from company " +
			   "	where Ticker = ?");
	   
	   pstmt.setString(1,  ticker);
	   ResultSet rs = pstmt.executeQuery();
	   
	   if (rs.next()) {
		   System.out.printf("%s%n", rs.getString("Name"));
		   pstmt.close();
		   return true;
	   }
	   else {
		   System.out.println(ticker + " is not in database");
		   return false;
	   }  
   }
   /*
   * This method takes the ticker and dates if given, then outputs all stock split data, it also populates a new
   * data table with adjusted values and returns it. These adjusted values are to accommodate for stock splits so we
   * can analyze it later for our buying/selling strategy in doStrategy
   */
   static List<double[]> getStockData(String ticker, String start, String end) {	  	  	   
	   // create data table for ticker and the given date range
	   List<String[]> dataT;	 
	   StockData TickerData = new StockData();
	   TickerData.ticker = ticker;
	   TickerData.start = start;
	   TickerData.end = end;	     
	   // new data table (accommodated stock splits)
	   List<double[]> adjustedDataT = new ArrayList<>(); 
	   
	   try {
		   int splitCounter = 0;
		   int i;		  
		   dataT = TickerData.getDataT();
		   int dataTSize = dataT.size();		   
		   // accommodate for stock splits
		   double divisor = 1;		   
		   /*
		   * This loop accomplishes two tasks, in the name of efficiency. Firstly it outputs all stock 
		   * split data and the date it occurred along with the amount of splits and total trading days 
		   * (reused code from assignment 1). Secondly, this loop modifies and adds each days' data to 
		   * a new data table that will be used to calculate the buy/sell strategy. It is slightly modified 
		   * in that for every split that occurs that days data and the data after is adjusted to 
		   * accommodate for the split price (divisor). 
		   */		   		   
		   for (i = 0; i < dataTSize-1; i++) {
			   
			   double opening = Double.parseDouble(dataT.get(i)[1]);
	           double closing = Double.parseDouble(dataT.get(i+1)[4]);	           
	           String closingDay = dataT.get(i+1)[0];	   
	           
	           if (splitType(closing, opening).equals("2:1")) {
	        	   System.out.printf("2:1 Split on " + closingDay + " ");
	        	   System.out.printf("%.2f", closing);
        		   System.out.printf(" ---> ");
        		   System.out.printf("%.2f%n", opening);	        	   
       			   splitCounter++;
       			   // update divisor for new data table
       			   divisor = divisor*2;       			   
       		   }
	           else if (splitType(closing, opening).equals("3:1")) {
	        	   System.out.printf("3:1 Split on " + closingDay + " ");
	       		   System.out.printf("%.2f", closing);
    			   System.out.printf(" ---> ");
    			   System.out.printf("%.2f%n", opening);
       			   splitCounter++;
       			   // update divisor for new data table
       			   divisor = divisor*3;
       		   }
	           else if (splitType(closing, opening).equals("3:2")) {
	        	   System.out.printf("3:2 Split on " + closingDay + " ");
	       		   System.out.printf("%.2f", closing);
    			   System.out.printf(" ---> ");
    			   System.out.printf("%.2f%n", opening);
       			   splitCounter++;
       			   // update divisor for new data table
       			   divisor = divisor*(1.5);
       		   }	           
	           /* 
	           * take the current iteration of the data and turn in into a double array 
	           * while adjusting the values to remove stock splits. Finally, add it to 
	           * the new data table. Data points: [#]: open, high, low, close
	           */	           
	           // two arrays here are used to avoid index out of bound errors 	           
	           double[] doubleLine0= new double[4];
	           double[] doubleLine = new double[4];			
			   // first elements' data adjusted and added to new table
			   if(i == 0) {
				   for (int j = 1; j<= 4; j++) {
					   doubleLine0[j-1] = Double.parseDouble(dataT.get(0)[j])/divisor;
				   }
				   adjustedDataT.add(doubleLine0);		   
			   }			   
			   // rest of the data adjusted and added to new table   
			   for (int j = 1; j<= 4; j++) {
				   doubleLine[j-1] = Double.parseDouble(dataT.get(i+1)[j])/divisor;	   
			   }
			   adjustedDataT.add(doubleLine);			  			 
		   }
		   System.out.println(splitCounter + " splits in " + (i+1) + " trading days");		   
	   }  
	   catch (SQLException ex) { 
		   System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                   ex.getMessage(), ex.getSQLState(), ex.getErrorCode()); 
	   }    
      return adjustedDataT;
   }
   /*
   * This Method takes the ticker data table, which is an arraylist of doubles, processes the information
   * and executes transactions based on the buying and selling criteria then outputs total net cash and
   * the amount of transactions that occurred
   */
   static void doStrategy(List<double[]> adjustedDataT) {	   
	   // reverse table to more easily iterate through dates (ascending) 
	   Collections.reverse(adjustedDataT);	   	     
	   // track cash and shares
	   double totalCash = 0;
	   double totalShares = 0;	   
	   // required data points
	   double open;
	   double close;
	   double nextOpen;
	   double prevClose;
	   double fiftyTotal;
	   double avg;	   
	   // count transactions
	   int trans = 0;	   	  	   
	   // iterate over the adjusted data and start buying/selling
	   for (int i = 0; i < adjustedDataT.size()-1; i++) {				
		   // start tracking 50 day average at day 51  
		   if (i > 49) {				   
			   // data points:
			   //(i)[#]: open, high, low, close
			   open = adjustedDataT.get(i)[0];
			   close = adjustedDataT.get(i)[3];
			   nextOpen = adjustedDataT.get(i+1)[0];		  
			   prevClose = adjustedDataT.get(i-1)[3];  
			   
			   // maintain 50 day average
			   fiftyTotal = 0;
			   for (int j = i-50; j < i; j++) {
				   fiftyTotal = fiftyTotal + adjustedDataT.get(j)[3];				  
			   }		   
			   avg = fiftyTotal/50;
			   
			   // start trading/selling after day 50
			   if (i >= 50) {
				   /*
				   * (Buy criterion) If the close(d) < 50-day average and close(d) is less than
				   * open(d) by 3% or more (close(d) / open(d) <= 0.97), buy 100 shares of the stock
				   * at price open(d+1). Add +1 to the transaction counter, -8 to cash for transaction
				   * fee
	               */
				   if ((close < avg) && ((close/open) < 0.97000001)) {	
						   totalCash = totalCash - (100*nextOpen);
						   totalShares = totalShares + 100;
						   totalCash = totalCash - 8;
						   trans += 1;					   					   					   
				   }			   	   
				   /*
				   * (Sell criterion) If the buy criterion is not met, then if shares >= 100 and
				   * open(d) > 50-day average and open(d) exceeds close(d-1) by 1% or more
	               * (open(d) / close(d-1) >= 1.01), sell 100 shares at price (open(d) + close(d))/2.
				   */			    
				   else if (totalShares >= 100) {
					   if (open > avg) {
						   if((open/prevClose) > 1.00999999) {						   
							   totalCash = totalCash + 100*((open + close)/2);
							   totalShares = totalShares - 100;
							   totalCash = totalCash - 8;  
							   trans += 1;
							   //System.out.println("sell "+ "day: " + (i+1) + " 100 shares @ " + ((open + close)/2) + " total shares = " + totalShares + "," + " cash = "+ totalCash);
						   }
					   }				   				   
				   }			 
				   /*
				   * After having processed the data through the second-to-last day, if there are any 
				   * shares remaining, on the last day, add open(d) * shares (No transaction fee 
				   * applies to this). Count the transaction
				   */
				   else if (i == adjustedDataT.size()-1) {
					   if (totalShares > 0) {
						   totalCash = totalCash + (open*totalShares);
						   trans += 1;
					   }
				   }
			   }			   
		   }		   		   
	   }		   	   	   	   
	   // output results
	   System.out.println("Transactions executed: " + trans);
	   System.out.printf("Net cash: ");
	   System.out.printf("%.2f%n", totalCash);
   }
   // takes opening and closing prices as parameters, returns split type if it exists as strings
   private static String splitType(double closing, double opening) {
   	
		double splitMath = closing/opening;
       	// 2:1
       	if (Math.abs(splitMath-2) < .2) {
       		return "2:1";
       	}
       	// 3:1
       	else if (Math.abs(splitMath-3) < .3){
       		return "3:1";	
       	}
       	// 3:2
       	else if (Math.abs(splitMath-1.5) < .15) {
       		return "3:2";
       	}
       	// not a split
       	else {
       		return "not a split";
       	}
   }      
}
