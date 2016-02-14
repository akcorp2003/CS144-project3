package edu.ucla.cs.cs144;

import org.apache.commons.lang3.StringEscapeUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
	
	private IndexSearcher _indexsearcher = null;
	private QueryParser _queryparser = null;
	
	public AuctionSearch() throws IOException{
		_indexsearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/index1"))));
		_queryparser = new QueryParser("Info", new StandardAnalyzer());
	}
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, int numResultsToReturn) {
		
		ArrayList<SearchResult> my_final_results = new ArrayList<SearchResult>();
		
		try {
			TopDocs results = performSearch(query, numResultsToSkip + numResultsToReturn);
			ScoreDoc[] hits = results.scoreDocs;
			
			//account for the number of results the user wants to skip
			for(int i = numResultsToSkip, j = 0; i < hits.length; i++, j++){
				SearchResult result = new SearchResult(getDocument(hits[i].doc).get("ItemID"), getDocument(hits[i].doc).get("Name"));
				my_final_results.add(result);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return (SearchResult[])(my_final_results.toArray(new SearchResult[my_final_results.size()]));
	}
	
	private TopDocs performSearch(String query, int n)
	throws IOException, ParseException {
		Query my_query = _queryparser.parse(query);
		return _indexsearcher.search(my_query, n);
	}
	
	private Document getDocument(int docID) throws IOException{
		return _indexsearcher.doc(docID);
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		//first create connection to SQL
		//SearchResult[] finalresults = new SearchResult[numResultsToReturn];
		ArrayList<SearchResult> finalresults = new ArrayList<SearchResult>();
		try {
			Connection my_db_conn = DbManager.getConnection(true);
			
			//build the spatial index
			//buildSpatialIndex(my_db_conn);
			
			//get the regional items
			ResultSet results = getRegionalItems(my_db_conn, region);
			//to enable faster searching, put it into a hashset
			Set<String> results_set = new HashSet<String>();
			while(results.next()){
				if(!results_set.contains(results.getString("ItemID"))){
					results_set.add(results.getString("ItemID"));
				}
			}
			
			//run a basic search
			SearchResult[] basicresults = basicSearch(query, 0, Integer.MAX_VALUE);
			
			//if the user wants to skip a lot of results, we just return our length 0 array
			if(basicresults.length <= numResultsToSkip){
				return finalresults.toArray(new SearchResult[finalresults.size()]);
			}
			
			//for each basicresult, we need to check if it is in results, if so, we can add it to the finalresults array
			for(int i = numResultsToSkip; i < basicresults.length; i++){
				String id = basicresults[i].getItemId();
				if(results_set.contains(id)){
					SearchResult newresult = new SearchResult(basicresults[i].getItemId(), basicresults[i].getName());
					finalresults.add(newresult);
				}
			}//end for
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		return finalresults.toArray(new SearchResult[finalresults.size()]);
	}
	
	/**
	 * Builds the spatial index. Assumes that the SQL script file for creating SpatialItems is already
	 * created.
	 * 
	 * @param my_db_conn The connection to the database.
	 */
	private void buildSpatialIndex(Connection my_db_conn){
		//build the spatial index
		try {
			Statement my_statem = my_db_conn.createStatement();
			/*Statement my_statem = my_db_conn.createStatement();
			my_statem.executeUpdate("CREATE TABLE SpatialItems(ItemID varchar(80), LatLong POINT NOT NULL, SPATIAL INDEX(LatLong)) ENGINE=MyISAM");*/
			
			//fetch the tables
			String itemid, latitude, longitude;
			
			ResultSet items = my_statem.executeQuery("Select * FROM Item");
			
			PreparedStatement preparePointInsertion = my_db_conn.prepareStatement
			(
					"INSERT INTO SpatialItems(ItemID, LatLong) VALUES(?, POINT(?, ?))"
					
			);
			
			while(items.next()){
				itemid = items.getString("ItemID");
				latitude = items.getString("Latitude");
				longitude = items.getString("longitude");
				
				preparePointInsertion.setString(1, itemid);
				preparePointInsertion.setString(2, latitude);
				preparePointInsertion.setString(3, longitude);
				preparePointInsertion.executeUpdate();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	/**
	 * Gets the regional items based on the region
	 * 
	 * @param my_db_conn The connection to the database
	 * @param region The rectangle to look for the items
	 * @return A ResultSet containing ItemIDs of Items that lie within the specified region.
	 */
	private ResultSet getRegionalItems(Connection my_db_conn, SearchRegion region){
		ResultSet geo_items = null;
		try {
			Statement my_statem = my_db_conn.createStatement();
			
			double lx = region.getLx();
			double ly = region.getLy();
			double rx = region.getRx();
			double ry = region.getRy();
			
			String polygonstring = String.format("SELECT ItemID FROM SpatialItems WHERE MBRContains(GeomFromText('Polygon((%f %f, %f %f, %f %f, %f %f, %f %f))'), LatLong)", lx, ly, lx, ry, rx, ry, rx, ly, lx, ly);
			System.out.println(polygonstring);
			/*String polygonstring = "SET @rect = 'Polygon((" + 
			Double.toString(region.getLx()) + " " + Double.toString(region.getLy()) + ", " + 
			Double.toString(region.getLx()+region.getRx())+ " " + Double.toString(region.getLy()) + ", " + 
			Double.toString(region.getRx()) + " " + Double.toString(region.getRy()) + ", " +
			Double.toString(region.getLx()) + " " + Double.toString(region.getRy()) + ", " +
			Double.toString(region.getLx()) + " " + Double.toString(region.getLy()) + "))';";
			
			System.out.println(polygonstring);
			PreparedStatement preparePolygon = my_db_conn.prepareStatement
			(
					polygonstring
			);*/
			/*
			//set lower left
			preparePolygon.setDouble(1, region.getLx());
			preparePolygon.setDouble(2, region.getLy());
			//set lower right
			preparePolygon.setDouble(3, region.getLx()+region.getRx());
			preparePolygon.setDouble(4, region.getLy());
			//set upper right
			preparePolygon.setDouble(5, region.getRx());
			preparePolygon.setDouble(6, region.getRy());
			//set upper left
			preparePolygon.setDouble(7, region.getLx());
			preparePolygon.setDouble(8, region.getRy());
			//connect it back to the beginning
			preparePolygon.setDouble(9, region.getLx());
			preparePolygon.setDouble(10, region.getLy());*/
			
			//preparePolygon.execute();
			
			geo_items = my_statem.executeQuery(polygonstring);
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return geo_items;
	}
	
	private String FormatDate(String date) {
	      DateFormat sqldate = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
	      DateFormat xmldate = new SimpleDateFormat("MMM-dd-yy kk:mm:ss");
	      try {
	         Date d = sqldate.parse(date);
	         return xmldate.format(d);
	      } catch ( java.text.ParseException e) {
	         e.printStackTrace();
	         return "";
	      }
	   }
	
	public String getBids(String itemid, Connection conn){
		
		try {
			String bidquery = String.format("SELECT UserID, Time, Amount FROM Bid WHERE ItemID = \"%s\"", itemid);
		    Statement bidstatement = conn.createStatement();
		    ResultSet bids = bidstatement.executeQuery(bidquery);
		    
		    String b = "";
		    b += "<Bids>\n";
	    
			while(bids.next()){
				String userid = bids.getString("UserID");
				PreparedStatement bidstatement2 = conn.prepareStatement("SELECT * FROM User WHERE UserID=" + userid);
				ResultSet userdata = bidstatement2.executeQuery();
				String location = null, country = null, bidrating = null;
				if(userdata.next()){
					location = userdata.getString("Location");
					country = userdata.getString("Country");
					bidrating = userdata.getString("Bid_Rating");
				}
				b += "<Bid>\n";
				b += String.format("<Bidder Rating=\"%s\" UserID=\"%s\">\n", bidrating, StringEscapeUtils.escapeXml10(userid));
				if(location != null){
					b += String.format("<Location>%s</Location>\n", location);
				}
				if(country != null){
					b += String.format("<Country>%s</Country>\n", country);
				}
				b += "</Bidder>\n";
				b += String.format("<Time>%s</Time>\n", FormatDate(bids.getString("Time")));
				b += String.format("<Amount>$%s</Amount>\n", bids.getString("Amount"));
				b += "</Bid>\n";
				
			}
			return b;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}

	   
	}

	public String getXMLDataForItemId(String itemId) {
		String xml = "";
		try {
			Connection my_db_conn = DbManager.getConnection(true);
			Statement searchitem = my_db_conn.createStatement();
			String itemquery = "SELECT * FROM Item WHERE ItemID='" + itemId+"'";
			ResultSet item = searchitem.executeQuery(itemquery);
			
			String catquery = "SELECT Category FROM ItemCat WHERE ItemID='" + itemId+"'";
			Statement catstatement = my_db_conn.createStatement();
			ResultSet cats = catstatement.executeQuery(catquery);
			
			if(item.next()){
				String name = item.getString("Name");
				String categories = "";
				while(cats.next()){
					categories += String.format("<Category>%s</Category>\n", StringEscapeUtils.escapeXml10(cats.getString("Category")));					
				}
				
				String current = item.getString("Currently");
				String buy = null;
				if(item.getDouble("Buy_Price") != 0){
					buy = Double.toString(item.getDouble("Buy_Price"));
				}
				String first = Double.toString(item.getDouble("First_Bid"));
				String number = item.getString("Number_of_Bids");
				String latitude = item.getString("Latitude");
				String longitude = item.getString("Longitude");
				String country = item.getString("Country");
				String start = item.getString("Started");
				String end = item.getString("Ends");
				String description = item.getString("Description");
				String seller = item.getString("UserID");
				PreparedStatement ps = my_db_conn.prepareStatement("SELECT Sell_Rating FROM User WHERE UserID='" + seller+"'");
				ResultSet rs = ps.executeQuery();
				String sellerrating = null;
				if(rs.next()){
					sellerrating = rs.getString("Sell_Rating");
				}
				xml += String.format("<Item ItemID=\"%s\">\n", itemId);
				xml += String.format("<Name>%s</Name>\n", StringEscapeUtils.escapeXml10(name));
				xml += categories;
				xml += String.format("<Currently>$%s</Currently>\n", current);
				if(buy != null){
					xml += String.format("<Buy_Price>$%s</Buy_Price>\n", buy);
				}
				xml += String.format("<First_Bid>$%s</First_Bid>\n", first);
				xml += String.format("<Number_of_Bids>%s</Number_of_Bids>\n", number);
				
				if(Integer.parseInt(number) > 0){
					xml += getBids(itemId, my_db_conn);
				}
				else{
					xml += "<Bids />\n";
				}
				if(latitude != null && longitude != null){
					xml += String.format("<Location Latitude=\"%s\" Longitude=\"%s\">%s</Location>\n",
							latitude,longitude,StringEscapeUtils.escapeXml10(item.getString("Location")));
				}
				else {
		            xml += String.format("<Location>%s</Location>\n", StringEscapeUtils.escapeXml10(item.getString("Location")));
		        }
				xml += String.format("<Country>%s</Country>\n", StringEscapeUtils.escapeXml10(country));
				xml += String.format("<Started>%s</Started>\n", FormatDate(start));
				xml += String.format("<Ends>%s</Ends>\n", FormatDate(end));
				xml += String.format("<Seller Rating=\"%s\" UserID=\"%s\" />\n", sellerrating, StringEscapeUtils.escapeXml10(seller));
				xml += String.format("<Description>%s</Description>\n", StringEscapeUtils.escapeXml10(description));
				xml += "</Item>";
			}
			return xml;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
		
		
	}
	
	public String echo(String message) {
		return message;
	}

}
