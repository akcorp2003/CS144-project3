package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
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
		_queryparser = new QueryParser("nothing", new StandardAnalyzer());
	}
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, int numResultsToReturn) {
		
		SearchResult[] results_array = new SearchResult[numResultsToReturn];
		
		try {
			TopDocs results = performSearch(query, numResultsToSkip + numResultsToReturn);
			ScoreDoc[] hits = results.scoreDocs;
			
			//account for the number of results the user wants to skip
			for(int i = numResultsToSkip, j = 0; i < hits.length; i++, j++){
				SearchResult result = new SearchResult(getDocument(hits[i].doc).get("ItemID"), getDocument(hits[i].doc).get("Name"));
				results_array[j] = result;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return results_array;
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
		try {
			Connection my_db_conn = DbManager.getConnection(true);
			
			//build the spatial index
			buildSpatialIndex(my_db_conn);
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		return new SearchResult[0];
	}
	
	private void buildSpatialIndex(Connection my_db_conn){
		//build the spatial index
		try {
			Statement my_statem = my_db_conn.createStatement();
			my_statem.executeUpdate("CREATE TABLE SpatialItems(ItemID varchar(80), LatLong POINT NOT NULL, SPATIAL INDEX(LatLong)) ENGINE=MyISAM");
			
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
	
	private ResultSet getRegionalItems(Connection my_db_conn, SearchRegion region){	
		try {
			Statement my_statem = my_db_conn.createStatement();
			
			PreparedStatement preparePolygon = my_db_conn.prepareStatement
			(
					"SET @rect = "
					+ "'Polygon((? ?, ? ?, ? ?, ? ?, ? ?))'"
			);
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}