package edu.ucla.cs.cs144;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {
	
	private IndexWriter _writer = null;
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }
    
    public IndexWriter getIndexWriter(){
		if(_writer == null){
			Directory indexDir;
			try {
				indexDir = FSDirectory.open(new File("/var/lib/lucene/index1"));
				IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_2, new StandardAnalyzer());
				IndexWriter indexWriter = new IndexWriter(indexDir, config);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		return _writer;
	}
 
    public void rebuildIndexes() {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
		try {
		    conn = DbManager.getConnection(true);
		} catch (SQLException ex) {
		    System.out.println(ex);
		}
	
	
		/*
		 * Add your code here to retrieve Items using the connection
		 * and add corresponding entries to your Lucene inverted indexes.
	         *
	         * You will have to use JDBC API to retrieve MySQL data from Java.
	         * Read our tutorial on JDBC if you do not know how to use JDBC.
	         *
	         * You will also have to use Lucene IndexWriter and Document
	         * classes to create an index and populate it with Items data.
	         * Read our tutorial on Lucene as well if you don't know how.
	         *
	         * As part of this development, you may want to add 
	         * new methods and create additional Java classes. 
	         * If you create new classes, make sure that
	         * the classes become part of "edu.ucla.cs.cs144" package
	         * and place your class source files at src/edu/ucla/cs/cs144/.
		 * 
		 */
	
		
		getIndexWriter();
		
		try {
			
			//query all items in database
			Statement item_statement = conn.createStatement();
			ResultSet itemset = item_statement.executeQuery("SELECT ItemId, Name, Description FROM Item");
			
			//query all categories of a given item
			PreparedStatement cat_statement = conn.prepareStatement("SELECT Category FROM ItemCat WHERE ItemID = ?");
			
			//for each item
			while(itemset.next()){
				//find item information
				String itemid = itemset.getString("ItemId");
				String name = itemset.getString("Name");
				String description = itemset.getString("Description");
				
				//find all categories of that item and concatenate into one string
				cat_statement.setString(1, itemid);
				ResultSet catset = cat_statement.executeQuery();
				String categories = "";
				while(catset.next()){
					categories += catset.getString("Category");
					categories += " ";
				}
				
				//create new document, add fields
				Document doc = new Document();
				doc.add(new StringField("ItemId", itemid, Field.Store.YES));
				doc.add(new StringField("Name", name, Field.Store.YES));
				//"Info" is an index consisting of the concatenation of name, categories, and description
				doc.add(new TextField("Info", name + categories + description, Field.Store.NO));
				_writer.addDocument(doc);
				
				
			}
			
			if(_writer != null){
				_writer.close();
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	
	        // close the database connection
		try {
		    conn.close();
		} catch (SQLException ex) {
		    System.out.println(ex);
		}
    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
