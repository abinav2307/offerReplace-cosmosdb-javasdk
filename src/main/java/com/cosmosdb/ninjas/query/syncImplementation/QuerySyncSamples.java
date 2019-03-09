package com.cosmosdb.ninjas.query.syncImplementation;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.cosmosdb.ninjas.offerReplace.bootup.CosmosDBBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;

public class QuerySyncSamples {
    
    private DocumentClient documentClient;
    private String COLLECTION_LINK;
    
    public QuerySyncSamples() throws Exception {
        // Load account details from config.properties
        Properties settings = new Properties();
        InputStream propertiesInputStream =
            Main.class.getClassLoader().getResourceAsStream("config.properties");
        settings.load(propertiesInputStream);
        
        String accountName = settings.getProperty("cosmosDbAccountName");
        String accountKey = settings.getProperty("cosmosDbAccountKey");
        String databaseName = settings.getProperty("cosmosDbDatabaseName");
        String collectionName = settings.getProperty("cosmosDbCollectionName");
        
        this.COLLECTION_LINK = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        
        // Fetch the DocumentClient
        this.documentClient = CosmosDBBootupManager.bootup(accountName, accountKey);
    }
    
    /**
     * Executes a query with an IN clause
     */
    public void executeQueryWithInClause() {
        boolean hasMoreResults = true;
        FeedOptions queryFeedOptions = new FeedOptions();
        queryFeedOptions.setPageSize(100);
        queryFeedOptions.setEnableCrossPartitionQuery(true);
        
        String query = "SELECT * FROM c where c.pk in ('8502253', '3593673', '2568184') ";
        FeedResponse<Document> queryResponse = this.documentClient.queryDocuments(COLLECTION_LINK, query, queryFeedOptions);
        
        while (hasMoreResults) {
            try {
                List<Document> documents = queryResponse.getQueryIterable().fetchNextBlock();
                for (Document eachDocumentRetrieved : documents) {
                    System.out.println("Each id retrieved = " + eachDocumentRetrieved.getId());
                }
                
                hasMoreResults = queryResponse.getQueryIterator().hasNext();
                
                if(hasMoreResults) {
                    String continuationToken = queryResponse.getResponseContinuation();
                    queryFeedOptions.setRequestContinuation(continuationToken);
                }
            } catch (DocumentClientException e) {
                System.out.println("Exception thrown when running the query");
                e.printStackTrace();
            }
        }
    }
}
