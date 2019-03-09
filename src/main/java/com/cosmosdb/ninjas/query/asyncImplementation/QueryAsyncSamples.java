package com.cosmosdb.ninjas.query.asyncImplementation;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.InputStream;
import rx.Observable;
import rx.Subscriber;
//import rx.observable.ListenableFutureObservable;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.cosmosdb.ninjas.offerReplace.bootup.async.CosmosDBAsyncBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

public class QueryAsyncSamples {

    private AsyncDocumentClient asyncDocumentClient;
    private String COLLECTION_LINK;
    
    public QueryAsyncSamples() throws Exception {
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
        this.asyncDocumentClient = CosmosDBAsyncBootupManager.bootup(accountName, accountKey);
    }
    
    /**
     * Executes a query with an IN clause
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void executeQueryWithInClause() throws InterruptedException, ExecutionException {
        int requestPageSize = 10;
        
        FeedOptions options = new FeedOptions();
        options.setMaxItemCount(requestPageSize);
        options.setEnableCrossPartitionQuery(true);
        
        Observable<FeedResponse<Document>> documentQueryObservable = this.asyncDocumentClient
                .queryDocuments(this.COLLECTION_LINK, "SELECT * FROM c where c.pk in ('8502253', '3593673', '2568184') ", options);

        List<String> resultList = Collections.synchronizedList(new ArrayList<>());

        documentQueryObservable.map(FeedResponse::getResults)
        // Map the logical page to the list of documents in the page
        .concatMap(Observable::from) // Flatten the list of documents
        .map(doc -> doc.getId()) // Map to the document Id
        .forEach(docId -> resultList.add(docId)); // Add each document Id to the resultList

        Thread.sleep(4000);

        for (String eachString : resultList) {
            System.out.println("Each id retrieved = " + eachString);
        }
    }
}
