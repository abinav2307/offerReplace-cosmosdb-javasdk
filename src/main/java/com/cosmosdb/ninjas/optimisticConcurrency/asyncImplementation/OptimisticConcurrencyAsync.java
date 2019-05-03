package com.cosmosdb.ninjas.optimisticConcurrency.asyncImplementation;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cosmosdb.ninjas.offerReplace.bootup.async.CosmosDBAsyncBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.cosmosdb.AccessCondition;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import rx.Scheduler;
import rx.schedulers.Schedulers;

public class OptimisticConcurrencyAsync {

    private AsyncDocumentClient asyncDocumentClient;
    
    // scheduler for blocking work
    private final Scheduler schedulerForBlockingWork;
    private final ExecutorService executor;
    
    public OptimisticConcurrencyAsync() throws Exception {
        
        this.executor = Executors.newSingleThreadExecutor();
        this.schedulerForBlockingWork = Schedulers.from(executor);
    }
    
    /**
     * Replaces a document in Cosmos DB using Optimistic Concurrency Control
     * 
     * @throws Exception
     */
    public void executeReplaceWithOCC() throws Exception {
        
        // Load account details from config.properties
        Properties settings = new Properties();
        InputStream propertiesInputStream =
            Main.class.getClassLoader().getResourceAsStream("config.properties");
        settings.load(propertiesInputStream);
        
        String accountName = settings.getProperty("cosmosDbAccountName");
        String accountKey = settings.getProperty("cosmosDbAccountKey");
        String databaseName = settings.getProperty("cosmosDbDatabaseName");
        String collectionName = settings.getProperty("cosmosDbCollectionName");
        
        // Fetch the DocumentClient
        this.asyncDocumentClient = CosmosDBAsyncBootupManager.bootup(accountName, accountKey);
        
        Document insertedDocument = insertSampleDocument(databaseName, collectionName);
        
        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setCondition(insertedDocument.getETag());
        
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setAccessCondition(accessCondition);
        
        insertedDocument.set("country", "Spain");
        
        this.asyncDocumentClient.replaceDocument(insertedDocument, requestOptions).toBlocking().single();
        
        System.out.println("Successfully replaced the document");
    }
    
    /**
     * Insert a sample document to be replaced using Optimistic Concurrency Control (OCC)
     * @param client DocumentClient instance to be used when interacting with the Azure Cosmos DB Service
     * @param databaseName Cosmos DB database containing the collection within which the sample document will be inserted
     * @param collectionName Cosmos DB collection into which the sample document will be inserted
     * 
     * @return The document inserted into the specified Cosmos DB collection
     */
    private Document insertSampleDocument(String databaseName, String collectionName) {
        String sampleDocument = "{\"firstName\":\"Abinav\",\"lastName\":\"Rameesh\",\"city\":\"Constantinople\",\"country\":\"Italy\"}";
        Document document = new Document(sampleDocument);
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        
        Document insertedDocument = null;
        
        ResourceResponse<Document> resourceResponse = this.asyncDocumentClient.createDocument(collectionLink, document, null, false)
        .subscribeOn(schedulerForBlockingWork).toBlocking().single();
        
        insertedDocument = resourceResponse.getResource();
        
        return insertedDocument;
    }
}