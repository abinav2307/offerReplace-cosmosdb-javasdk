package com.cosmosdb.ninjas.optimisticConcurrency.syncImplementation;

import java.io.InputStream;
import java.util.Properties;

import com.cosmosdb.ninjas.offerReplace.bootup.CosmosDBBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.ResourceResponse;

public class OptimisticConcurrencySync {
    
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
        DocumentClient client = CosmosDBBootupManager.bootup(accountName, accountKey);
        
        Document insertedDocument = insertSampleDocument(client, databaseName, collectionName);
        
        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setCondition(insertedDocument.getETag());
        
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setAccessCondition(accessCondition);
        
        insertedDocument.set("country", "Spain");
        
        client.replaceDocument(insertedDocument, requestOptions);
        
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
    private Document insertSampleDocument(DocumentClient client, String databaseName, String collectionName) {
        
        String sampleDocument = "{\"firstName\":\"Abinav\",\"lastName\":\"Rameesh\",\"city\":\"Constantinople\",\"country\":\"Italy\"}";
        Document document = new Document(sampleDocument);
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        
        Document insertedDocument = null;
        try {
            ResourceResponse<Document> resourceResponse = client.createDocument(collectionLink, document, null, false);
            insertedDocument = resourceResponse.getResource();
        } catch (DocumentClientException ex) {
            System.out.println("Exception thrown when creating document. Exception message is: " + ex.getMessage());
        }
     
        return insertedDocument;
    }

}
