package com.cosmosdb.ninjas.offerReplace.sharedThoroughput.syncImplementation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import com.cosmosdb.ninjas.offerReplace.bootup.CosmosDBBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.RequestOptions;

public class DatabaseOfferReplace {
    
    /**
     * Creates a shared throughput offer (RUs provisioned at the Database level)
     * 
     * @throws IOException
     */
    public void createSharedThroughputDatabase() throws IOException {
        
        // Load account details from config.properties
        Properties settings = new Properties();
        InputStream propertiesInputStream =
            Main.class.getClassLoader().getResourceAsStream("config.properties");
        settings.load(propertiesInputStream);
        
        String accountName = settings.getProperty("cosmosDbAccountName");
        String accountKey = settings.getProperty("cosmosDbAccountKey");
        String databaseName = settings.getProperty("cosmosDbDatabaseName");
        int databaseLevelThroughputOnCreate = Integer.parseInt(settings.getProperty("databaseLevelThroughputOnCreate"));
        int newOfferThroughputToSet = Integer.parseInt(settings.getProperty("newOfferThroughputToSet"));
        
        // Fetch the DocumentClient
        DocumentClient client = CosmosDBBootupManager.bootup(accountName, accountKey);
        
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(databaseLevelThroughputOnCreate);
        
        Database sharedThroughputDatabase = new Database();
        sharedThroughputDatabase.setId(databaseName);
        
        try {
            System.out.println("About to create a shared throughput database");
            client.createDatabase(sharedThroughputDatabase, requestOptions);
            System.out.println("Successfully created shared throughput database");
        } catch (DocumentClientException e) {
            System.out.println(
                "Exception encountered when attempting to create a shared throughput database. " + 
                "Original exception message was: " + 
                e.getMessage());
        }        
    }
    
    /**
     * Executes an offer replace for the RUs provisioned at the Database level
     * 
     * @throws Exception
     */
    public void executeOfferReplace() throws Exception {
        
        // Load account details from config.properties
        Properties settings = new Properties();
        InputStream propertiesInputStream =
            Main.class.getClassLoader().getResourceAsStream("config.properties");
        settings.load(propertiesInputStream);
        
        String accountName = settings.getProperty("cosmosDbAccountName");
        String accountKey = settings.getProperty("cosmosDbAccountKey");
        String databaseName = settings.getProperty("cosmosDbDatabaseName");
        int offerThroughputToCreateDatabase = Integer.parseInt(settings.getProperty("databaseLevelThroughputOnCreate"));
        int databaseLevelThroughputForReplace = Integer.parseInt(settings.getProperty("databaseLevelThroughputForReplace"));
        
        // Fetch the DocumentClient
        DocumentClient client = CosmosDBBootupManager.bootup(accountName, accountKey);
        
        // 1. Retrieve the current throughput for this database
        int currentOfferThroughput = getCurrentThroughputForDatabase(client, databaseName);
        System.out.println("Current offer throughput for the collection = " + currentOfferThroughput);
        
        // 2. Replace the offer and confirm
        executeOfferReplace(client, databaseName, databaseLevelThroughputForReplace);
        int newCurrentOfferThroughput = getCurrentThroughputForDatabase(client, databaseName);
        System.out.println("New offer throughput for database " + databaseName + newCurrentOfferThroughput); 
    }

    /**
     * Retrieves the currently provisioned throughput for the specified database
     * 
     * @param client DocumentClient (sync SDK) instance to interact with the Cosmos DB account/collection(s)
     * @param databaseName Name of the Azure Cosmos DB database
     * 
     * @return int offerThroughput currently provisioned for the specified database
     * 
     * @throws Exception DocumentClientException
     */
    private static int getCurrentThroughputForDatabase(DocumentClient client, String databaseName) throws Exception {
        
        int currentOfferThroughput;
        
        String databaseLink = String.format("/dbs/%s", databaseName);
        String databaseResourceId = client.readDatabase(databaseLink, null).getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        Iterator<Offer> it = client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", databaseResourceId), null).getQueryIterator();
        
        Offer offer = it.next();
        currentOfferThroughput = offer.getContent().getInt("offerThroughput");
        
        return currentOfferThroughput;
    }
    
    /**
     * Replaces the throughput provisioned for the database with the specified input offer
     * 
     * @param client DocumentClient (sync SDK) instance to interact with the Cosmos DB account/collection(s)
     * @param databaseName Name of the Azure Cosmos DB database
     * @param newOfferThroughputToSet The new offer throughput to set for the specified database
     * 
     * @throws Exception DocumentClientException
     */
    private static void executeOfferReplace(DocumentClient client, String databaseName, int newOfferThroughputToSet) throws Exception {
        
        String databaseLink = String.format("/dbs/%s", databaseName);
        String databaseResourceId = client.readDatabase(databaseLink, null).getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        Iterator<Offer> it = client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", databaseResourceId), null).getQueryIterator();
        Offer offer = it.next();
        
        // Replace the existing throughput with the new throughput
        offer.getContent().put("offerThroughput", newOfferThroughputToSet);
        client.replaceOffer(offer);        
    }
}
