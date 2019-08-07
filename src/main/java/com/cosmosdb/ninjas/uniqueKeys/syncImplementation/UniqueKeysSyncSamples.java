package com.cosmosdb.ninjas.uniqueKeys.syncImplementation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import com.cosmosdb.ninjas.offerReplace.bootup.CosmosDBBootupManager;
import com.cosmosdb.ninjas.offerReplace.main.Main;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.UniqueKey;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;

public class UniqueKeysSyncSamples {
    
    private DocumentClient client;
    
    public void displayUniqueKeys() throws IOException, DocumentClientException {
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
        this.client = CosmosDBBootupManager.bootup(accountName, accountKey);
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        DocumentCollection collection = this.client.readCollection(collectionLink, null).getResource();
        Collection<UniqueKey> uniqueKeysForCollection = collection.getUniqueKeyPolicy().getUniqueKeys();

        for (UniqueKey eachUniqueKey : uniqueKeysForCollection) {
            System.out.println("Unique key constraint ");
            
            for (String eachPath : eachUniqueKey.getPaths()) {
                System.out.println("Each path for this constraint: " + eachPath);
            }
        }
    }

}
