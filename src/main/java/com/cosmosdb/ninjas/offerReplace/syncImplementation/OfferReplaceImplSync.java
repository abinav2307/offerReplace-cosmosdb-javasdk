package com.cosmosdb.ninjas.offerReplace.syncImplementation;

import java.util.Iterator;

import com.microsoft.azure.documentdb.Offer;
import com.cosmosdb.ninjas.offerReplace.bootup.CosmosDBBootupManager;
import com.microsoft.azure.documentdb.DocumentClient;

public class OfferReplaceImplSync {
    
    public void executeOfferReplace(int newOfferThroughputToSet) throws Exception {
        
        String accountName = "abinav-bulkexecutor-splitproof";
        String accountKey = "xMZFG3s4OiGbkINCzqmvxczthvymnbWy8VOifbY9zVSi1DvzgNggX5XpizV1yTlyHbFOjDPCOOHyMo86IvW9JA==";
        String databaseName = "SplitProofDB";
        String collectionName = "UpdateSplitProofV1";
        
        // Fetch the DocumentClient
        DocumentClient client = CosmosDBBootupManager.bootup(accountName, accountKey);
        
        // 1. Retrieve the current throughput for this collection
        int currentOfferThroughput = getCurrentThroughputForCollection(client, databaseName, collectionName);
        System.out.println("Current offer throughput for the collection = " + currentOfferThroughput);
        
        // 2. Replace the offer and confirm
        executeOfferReplace(client, databaseName, collectionName, newOfferThroughputToSet);
        int newCurrentOfferThroughput = getCurrentThroughputForCollection(client, databaseName, collectionName);
        System.out.println("New offer throughput for collection " + collectionName + " + " + newCurrentOfferThroughput); 
    }

    /**
     * Retrieves the current provisioned throughput for the specified collection
     * 
     * @param client DocumentClient (sync SDK) instance to interact with the Cosmos DB account/collection(s)
     * @param databaseName Name of the Azure Cosmos DB database
     * @param collectionName Name of the Azure Cosmos DB collection
     * 
     * @return int offerThroughput currently provisioned for the specified collection
     * 
     * @throws Exception DocumentClientException
     */
    private static int getCurrentThroughputForCollection(DocumentClient client, String databaseName, String collectionName) throws Exception {
        
        int currentOfferThroughput;
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        String collectionResourceId = client.readCollection(collectionLink, null).getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        Iterator<Offer> it = client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        
        Offer offer = it.next();
        currentOfferThroughput = offer.getContent().getInt("offerThroughput");
        
        return currentOfferThroughput;
    }
    
    /**
     * Replaces the throughput provisioned for the collection with the specified input offer
     * 
     * @param client DocumentClient (sync SDK) instance to interact with the Cosmos DB account/collection(s)
     * @param databaseName Name of the Azure Cosmos DB database
     * @param collectionName Name of the Azure Cosmos DB collection
     * @param newOfferThroughputToSet The new offer throughput to set for the specified collection
     * 
     * @throws Exception DocumentClientException
     */
    private static void executeOfferReplace(DocumentClient client, String databaseName, String collectionName, int newOfferThroughputToSet) throws Exception {
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        String collectionResourceId = client.readCollection(collectionLink, null).getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        Iterator<Offer> it = client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        Offer offer = it.next();
        
        // Replace the existing throughput with the new throughput
        offer.getContent().put("offerThroughput", newOfferThroughputToSet);
        client.replaceOffer(offer);        
    }
}
