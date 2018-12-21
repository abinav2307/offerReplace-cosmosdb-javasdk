package com.cosmosdb.ninjas.offerReplace.asyncImplementation;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.cosmosdb.ninjas.offerReplace.bootup.async.CosmosDBAsyncBootupManager;
import com.microsoft.azure.cosmosdb.Offer;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

public class OfferReplaceImplAsync {

    private static Integer currentOfferThroughput;
    
    public void executeOfferReadAndReplace(int newOfferThroughputToSet) throws Exception {
    
        String accountName = "abinav-bulkexecutor-splitproof";
        String accountKey = "xMZFG3s4OiGbkINCzqmvxczthvymnbWy8VOifbY9zVSi1DvzgNggX5XpizV1yTlyHbFOjDPCOOHyMo86IvW9JA==";
        String databaseName = "SplitProofDB";
        String collectionName = "UpdateSplitProofV1";
        
        // Fetch the DocumentClient
        AsyncDocumentClient client = CosmosDBAsyncBootupManager.bootup(accountName, accountKey);
        
        // 1. Retrieve the current throughput for this collection
        getCurrentThroughputForCollectionAsync(client, databaseName, collectionName);
        System.out.println("Current offer throughput for the collection = " + currentOfferThroughput);
        
        // 2. Replace the offer and confirm
        executeOfferReplaceAsync(client, databaseName, collectionName, newOfferThroughputToSet);
        getCurrentThroughputForCollectionAsync(client, databaseName, collectionName);
        System.out.println("New offer throughput for collection " + collectionName + " + " + currentOfferThroughput);
    }
    
    /**
     * Retrieves the current provisioned throughput for the specified collection
     * 
     * @param client DocumentClient (sync SDK) instance to interact with the Cosmos DB account/collection(s)
     * @param databaseName Name of the Azure Cosmos DB database
     * @param collectionName Name of the Azure Cosmos DB collection
     * 
     * @throws Exception DocumentClientException
     */
    private static void getCurrentThroughputForCollectionAsync(AsyncDocumentClient client, String databaseName, String collectionName) throws Exception {
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        String collectionResourceId = client.readCollection(collectionLink, null).toBlocking().single().getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);

        // Find offer associated with this collection
        client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId),
            null).subscribe(offerResourceResponse -> {
                
                Offer offer = offerResourceResponse.getResults().get(0);
                currentOfferThroughput = offer.getContent().getInt("offerThroughput");
                
                successfulCompletionLatch.countDown();
            }, error -> {
                System.err
                        .println("an error occurred while updating the offer: actual cause: " + error.getMessage());
            });

        successfulCompletionLatch.await();
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
    private static void executeOfferReplaceAsync(AsyncDocumentClient client, String databaseName, String collectionName, int newOfferThroughputToSet) throws Exception {
        
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        String collectionResourceId = client.readCollection(collectionLink, null).toBlocking().single().getResource().getResourceId();
        
        // Retrieve the current throughput provisioned for this collection
        final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);

        // Find offer associated with this collection
        client.queryOffers(
            String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId),
            null).flatMap(offerFeedResponse -> {
                
                List<Offer> offerList = offerFeedResponse.getResults();
                Offer offer = offerList.get(0);
                
                // Update the collection's throughput
                offer.getContent().put("offerThroughput", newOfferThroughputToSet);

                // Replace the offer
                return client.replaceOffer(offer);
                
            }).subscribe(offerResourceResponse -> {
                successfulCompletionLatch.countDown();
            }, error -> {
                System.err.println("An error occurred while updating the offer: Original error message: " + error.getMessage());
            });

        successfulCompletionLatch.await();
    }
}
