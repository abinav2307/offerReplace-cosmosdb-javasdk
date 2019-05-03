package com.cosmosdb.ninjas.offerReplace.main;

import com.cosmosdb.ninjas.offerReplace.asyncImplementation.OfferReplaceImplAsync;
import com.cosmosdb.ninjas.offerReplace.sharedThoroughput.syncImplementation.DatabaseOfferReplace;
import com.cosmosdb.ninjas.offerReplace.syncImplementation.OfferReplaceImplSync;
import com.cosmosdb.ninjas.optimisticConcurrency.asyncImplementation.OptimisticConcurrencyAsync;
import com.cosmosdb.ninjas.optimisticConcurrency.syncImplementation.OptimisticConcurrencySync;
import com.cosmosdb.ninjas.query.asyncImplementation.QueryAsyncSamples;
import com.cosmosdb.ninjas.query.syncImplementation.QuerySyncSamples;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        // Java Sync SDK implementation for replacing the throughput of a collection
        OfferReplaceImplSync syncImplementation = new OfferReplaceImplSync();
        syncImplementation.executeOfferReplace();
        
        // Java Async SDK implementation for replacing the throughput of a collection
        OfferReplaceImplAsync asyncImplementation = new OfferReplaceImplAsync();
        asyncImplementation.executeOfferReadAndReplace();
        
        // Java Sync SDK implementation for creating and the replacing the throughput of a Database
        DatabaseOfferReplace databaseLevelSyncImplementation = new DatabaseOfferReplace();
        databaseLevelSyncImplementation.createSharedThroughputDatabase();
        databaseLevelSyncImplementation.executeOfferReplace();
        
        // Java Async SDK implementation of a query with an IN clause
        QueryAsyncSamples queryAsyncSamples = new QueryAsyncSamples();
        queryAsyncSamples.executeQueryWithInClause();
        
        // Java Sync SDK implementation of a query with an IN clause
        QuerySyncSamples querySyncSamples = new QuerySyncSamples();
        querySyncSamples.executeQueryWithInClause();
        
        OptimisticConcurrencySync optimisticConcurrencySync = new OptimisticConcurrencySync();
        optimisticConcurrencySync.executeReplaceWithOCC();
        
        OptimisticConcurrencyAsync optimisticConcurrencyAsync = new OptimisticConcurrencyAsync();
        optimisticConcurrencyAsync.executeReplaceWithOCC();
    }
}
