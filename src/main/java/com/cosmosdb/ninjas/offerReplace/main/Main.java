package com.cosmosdb.ninjas.offerReplace.main;

import com.cosmosdb.ninjas.offerReplace.asyncImplementation.OfferReplaceImplAsync;
import com.cosmosdb.ninjas.offerReplace.syncImplementation.OfferReplaceImplSync;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        // Java Sync SDK implementation for replacing the throughput of a collection
        int newOfferThroughputToSet = 150000;
        OfferReplaceImplSync syncImplementation = new OfferReplaceImplSync();
        syncImplementation.executeOfferReplace(newOfferThroughputToSet);
        
        // Java Async SDK implementation for replacing the throughput of a collection
        newOfferThroughputToSet = 160000;
        OfferReplaceImplAsync asyncImplementation = new OfferReplaceImplAsync();
        asyncImplementation.executeOfferReadAndReplace(newOfferThroughputToSet);
    }
}
