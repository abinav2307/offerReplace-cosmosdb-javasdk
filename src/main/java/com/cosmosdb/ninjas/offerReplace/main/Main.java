package com.cosmosdb.ninjas.offerReplace.main;

import com.cosmosdb.ninjas.offerReplace.asyncImplementation.OfferReplaceImplAsync;
import com.cosmosdb.ninjas.offerReplace.syncImplementation.OfferReplaceImplSync;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        // Java Sync SDK implementation for replacing the throughput of a collection
        OfferReplaceImplSync syncImplementation = new OfferReplaceImplSync();
        syncImplementation.executeOfferReplace();
        
        // Java Async SDK implementation for replacing the throughput of a collection
        OfferReplaceImplAsync asyncImplementation = new OfferReplaceImplAsync();
        asyncImplementation.executeOfferReadAndReplace();
    }
}
