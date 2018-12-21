package com.cosmosdb.ninjas.offerReplace.bootup.async;

import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

public class CosmosDBAsyncBootupManager 
{
    private static String HOST_PREFIX = "https://"; 
    private static String HOST_POSTFIX = ".documents.azure.com:443/";
    
    public static AsyncDocumentClient bootup(String accountName, String accountKey)
    {
        try
        {
            String host = HOST_PREFIX + accountName + HOST_POSTFIX;
            String key = accountKey;
            
            ConnectionPolicy connectionPolicy = new ConnectionPolicy();
            connectionPolicy.setConnectionMode(ConnectionMode.Gateway);
            connectionPolicy.setMaxPoolSize(1000);
            
            System.out.println("Connecting to host: " + host);
            
            AsyncDocumentClient asyncDocumentClient = new AsyncDocumentClient.Builder()
                    .withServiceEndpoint(host)
                    .withMasterKey(key)
                    .withConnectionPolicy(connectionPolicy)
                    .withConsistencyLevel(ConsistencyLevel.Eventual)
                    .build();
            
            return asyncDocumentClient;
        }
        catch(Exception e)
        {
            throw new RuntimeException("Failed to initialize Java sync DocumentClient for account: " + accountName + ". Original exception was: ", e);
        }
    }
}
