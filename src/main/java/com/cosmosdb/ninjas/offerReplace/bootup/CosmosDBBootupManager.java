package com.cosmosdb.ninjas.offerReplace.bootup;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

public class CosmosDBBootupManager 
{
    private static String HOST_PREFIX = "https://"; 
    private static String HOST_POSTFIX = ".documents.azure.com:443/";
    
    public static DocumentClient bootup(String accountName, String accountKey)
    {
        try
        {
            String host = HOST_PREFIX + accountName + HOST_POSTFIX;
            String key = accountKey;
             
            ConnectionPolicy connectionPolicy = new ConnectionPolicy();
            connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
            connectionPolicy.setMaxPoolSize(1000);
            
            DocumentClient documentClient = new DocumentClient(host, key, connectionPolicy, ConsistencyLevel.Eventual);
            
            return documentClient;
        }
        catch(Exception e)
        {
            throw new RuntimeException("Failed to initialize Java sync DocumentClient for account: " + accountName + ". Original exception was: ", e);
        }
    }
}
