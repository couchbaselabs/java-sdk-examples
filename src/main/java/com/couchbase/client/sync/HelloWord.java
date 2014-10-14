package com.couchbase.client.sync;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class HelloWord {

  public static void main(String[] args) {
    //connect to a cluster reachable at localhost and get default bucket
    Cluster cluster = CouchbaseCluster.create();
    Bucket bucket = cluster.openBucket();

    //prepare a json object to store in a JsonDocument with ID "walter"
    JsonObject user = JsonObject.empty()
        .put("firstname", "Walter")
        .put("lastname", "White")
        .put("job", "chemistry teacher")
        .put("age", 50);
    JsonDocument doc = JsonDocument.create("walter", user);

    //insert doc in bucket, updating it if it exists
    JsonDocument response = bucket.upsert(doc);

    //retrieve the document and show data
    JsonDocument walter = bucket.get("walter");
    System.out.println("Found: " + walter);
    System.out.println("Age: " + walter.content().getInt("age"));

    //get-and-update operation
    JsonDocument loaded = bucket.get("walter");
    if (loaded == null) {
      System.err.println("Document not found!");
    } else {
      loaded.content().put("age", 52);
      JsonDocument updated = bucket.replace(loaded);
      System.out.println("Updated: " + updated.id());
    }

    //cleanup (in a synchronous way) and disconnect
    System.out.println("Cleaning Up");
    bucket.remove("walter");
    System.out.println("Exiting");
    cluster.disconnect();
  }
}
