package com.couchbase.client.examples.sync;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;

public class ErrorHandling {

  public static void main(String[] args) {
    //connect to a cluster reachable at localhost and get default bucket
    Cluster cluster = CouchbaseCluster.create();
    Bucket bucket = cluster.openBucket();

    final String key = "heisenberg";
    try {
      JsonDocument loaded = bucket.get(key);
      if (loaded == null) {
        System.err.println("No such document: " + key);
      } else {
        System.out.println("There was no error getting " + loaded);
      }
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
    }

    //cleanup (in a synchronous way) and disconnect
    System.out.println("Exiting");
    cluster.disconnect();
  }

}
