package com.couchbase.client.examples.async;

import java.util.NoSuchElementException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;

import rx.functions.Action0;
import rx.functions.Action1;

public class ErrorHandling {

  public static void main(String[] args) throws InterruptedException {
    //connect to a cluster reachable at localhost and get default bucket
    Cluster cluster = CouchbaseCluster.create();
    Bucket bucket = cluster.openBucket();

    final String key = "heisenberg";

    bucket.async()
        .get(key)
            //will throw an NoSuchElement exception if the observable doesn't emit anything
        .first()
            //registers an action to do AFTER the observable has completed or errored
        .finallyDo(new Action0() {
          @Override
          public void call() {
            System.out.println("Exiting");
            cluster.disconnect();
          }
        })
        .subscribe(
            //register what to do on next element
            new Action1<JsonDocument>() {
              @Override
              public void call(JsonDocument loaded) {
                System.out.println("There was no error getting " + loaded);
              }
            },
            //register what to do on error
            new Action1<Throwable>() {
              @Override
              public void call(Throwable error) {
                if (error instanceof NoSuchElementException)
                  System.err.println("No such document:" + key);
                else
                  System.err.println("Unexpected exception " + error);
              }
            }
        );
  }

}
