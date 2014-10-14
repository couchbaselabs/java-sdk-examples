package com.couchbase.client.async;

import java.util.concurrent.CountDownLatch;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class HelloWord {

  public static void main(String[] args) throws InterruptedException {
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

    //insert doc in bucket, updating it if it exists (upsert)
    //as everything will be done asynchronously, we need some tricks to be deterministic in regard to following reads
    //we could use a CountDownLatch, but Rx allows blocking semantics with toBlocking(), which has the same spirit.
    bucket.async()
        .upsert(doc)
        .doOnNext(new Action1<JsonDocument>() {
          @Override
          public void call(JsonDocument jsonDocument) {
            System.out.printf("Persisted doc wit CAS %s vs %s\n", jsonDocument.cas(), doc.cas());
          }
        })
        .toBlocking().single(); //blocks and returns the only result (would throw an exception if <> 1 result)

    //retrieve the document and show data, this one is fire-and-forget
    bucket.async()
        .get("walter")
        .subscribe(new Action1<JsonDocument>() {
          @Override
          public void call(JsonDocument result) {
            System.out.printf("Found: %s\nAge: %d\n", result, doc.content().getInt("age"));
          }
        });

    //get-and-update operation
    //here we will use a subscription to execute the flow and display result, which cannot be used with blocking observables...
    //so in order to see anything before the thread exits we will wait using a latch
    CountDownLatch latch = new CountDownLatch(1);
    bucket
        .async()
        .get("walter")
        .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final JsonDocument loaded) {
            loaded.content().put("age", 52);
            return bucket.async().replace(loaded);
          }
        })
        .subscribe(new Action1<JsonDocument>() {
          @Override
          public void call(final JsonDocument updated) {
            System.out.println("Updated: " + updated.id());
            latch.countDown();
          }
        });

    //wait for the get and update operation to be finished before exiting
    latch.await();

    //cleanup (in a synchronous way) and disconnect
    System.out.println("Cleaning Up");
    bucket.remove("walter");
    System.out.println("Exiting");
    cluster.disconnect();
  }
}
