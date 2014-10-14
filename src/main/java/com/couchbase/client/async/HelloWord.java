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

    //insert doc in bucket, updating it if it exists
    bucket.async()
        .upsert(doc)
        .doOnNext(new Action1<JsonDocument>() {
          @Override
          public void call(JsonDocument jsonDocument) {
            System.out.printf("Persisted doc wit CAS %s vs %s\n", jsonDocument.cas(), doc.cas());
          }
        });

    //retrieve the document and show data
    bucket.async()
        .get("walter")
        .subscribe(new Action1<JsonDocument>() {
          @Override
          public void call(JsonDocument doc) {
            System.out.println("Found: " + doc);
            System.out.println("Age: " + doc.content().getInt("age"));
          }
        });

    //for this simple example, in order to see anything before the thread exits we will wait using a latch
    CountDownLatch latch = new CountDownLatch(1);

    //get-and-update operation
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
          }
        });

    //wait for the get and update operation to be finished before exiting
    latch.await();
    //disconnect the client
    cluster.disconnect();
  }
}
