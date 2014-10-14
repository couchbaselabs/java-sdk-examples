package com.couchbase.client.examples.asyncLambdas;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import rx.Observable;

/**
 * Example that shows how to :<ul>
 *   <li>connect to a cluster, configure the cluster connection, reuse the connection</li>
 *   <li>connect to one or multiple buckets</li>
 *   <li>exit cleanly</li></ul>
 */
public class Connecting {

  public static void main(String[] args) {
    /* **** Cluster connection **** */
    //first simplest method is to connect to a localhost-reachable cluster
    CouchbaseCluster cluster1 = CouchbaseCluster.create();

    //alternatively, specify known nodes to reach the cluster (ideally more than one to )
    CouchbaseCluster cluster2 = CouchbaseCluster.create("127.0.0.1", "192.168.1.1");

    //only one cluster connection should ever be made for a given cluster
    //but if you have to connect to several clusters, you should share a {@link CouchbaseEnvironment}
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(TimeUnit.SECONDS.toMillis(10))
        .requestBufferSize(1024)
        .build();
    CouchbaseCluster cluster1b = CouchbaseCluster.create(env);
    CouchbaseCluster cluster2b = CouchbaseCluster.create(env, "127.0.0.1", "192.168.1.1");

    //the previous code will reuse the io pools and such, but as soon as you pass an environment, you have to cleanly shutdown it yourself.
      Observable
          .just(cluster1, cluster2)
          .flatMap(c -> Observable.just(c.disconnect())) //disconnect each cluster
          .last()
          .flatMap(isDisconnected -> env.shutdown()) //shutdown environment when last cluster has disconnected
          .subscribe();

    /* **** Bucket Connections *** */
    //once we have a cluster reference, we can obtain a connection to the default bucket
    Bucket defaultBucket = cluster1.openBucket();
    //the method async() is used to go into asynchronous mode
    AsyncBucket defaultBucketAsync = defaultBucket.async();

    //we can also connect to a specific bucket, providing a password if needed
    Bucket specificBucket = cluster1.openBucket("beer-sample");
    Bucket passwordBucket = cluster1.openBucket("beer-sample", "myPassword"); //not protected, so it will gladly accept the password

    //you can reuse Bucket references and pass them along. Once a bucket is not useful, you can free the resources by closing it
    specificBucket.async()
        .close()
    //disconnecting from the cluster will close all buckets remaining open
        .subscribe(hasClosed -> cluster1.disconnect());

    //TODO : this should fail fast and not throw a TimeoutException : passwordBucket.async().get("toto");

    //don't forget to disconnect from all clusters, otherwise the JVM won't exit and resources won't be freed
    cluster2.disconnect();
  }

}
