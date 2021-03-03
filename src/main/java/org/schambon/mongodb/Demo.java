package org.schambon.mongodb;

import com.mongodb.*;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Updates.set;
import static java.time.Instant.MAX;
import static java.time.Instant.now;

public class Demo {

    public static void main(String[] args) throws InterruptedException {
        // Codec Registry : configuration du mapping des classes Java en documents BSON
        var codecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(
                        PojoCodecProvider.builder().automatic(true).build()));  // gestion des POJO (Java Beans avec setters et getters)

        var client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb+srv://URL A REMPLACER/"))
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(5, TimeUnit.SECONDS))
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .retryReads(true)
                .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor()))
                .codecRegistry(codecRegistry)
                .build()
        );

        var collection = client.getDatabase("test").getCollection("testreactive", Person.class);

        // drop collection
        var latch1 = new CountDownLatch(1); // pour synchroniser les appels
        collection.drop().subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Void unused) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                System.out.println("Collection dropped");
                latch1.countDown();
            }
        });
        latch1.await();

        // Insert many
        List<Person> personList = new ArrayList();
        for (var i = 0; i < 1000; i++) {
            personList.add(new Person("Dupont", i, now()));
        }

        var latch2 = new CountDownLatch(1);
        collection.insertMany(personList, new InsertManyOptions().ordered(false)).subscribe(new Subscriber<InsertManyResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(InsertManyResult insertManyResult) {
                System.out.println(String.format("Inserted %d documents", insertManyResult.getInsertedIds().size()));
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                latch2.countDown();
            }
        });
        latch2.await();

        // Lecture des 20 premiers enregistrements
        var latch3 = new CountDownLatch(1);
        collection.find().skip(0).limit(20).maxTime(10, TimeUnit.SECONDS).subscribe(new Subscriber<Person>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Person person) {
                System.out.println(person.toString());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                latch3.countDown();
            }
        });
        latch3.await();

        // Lecture de tous les enregistrements
        var latch4 = new CountDownLatch(1);
        var cumulativeAge = new AtomicInteger();
        collection.find().subscribe(new Subscriber<Person>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Person person) {
                cumulativeAge.addAndGet(person.getAge());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                latch4.countDown();
            }
        });
        latch4.await();
        System.out.println(String.format("Cumulative age %d", cumulativeAge.get()));

        // agrégation
        var latch5 = new CountDownLatch(1);
        collection.aggregate(Arrays.asList(group(null, sum("cumulativeAge", "$age"))), Document.class)
                .subscribe(new Subscriber<Document>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Document document) {
                        System.out.println(String.format("Cumulative age %d", document.getInteger("cumulativeAge")));
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {
                        latch5.countDown();
                    }
                });
        latch5.await();

        // Création d'index
        var latch6 = new CountDownLatch(1);
        collection.createIndex(ascending("age")).subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(String s) {
                System.out.println(String.format("Created index: %s", s));
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                latch6.countDown();
            }
        });
        latch6.await();

        // update
        var latch7 = new CountDownLatch(1);
        collection.updateOne(eq("age", 5), set("dateOfBirth", now().minus(5 * 365, ChronoUnit.DAYS)))
                .subscribe(new Subscriber<UpdateResult>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(UpdateResult updateResult) {
                        System.out.println(String.format("Updated %d records", updateResult.getModifiedCount()));
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {
                        latch7.countDown();
                    }
                });
        latch7.await();


        // delete
        var latch8 = new CountDownLatch(1);
        collection.deleteMany(gt("age", 5)).subscribe(new Subscriber<DeleteResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(DeleteResult deleteResult) {
                System.out.println(String.format("Deleted %d", deleteResult.getDeletedCount()));
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                latch8.countDown();
            }
        });
        latch8.await();

        // count
        var latch9 = new CountDownLatch(1);
        collection.countDocuments().subscribe(new Subscriber<Long>() {
                                                  @Override
                                                  public void onSubscribe(Subscription s) {
                                                      s.request(1);
                                                  }

                                                  @Override
                                                  public void onNext(Long aLong) {
                                                      System.out.println(String.format("Remaining %d", aLong));
                                                  }

                                                  @Override
                                                  public void onError(Throwable t) {

                                                  }

                                                  @Override
                                                  public void onComplete() {
                                                      latch9.countDown();
                                                  }
                                              });
        latch9.await();

        // transaction
        var latch10 = new CountDownLatch(1);
        client.startSession().subscribe(new Subscriber<ClientSession>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(ClientSession session) {
                session.startTransaction(TransactionOptions.builder().maxCommitTime(10l, TimeUnit.SECONDS)
                    .readConcern(ReadConcern.SNAPSHOT).build());
                var latch11 = new CountDownLatch(2);
                collection.insertOne(session, new Person("Durand", 30, now().minus(30*365, ChronoUnit.DAYS))).subscribe(new Subscriber<InsertOneResult>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(InsertOneResult insertOneResult) {
                        System.out.println("tx - Inserted one");
                    }

                    @Override
                    public void onError(Throwable t) {

                        t.printStackTrace();
                        session.abortTransaction();
                    }

                    @Override
                    public void onComplete() {
                        latch11.countDown();
                    }
                });
                collection.deleteMany(eq("name", "Dupont")).subscribe(new Subscriber<DeleteResult>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(DeleteResult deleteResult) {
                        System.out.println(String.format("tx - deleted %d", deleteResult.getDeletedCount()));
                    }

                    @Override
                    public void onError(Throwable t) {

                        t.printStackTrace();
                        session.abortTransaction();
                    }

                    @Override
                    public void onComplete() {
                        latch11.countDown();
                    }
                });
                try {
                    latch11.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                var latch12 = new CountDownLatch(1);
                session.commitTransaction().subscribe(new Subscriber<Void>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(Void unused) {

                    }

                    @Override
                    public void onError(Throwable t) {

                        t.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        latch12.countDown();
                    }
                });
                try {
                    latch12.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                session.close();
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                latch10.countDown();
            }
        });

        latch10.await();

        System.exit(0);
    }
}