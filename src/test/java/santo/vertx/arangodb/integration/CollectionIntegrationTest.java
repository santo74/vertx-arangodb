/*
 * Copyright 2014 sANTo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package santo.vertx.arangodb.integration;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.VertxAssert;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.CollectionAPI;
import santo.vertx.arangodb.rest.DocumentAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.CollectionAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CollectionIntegrationTest extends BaseIntegrationTest {

    @Test
    public void test01CreateTestCollections() {
        System.out.println("*** test01CreateTestCollections ***");
        // Create a test collection that we can use throughout the whole test cycle
        JsonObject documentObject = new JsonObject().putString("name", vertexColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
        requestObject.putObject(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Collection creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No collection id received", arangoResult.getString("id"));

                    // Create an extra collection for edges
                    JsonObject documentObject = new JsonObject().putString("name", edgeColName);
                    documentObject.putNumber("type", 3);
                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                    requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
                    requestObject.putObject(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertTrue("Collection creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No collection id received", arangoResult.getString("id"));
                                
                                // Create an extra temp collection that we can remove in the next test
                                JsonObject documentObject = new JsonObject().putString("name", "tempcol");
                                JsonObject requestObject = new JsonObject();
                                requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                                requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
                                requestObject.putObject(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                                vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> reply) {
                                        try {
                                            JsonObject response = reply.body();
                                            System.out.println("response: " + response);
                                            JsonObject arangoResult = response.getObject("result");
                                            VertxAssert.assertTrue("Collection creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                            if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No collection id received", arangoResult.getString("id"));
                                        }
                                        catch (Exception e) {
                                            VertxAssert.fail("test01CreateTestCollections");
                                        }
                                        VertxAssert.testComplete();
                                    }
                                });
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test01CreateTestCollections");
                            }
                        }
                    });
                }
                catch (Exception e) {
                    VertxAssert.fail("test01CreateTestCollections");
                }
            }
        });
    }

    @Test
    public void test02Rename() {
        System.out.println("*** test02Rename ***");
        JsonObject documentObject = new JsonObject().putString("name", "tempcol-renamed");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_RENAME);
        requestObject.putObject(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, "tempcol");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Loading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test02Rename");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03DeleteCollection() {
        System.out.println("*** test03DeleteCollection ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_DELETE);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, "tempcol-renamed");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The removal of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test03DeleteCollection");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04Rotate() {
        System.out.println("*** test04Rotate ***");
        
        // A collection can only be truncated if it already has a journal, meaning we should have inserted at least 1 document already.
        JsonObject documentObject = new JsonObject().putString("description", "test");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));

                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                    requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_ROTATE);
                    requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertEquals("Rotation of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                                System.out.println("response details: " + arangoResult);
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test04Rotate");
                            }
                            VertxAssert.testComplete();
                        }
                    });
                }
                catch (Exception e) {
                    VertxAssert.fail("test04Rotate");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05TruncateCollection() {
        System.out.println("*** test05TruncateCollection ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_TRUNCATE);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Truncation of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05TruncateCollection");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06Unload() {
        System.out.println("*** test06Unload ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_UNLOAD);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Unloading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06Unload");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07Load() {
        System.out.println("*** test07Load ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_LOAD);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Loading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test07Load");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test08ChangeProperties() {
        System.out.println("*** test08ChangeProperties ***");
        JsonObject documentObject = new JsonObject().putBoolean("waitForSync", false);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CHANGE_PROPERTIES);
        requestObject.putObject(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Changing properties of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test08ChangeProperties");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test09ReadCollection() {
        System.out.println("*** test09ReadCollection ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_READ);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Reading the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test09ReadCollection");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test10ListCollections() {
        System.out.println("*** test10ListCollections ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_LIST);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Listing all collections for the specified database resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test10ListCollections");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test11GetCollectionProperties() {
        System.out.println("*** test11GetCollectionProperties ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_GET_PROPERTIES);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Getting properties of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test11GetCollectionProperties");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test12GetCollectionCount() {
        System.out.println("*** test12GetCollectionCount ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_COUNT);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Getting count info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test12GetCollectionCount");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test13GetCollectionFigures() {
        System.out.println("*** test13GetCollectionFigures ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_FIGURES);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Getting figures of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test13GetCollectionFigures");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test14GetCollectionRevision() {
        System.out.println("*** test14GetCollectionRevision ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_REVISION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Getting revision info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test14GetCollectionRevision");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test15GetCollectionChecksum() {
        System.out.println("*** test15GetCollectionChecksum ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CHECKSUM);
        requestObject.putString(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Getting checksum info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test15GetCollectionChecksum");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
