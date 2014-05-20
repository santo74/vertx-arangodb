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
import santo.vertx.arangodb.rest.DocumentAPI;
import santo.vertx.arangodb.rest.EdgeAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.EdgeAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EdgeIntegrationTest extends BaseIntegrationTest {
    
    @Test
    public void test01CreateEdge() {
        System.out.println("*** test01CreateEdge ***");
        
        // Create from-document in testcol
        JsonObject documentObject = new JsonObject().putString("description", "from-doc");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, "testcol");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    
                    fromId = arangoResult.getString("_id");

                    // Create to-document in testcol
                    JsonObject documentObject = new JsonObject().putString("description", "to-doc");
                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
                    requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
                    requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, "testcol");
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));

                                toId = arangoResult.getString("_id");
                                
                                // Now create an edge between from-doc and to-doc
                                JsonObject documentObject = new JsonObject().putString("description", "testedge");
                                JsonObject requestObject = new JsonObject();
                                requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
                                requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_CREATE);
                                requestObject.putObject(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                                requestObject.putString(EdgeAPI.MSG_PROPERTY_COLLECTION, "edgecol");
                                requestObject.putString(EdgeAPI.MSG_PROPERTY_FROM, fromId);
                                requestObject.putString(EdgeAPI.MSG_PROPERTY_TO, toId);
                                vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> reply) {
                                        try {
                                            JsonObject response = reply.body();
                                            System.out.println("response: " + response);
                                            JsonObject arangoResult = response.getObject("result");
                                            VertxAssert.assertTrue("Edge creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                            if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No edge key received", arangoResult.getString("_id"));

                                            edgeId = arangoResult.getString("_id");
                                            edgeRevision = arangoResult.getString("_rev");
                                        }
                                        catch (Exception e) {
                                            VertxAssert.fail("test01CreateEdge");
                                        }
                                        VertxAssert.testComplete();
                                    }
                                });

                            }
                            catch (Exception e) {
                                VertxAssert.fail("test01CreateEdge");
                            }
                        }
                    });

                }
                catch (Exception e) {
                    VertxAssert.fail("test01CreateEdge");
                }
            }
        });
    }

    @Test
    public void test02GetEdge() {
        System.out.println("*** test02GetEdge ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_READ);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified edge resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertTrue("The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test02GetEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03GetEdgeHeader() {
        System.out.println("*** test03GetEdgeHeader ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_HEAD);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified edge header resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test03GetEdgeHeader");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04UpdateEdge() {
        System.out.println("*** test04UpdateEdge ***");
        JsonObject documentObject = new JsonObject().putString("description", "updated testedge");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_UPDATE);
        requestObject.putObject(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Edge update resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No edge key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Edge not correctly updated", edgeRevision, arangoResult.getString("_rev"));
                    System.out.println("edge details: " + arangoResult);
                    
                    edgeRevision = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    VertxAssert.fail("test04UpdateEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05ReplaceEdge() {
        System.out.println("*** test05ReplaceEdge ***");
        JsonObject documentObject = new JsonObject().putString("description", "replaced testedge");
        documentObject.putString("name", "replacement edge");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_REPLACE);
        requestObject.putObject(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Edge replacement resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No edge key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Edge not correctly replaced", edgeRevision, arangoResult.getString("_rev"));
                    System.out.println("edge details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05ReplaceEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06GetEdgeRelations() {
        System.out.println("*** test06GetEdgeRelations ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_RELATIONS);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_COLLECTION, "edgecol");
        requestObject.putString(EdgeAPI.MSG_PROPERTY_VERTEX, fromId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The listing of all the edges for the specified collection resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("edges for collection: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06GetEdgeRelations");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07DeleteEdge() {
        System.out.println("*** test07DeleteEdge ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_DELETE);
        requestObject.putString(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The deletion of the specified edge resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test07DeleteEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

}
