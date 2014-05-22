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

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.DocumentAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentIntegrationTest extends BaseIntegrationTest {
    
    @Test
    public void test01aCreateTestDocument() {
        System.out.println("*** test01aCreateTestDocument ***");
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
                    
                    // Create another document that can be used in later tests
                    JsonObject documentObject = new JsonObject().putString("description", "test2");
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

                                docId = arangoResult.getString("_id");
                                docRevision = arangoResult.getString("_rev");
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test01aCreateTestDocument");
                            }
                            VertxAssert.testComplete();
                        }
                    });
                }
                catch (Exception e) {
                    VertxAssert.fail("test01aCreateTestDocument");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test01bCreateGeoDocument() {
        System.out.println("*** test01bCreateGeoDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "GEO location test document");
        documentObject.putNumber(DocumentAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        documentObject.putNumber(DocumentAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
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
                }
                catch (Exception e) {
                    VertxAssert.fail("test01bCreateGeoDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test01cCreateRangeDocument() {
        System.out.println("*** test01cCreateRangeDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "Range test document (skiplist)");
        documentObject.putNumber("age", 30);
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
                }
                catch (Exception e) {
                    VertxAssert.fail("test01cCreateRangeDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02GetDocument() {
        System.out.println("*** test02GetDocument ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_READ);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified document resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertTrue("The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test02GetDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03GetDocumentHeader() {
        System.out.println("*** test03GetDocumentHeader ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_HEAD);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified document header resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test03GetDocumentHeader");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04UpdateDocument() {
        System.out.println("*** test04UpdateDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "updated test");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_UPDATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document update resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Document not correctly updated", docRevision, arangoResult.getString("_rev"));
                    System.out.println("document details: " + arangoResult);
                    
                    docRevision = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    VertxAssert.fail("test04UpdateDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05ReplaceDocument() {
        System.out.println("*** test05ReplaceDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "replaced test");
        documentObject.putString("name", "replacement document");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_REPLACE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document replacement resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Document not correctly replaced", docRevision, arangoResult.getString("_rev"));
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05ReplaceDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06GetDocumentList() {
        System.out.println("*** test06GetDocumentList ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_LIST);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The listing of all the document for the specified collection resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("documents for collection: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06GetDocumentList");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07DeleteDocument() {
        System.out.println("*** test07DeleteDocument ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_DELETE);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The deletion of the specified document resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    //VertxAssert.assertTrue("The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test07DeleteDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
