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
import santo.vertx.arangodb.rest.AqlAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.AqlAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AqlIntegrationTest extends BaseIntegrationTest {
    
    public static String idVertex01;
    public static String idVertex02;
    
    @Test
    public void test01ExecuteCursor() {
        System.out.println("*** test01ExecuteCursor ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.putBoolean("count", true);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No count number received", arangoResult.getNumber("count"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test01ExecuteCursor");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02ValidateQuery() {
        System.out.println("*** test02ValidateQuery ***");
        String query = "FOR v in " + vertexColName + " LIMIT 1 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_VALIDATE);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No collections found", arangoResult.getArray("collections"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test02ValidateQuery");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03ExecuteNext() {
        System.out.println("*** test03ExecuteNext ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.putBoolean("count", true);
        documentObject.putNumber("batchSize", 1);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No count number received", arangoResult.getNumber("count"));
                    VertxAssert.assertTrue("No cursor available", arangoResult.getBoolean("hasMore"));
                    String cursorId = arangoResult.getString("id");
                    
                    // cursor ok, now retrieve next batch
                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
                    requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_NEXT);
                    requestObject.putString(AqlAPI.MSG_PROPERTY_ID, cursorId);
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No count number received", arangoResult.getNumber("count"));
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test03ExecuteNext");
                            }
                            VertxAssert.testComplete();
                        }
                    });
                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test03ExecuteNext");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04DeleteCursor() {
        System.out.println("*** test04DeleteCursor ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.putBoolean("count", true);
        documentObject.putNumber("batchSize", 1);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No count number received", arangoResult.getNumber("count"));
                    VertxAssert.assertTrue("No cursor available", arangoResult.getBoolean("hasMore"));
                    String cursorId = arangoResult.getString("id");
                    
                    // cursor ok, now retrieve next batch
                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
                    requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_DELETE);
                    requestObject.putString(AqlAPI.MSG_PROPERTY_ID, cursorId);
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 202);
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test04DeleteCursor");
                            }
                            VertxAssert.testComplete();
                        }
                    });
                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test04DeleteCursor");
                }
            }
        });
    }

    @Test
    public void test05ExplainQuery() {
        System.out.println("*** test05ExplainQuery ***");
        String query = "FOR v in " + vertexColName + " LIMIT 1 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXPLAIN);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05ExplainQuery");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06CreateFunction() {
        System.out.println("*** test06CreateFunction ***");
        String name = "test::mytestfunction";
        String code = "function (celsius) { return celsius * 1.8 + 32; }";
        JsonObject documentObject = new JsonObject();
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_NAME, name);
        documentObject.putString(AqlAPI.DOC_ATTRIBUTE_CODE, code);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_CREATE_FUNCTION);
        requestObject.putObject(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06CreateFunction");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07GetFunctions() {
        System.out.println("*** test07GetFunctions ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_GET_FUNCTION);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test07GetFunctions");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test08GetFunctionsFromNamespace() {
        System.out.println("*** test08GetFunctionsFromNamespace ***");
        String namespace = "test";
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_GET_FUNCTION);
        requestObject.putString(AqlAPI.MSG_PROPERTY_NAMESPACE, namespace);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test08GetFunctionsFromNamespace");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test09DeleteFunction() {
        System.out.println("*** test09DeleteFunction ***");
        String name = "test::mytestfunction";
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.putString(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_DELETE_FUNCTION);
        requestObject.putString(AqlAPI.MSG_PROPERTY_NAME, name);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test09DeleteFunction");
                }
                VertxAssert.testComplete();
            }
        });
    }

}
