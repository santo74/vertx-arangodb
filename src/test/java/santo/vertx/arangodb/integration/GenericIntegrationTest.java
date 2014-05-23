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
import santo.vertx.arangodb.rest.GenericAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.GenericAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GenericIntegrationTest extends BaseIntegrationTest {
    
    public static String idTestDoc;
    public static String revTestDoc;
    
    @Test
    public void test01PerformPostRequest() {
        System.out.println("*** test01PerformPostRequest ***");
        String path = "/document/?collection=" + vertexColName;
        JsonObject documentObject = new JsonObject().putString("name", "POST test document");
        documentObject.putNumber("age", 30);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_POST);
        requestObject.putObject(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The POST request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    VertxAssert.assertFalse("The POST request resulted in an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document id received", arangoResult.getString("_id"));
                    
                    idTestDoc = arangoResult.getString("_id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test01PerformPostRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02PerformGetRequest() {
        System.out.println("*** test02PerformGetRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_GET);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The GET request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertFalse("The GET request returned an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    //VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 200);
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test02PerformGetRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test03PerformHeadRequest() {
        System.out.println("*** test03PerformHeadRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_HEAD);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The HEAD request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertFalse("The HEAD request returned an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    //VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 200);
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test03PerformHeadRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04PerformPatchRequest() {
        System.out.println("*** test04PerformPatchRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject documentObject = new JsonObject().putString("name", "PATCH test document");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_PATCH);
        requestObject.putObject(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The PATCH request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    VertxAssert.assertFalse("The PATCH request resulted in an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document id received", arangoResult.getString("_id"));
                    
                    revTestDoc = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    VertxAssert.fail("test04PerformPatchRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05PerformPutRequest() {
        System.out.println("*** test05PerformPutRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject documentObject = new JsonObject().putString("name", "PUT test document");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_PUT);
        requestObject.putObject(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The PUT request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    VertxAssert.assertFalse("The PUT request resulted in an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document id received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Document not correctly replaced", revTestDoc, arangoResult.getString("_rev"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test05PerformPutRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06PerformDeleteRequest() {
        System.out.println("*** test06PerformDeleteRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.putString(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_DELETE);
        requestObject.putString(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The DELETE request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertFalse("The DELETE request returned an error: " + arangoResult.getString("errorMessage"), arangoResult.getBoolean("error"));
                    //VertxAssert.assertTrue("Wrong return code received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 202);
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06PerformDeleteRequest");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
