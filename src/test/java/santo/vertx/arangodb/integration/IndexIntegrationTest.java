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
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.VertxAssert;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.IndexAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.IndexAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexIntegrationTest extends BaseIntegrationTest {

    @Test
    public void test01CreateHashIndex() {
        System.out.println("*** test01CreateHashIndex ***");
        JsonObject indexObject = new JsonObject().putString(IndexAPI.DOC_ATTRIBUTE_TYPE, IndexAPI.TYPE_HASH);
        JsonArray fieldsArray = new JsonArray().addString("description");
        indexObject.putArray("fields", fieldsArray);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_CREATE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        requestObject.putObject(IndexAPI.MSG_PROPERTY_DOCUMENT, indexObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                    
                    indexHashId = arangoResult.getString("id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test01CreateHashIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02CreateSkiplistIndex() {
        System.out.println("*** test02CreateSkiplistIndex ***");
        JsonObject indexObject = new JsonObject().putString(IndexAPI.DOC_ATTRIBUTE_TYPE, IndexAPI.TYPE_SKIPLIST);
        JsonArray fieldsArray = new JsonArray().addString("age");
        indexObject.putArray("fields", fieldsArray);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_CREATE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        requestObject.putObject(IndexAPI.MSG_PROPERTY_DOCUMENT, indexObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                    
                    indexSkiplistId = arangoResult.getString("id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test02CreateSkiplistIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03CreateFulltextIndex() {
        System.out.println("*** test03CreateFulltextIndex ***");
        JsonObject indexObject = new JsonObject().putString(IndexAPI.DOC_ATTRIBUTE_TYPE, IndexAPI.TYPE_FULLTEXT);
        JsonArray fieldsArray = new JsonArray().addString("description");
        indexObject.putArray("fields", fieldsArray);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_CREATE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        requestObject.putObject(IndexAPI.MSG_PROPERTY_DOCUMENT, indexObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                    
                    indexFulltextId = arangoResult.getString("id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test03CreateFulltextIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test04CreateCapIndex() {
        System.out.println("*** test04CreateCapIndex ***");
        JsonObject indexObject = new JsonObject().putString(IndexAPI.DOC_ATTRIBUTE_TYPE, IndexAPI.TYPE_CAP);
        indexObject.putNumber(IndexAPI.DOC_ATTRIBUTE_SIZE, 10);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_CREATE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        requestObject.putObject(IndexAPI.MSG_PROPERTY_DOCUMENT, indexObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                    
                    indexCapId = arangoResult.getString("id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test04CreateCapIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05CreateGeoIndex() {
        System.out.println("*** test05CreateGeoIndex ***");
        JsonObject indexObject = new JsonObject().putString(IndexAPI.DOC_ATTRIBUTE_TYPE, IndexAPI.TYPE_GEO);
        JsonArray fieldsArray = new JsonArray().addString(IndexAPI.DOC_ATTRIBUTE_LATITUDE);
        fieldsArray.addString(IndexAPI.DOC_ATTRIBUTE_LONGITUDE);
        indexObject.putArray("fields", fieldsArray);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_CREATE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        requestObject.putObject(IndexAPI.MSG_PROPERTY_DOCUMENT, indexObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                    
                    indexGeoId = arangoResult.getString("id");
                }
                catch (Exception e) {
                    VertxAssert.fail("test05CreateGeoIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06GetHashIndex() {
        System.out.println("*** test06GetHashIndex ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_GET);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ID, indexHashId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test06GetHashIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07GetIndexList() {
        System.out.println("*** test07GetIndexList ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_LIST);
        requestObject.putString(IndexAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test07GetIndexList");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test08DeleteCapIndex() {
        System.out.println("*** test08DeleteCapIndex ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_INDEX);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ACTION, IndexAPI.MSG_ACTION_DELETE);
        requestObject.putString(IndexAPI.MSG_PROPERTY_ID, indexCapId);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Wrong return code received (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    VertxAssert.assertTrue("Index operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No index id received", arangoResult.getString("id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test08DeleteCapIndex");
                }
                VertxAssert.testComplete();
            }
        });
    }

}
