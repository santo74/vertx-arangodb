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
import santo.vertx.arangodb.rest.SimpleQueryAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.SimpleQueryAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleQueryIntegrationTest extends BaseIntegrationTest {
        
    @Test
    public void test01GetAll() {
        System.out.println("*** test01GetAll ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_ALL);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetAll() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetAll() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test01GetAll");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02GetByExample() {
        System.out.println("*** test02GetByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "from-doc");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByExample() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test02GetByExample");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03GetFirstExample() {
        System.out.println("*** test03GetFirstExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "to-doc");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FIRST_EXAMPLE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetFirstExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetFirstExample() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test03GetFirstExample");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04GetByExampleHash() {
        System.out.println("*** test04GetByExampleHash ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexHashId);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "from-doc");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_HASH);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByExampleHash() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByExampleHash() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test04GetByExampleHash");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test05GetByExampleSkiplist() {
        System.out.println("*** test05GetByExampleSkiplist ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexSkiplistId);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putNumber("age", 30);
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_SKIPLIST);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByExampleSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByExampleSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05GetByExampleSkiplist");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    // TODO
    /*
    @Test
    public void test06GetByExampleBitarray() {
        System.out.println("*** test06GetByExampleBitarray ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, "bitarray-id");
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "from-doc");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_BITARRAY);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByExampleBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByExampleBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test06GetByExampleBitarray");
                }
                VertxAssert.testComplete();
            }
        });
    }
    */
    
    @Test
    public void test07GetByConditionSkiplist() {
        System.out.println("*** test07GetByConditionSkiplist ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexSkiplistId);
        JsonObject conditionObject = new JsonObject();
        JsonArray conditionArray = new JsonArray();
        conditionArray.add(new JsonArray().add("<").add(40));
        conditionObject.putArray("age", conditionArray);
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_CONDITION, conditionObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_CONDITION_SKIPLIST);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByConditionSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByConditionSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test07GetByConditionSkiplist");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    // TODO
    /*
    @Test
    public void test08GetByConditionBitarray() {
        System.out.println("*** test08GetByConditionBitarray ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, "bitarray-id");
        JsonObject conditionObject = new JsonObject();
        conditionObject.putString("description", "from-doc");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_CONDITION, conditionObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_CONDITION_BITARRAY);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetByConditionBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetByConditionBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test08GetByConditionBitarray");
                }
                VertxAssert.testComplete();
            }
        });
    }
    */

    @Test
    public void test09GetAny() {
        System.out.println("*** test09GetAny ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_ANY);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetAny() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetAny() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test09GetAny");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test10GetRange() {
        System.out.println("*** test10GetRange ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_ATTRIBUTE, "age");
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_LEFT, 2);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_RIGHT, 100);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_RANGE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetRange() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetRange() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test10GetRange");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test11GetNear() {
        System.out.println("*** test11GetNear ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_NEAR);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetNear() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetNear() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test11GetNear");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test12GetWithin() {
        System.out.println("*** test12GetWithin ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
        queryObject.putNumber(SimpleQueryAPI.DOC_ATTRIBUTE_RADIUS, 5000);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_WITHIN);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetWithin() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetWithin() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test12GetWithin");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test13GetFulltext() {
        System.out.println("*** test13GetFulltext ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_ATTRIBUTE, "description");
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_QUERY, "doc");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FULLTEXT);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetFulltext() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetFulltext() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test13GetFulltext");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test14UpdateByExample() {
        System.out.println("*** test14UpdateByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "test2");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject newObject = new JsonObject();
        newObject.putString("description", "test2-updated");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_NEW_VALUE, newObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_UPDATE_BY_EXAMPLE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("UpdateByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("UpdateByExample() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test14UpdateByExample");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test15ReplaceByExample() {
        System.out.println("*** test15ReplaceByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "test2-updated");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject newObject = new JsonObject();
        newObject.putString("description", "removeme");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_NEW_VALUE, newObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_REPLACE_BY_EXAMPLE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("ReplaceByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("ReplaceByExample() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test15ReplaceByExample");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test16RemoveByExample() {
        System.out.println("*** test16RemoveByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.putString("description", "removeme");
        queryObject.putObject(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_REMOVE_BY_EXAMPLE);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("RemoveByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("RemoveByExample() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test16RemoveByExample");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test17GetFirst() {
        System.out.println("*** test17GetFirst ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FIRST);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetFirst() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetFirst() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test17GetFirst");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test18GetLast() {
        System.out.println("*** test18GetLast ***");
        JsonObject queryObject = new JsonObject();
        queryObject.putString(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.putString(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_LAST);
        requestObject.putObject(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("GetLast() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("GetLast() resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test18GetLast");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
