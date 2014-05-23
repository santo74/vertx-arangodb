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
import santo.vertx.arangodb.rest.DatabaseAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.DatabaseAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseIntegrationTest extends BaseIntegrationTest {
            
    @Test
    public void test00CleanupDatabase() {
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_DROP);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_DATABASE, dbName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    // We don't bother about the result, it's just a precaution before starting the tests
                }
                catch (Exception e) {
                    VertxAssert.fail("test00CleanupDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test01CreateDatabase() {
        System.out.println("*** test01CreateDatabase ***");
        JsonObject databaseObject = new JsonObject().putString("name", dbName);
        // Specify default user if authentication enabled to ensure we can access the database
        if (dbUser != null && dbPwd != null) {
            JsonObject usersObject = new JsonObject();
            usersObject.putString("username", dbUser);
            usersObject.putString("passwd", dbPwd);
            databaseObject.putArray("users", new JsonArray().add(usersObject));
        }
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_CREATE);
        requestObject.putObject(DatabaseAPI.MSG_PROPERTY_DOCUMENT, databaseObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Database creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Database could not be created", arangoResult.getBoolean("result"));
                    VertxAssert.assertTrue("The database was not created successfully: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 201);
                }
                catch (Exception e) {
                    VertxAssert.fail("test01CreateDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02GetCurrentDatabase() {
        System.out.println("*** test02GetCurrentDatabase ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_CURRENT);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_DATABASE, dbName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("The retrieval of the current database resulted in an error", !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("The request for the current database was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("current database : " + arangoResult.getObject("result"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test02GetCurrentDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03GetUserDatabases() {
        System.out.println("*** test03GetUserDatabases ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_USER);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_DATABASE, dbName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("The retrieval of the user databases resulted in an error", !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("The request for the user databases was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("user databases: " + arangoResult.getArray("result"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test03GetUserDatabases");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04ListDatabases() {
        System.out.println("*** test04ListDatabases ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_LIST);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Database listing resulted in an error", !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("The request was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("database list: " + arangoResult.getArray("result"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test04ListDatabases");
                }
                VertxAssert.testComplete();
            }
        });
    }

    /*
    @Test
    public void test05DropDatabase() {
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DATABASE);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_ACTION, DatabaseAPI.MSG_ACTION_DROP);
        requestObject.putString(DatabaseAPI.MSG_PROPERTY_DATABASE, dbName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Dropping database resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertTrue("Database could not be dropped", arangoResult.getBoolean("result"));
                    VertxAssert.assertTrue("The database was not dropped successfully: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test05DropDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }
    */

}
