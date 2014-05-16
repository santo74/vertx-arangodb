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

import java.net.URL;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.Helper;
import santo.vertx.arangodb.rest.DatabaseAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.DatabaseAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseIntegrationTest extends TestVerticle {
    
    private static final String DEFAULT_ADDRESS = "santo.vertx.arangodb";
    private static final String DEFAULT_TEST_DB = "testdb";
    
    private Logger logger;
    private final String logPrefix = "";
    private JsonObject config;
    private String address;
    private String dbName;
    
    @Override
    public void start() {
        initialize();
        logger = container.logger();
        config = loadConfig();
        address = Helper.getHelper().getOptionalString(config, "address", DEFAULT_ADDRESS);
        dbName = Helper.getHelper().getOptionalString(config, "dbname", DEFAULT_TEST_DB);
        
        // Deploy our persistor before starting the tests
        deployVerticle(ArangoPersistor.class.getName(), config, 1);
    }
    
    private void deployVerticle(final String vertName, JsonObject vertConfig, int vertInstances) {
        logger.trace(logPrefix + "(deployVerticle) vertName: " + vertName);
        if (vertName == null || vertConfig == null) {
            logger.error(logPrefix + "Unable to deploy the requested verticle because one of the parameters is invalid: " + "Name=" + vertName + ",Config=" + vertConfig);
            return;
        }
        container.deployVerticle(vertName, vertConfig, vertInstances, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                logger.info(logPrefix + "verticle " + vertName + (asyncResult.succeeded() ? " was deployed successfully !" : " failed to deploy"));
                VertxAssert.assertTrue(asyncResult.succeeded());
                VertxAssert.assertNotNull("Persistor deployment failed", asyncResult.result());
                startTests();
            }
        });
    }
        
    @Test
    public void test01CreateDatabase() {
        JsonObject databaseObject = new JsonObject().putString("name", dbName);
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
                    e.printStackTrace();
                    VertxAssert.fail("test01CreateDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02getCurrentDatabase() {
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
                    e.printStackTrace();
                    VertxAssert.fail("test02getCurrentDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03getUserDatabases() {
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
                    e.printStackTrace();
                    VertxAssert.fail("test03getUserDatabases");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04ListDatabases() {
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
                    e.printStackTrace();
                    VertxAssert.fail("test04ListDatabases");
                }
                VertxAssert.testComplete();
            }
        });
    }

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
                    e.printStackTrace();
                    VertxAssert.fail("test05DropDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }

    private JsonObject loadConfig() {
        logger.info(logPrefix + "(re)loading Config");
        URL url = getClass().getResource("/config.json");
        url.getFile();
        Buffer configBuffer = vertx.fileSystem().readFileSync(url.getFile());
        if (configBuffer != null) {
            return new JsonObject(configBuffer.toString());
        }
        
        return new JsonObject();
    }
}
