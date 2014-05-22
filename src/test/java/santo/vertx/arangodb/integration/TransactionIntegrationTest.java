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
import santo.vertx.arangodb.rest.TransactionAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.TransactionAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionIntegrationTest extends BaseIntegrationTest {
        
    @Test
    public void test01ExecuteSimpleTransaction() {
        System.out.println("*** test01ExecuteSimpleTransaction ***");
        JsonObject transactionObject = new JsonObject();
        JsonObject collectionsObject = new JsonObject();
        collectionsObject.putString("write", vertexColName);
        transactionObject.putObject(TransactionAPI.DOC_ATTRIBUTE_COLLECTIONS, collectionsObject);
        StringBuilder action = new StringBuilder();
        action.append("function () {");
        action.append("var db = require('internal').db;");
        action.append("db.").append(vertexColName).append(".save({'description': 'transaction doc'});");
        action.append("return db.").append(vertexColName).append(".count();");
        action.append("}");
        transactionObject.putString(TransactionAPI.DOC_ATTRIBUTE_ACTION, action.toString());
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRANSACTION);
        requestObject.putString(TransactionAPI.MSG_PROPERTY_ACTION, TransactionAPI.MSG_ACTION_EXECUTE);
        requestObject.putObject(TransactionAPI.MSG_PROPERTY_DOCUMENT, transactionObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Transaction operation resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    VertxAssert.assertTrue("Transaction operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("wrong returncode received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 200);
                }
                catch (Exception e) {
                    VertxAssert.fail("test01ExecuteSimpleTransaction");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02ExecuteInvalidTransaction() {
        System.out.println("*** test02ExecuteInvalidTransaction ***");
        JsonObject transactionObject = new JsonObject();
        JsonObject collectionsObject = new JsonObject();
        collectionsObject.putString("write", "invalid-collection");
        transactionObject.putObject(TransactionAPI.DOC_ATTRIBUTE_COLLECTIONS, collectionsObject);
        StringBuilder action = new StringBuilder();
        action.append("function () {");
        action.append("var db = require('internal').db;");
        action.append("db.").append("invalid-collection").append(".save({'description': 'transaction doc'});");
        action.append("return db.").append("invalid-collection").append(".count();");
        action.append("}");
        transactionObject.putString(TransactionAPI.DOC_ATTRIBUTE_ACTION, action.toString());
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRANSACTION);
        requestObject.putString(TransactionAPI.MSG_PROPERTY_ACTION, TransactionAPI.MSG_ACTION_EXECUTE);
        requestObject.putObject(TransactionAPI.MSG_PROPERTY_DOCUMENT, transactionObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("Transaction operation didn't return the expected error: " + response.getString("status"), "error", response.getString("status"));
                    VertxAssert.assertTrue("Transaction operation didn't return the expected error: " + arangoResult.getBoolean("error"), arangoResult.getBoolean("error"));
                    VertxAssert.assertTrue("wrong returncode received: " + arangoResult.getInteger("code"), arangoResult.getInteger("code") == 404);
                }
                catch (Exception e) {
                    VertxAssert.fail("test02ExecuteInvalidTransaction");
                }
                VertxAssert.testComplete();
            }
        });
    }

}
