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
import santo.vertx.arangodb.rest.DatabaseAPI;

/**
 *
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CleanupIntegrationTest extends BaseIntegrationTest {
    
    @Test
    public void cleanupDatabase() {
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
                    VertxAssert.fail("cleanupDatabase");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
