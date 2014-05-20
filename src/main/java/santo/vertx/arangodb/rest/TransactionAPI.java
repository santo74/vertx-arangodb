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

package santo.vertx.arangodb.rest;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import santo.vertx.arangodb.ArangoPersistor;

/**
 *
 * @author sANTo
 */
public class TransactionAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/transaction";
            
    public static final String MSG_ACTION_EXECUTE = "execute";
    
    public TransactionAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_EXECUTE:
                executeTransaction(msg, timeout, headers, dbName);
                break;

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    private void executeTransaction(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Ensure required parameters/attributes
        
        // REQUIRED: A JSON representation of the index (see ArangoDB docs for details)
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        if (!ensureAttribute(document, DOC_ATTRIBUTE_COLLECTIONS, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_ACTION, msg)) return;

        // prepare URI
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        
        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

}
