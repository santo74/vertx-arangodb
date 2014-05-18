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
import santo.vertx.arangodb.Helper;

/**
 *
 * @author sANTo
 */
public class DatabaseAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/database";

    public static final String MSG_ACTION_CURRENT = "current";
    public static final String MSG_ACTION_USER = "user";
    public static final String MSG_ACTION_LIST = "list";
    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_DROP = "drop";

    public DatabaseAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }

    @Override
    public void processRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        
        // MANDATORY: action to perform
        String action = helper.getMandatoryString(request, MSG_PROPERTY_ACTION, msg);
        if (action == null) return;

        // OPTIONAL: headers to use for the operation
        JsonObject headers = helper.getOptionalObject(request, MSG_PROPERTY_HEADERS);

        // OPTIONAL: timeout for the action (defaults to 10sec)
        int timeout = helper.getOptionalInt(request, MSG_PROPERTY_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
        
        // OPTIONAL: database on which the action should be performed (will default to the database specified in the config or _system if none was specified)
        String dbName = helper.getOptionalString(request, MSG_PROPERTY_DATABASE);

        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_CURRENT:
                getCurrent(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_USER:
                getUser(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_LIST:
                getList(msg, timeout, headers);
                break;                
            case MSG_ACTION_CREATE:
                createDb(msg, timeout, headers);
                break;                
            case MSG_ACTION_DROP:
                dropDb(msg, timeout, headers, dbName);
                break;                

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                Helper.getHelper().sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }
    
    // retrieves information about the current database
    private void getCurrent(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        getDb(msg, timeout, headers, dbName, MSG_ACTION_CURRENT);
    }

    // retrieves a list of all databases the current user can access
    private void getUser(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        getDb(msg, timeout, headers, persistor.SYSTEM_DATABASE, MSG_ACTION_USER);
    }

    // retrieves a list of all existing databases
    private void getList(Message<JsonObject> msg, int timeout, JsonObject headers) {
        getDb(msg, timeout, headers, persistor.SYSTEM_DATABASE, null);
    }

    private void getDb(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String filter) {
        // prepare URI
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        if (filter != null) apiPath.append("/").append(filter);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // creates a new database
    private void createDb(Message<JsonObject> msg, int timeout, JsonObject headers) {
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // Ensure required parameters/attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_NAME, msg)) return;

        // prepare URI
        StringBuilder apiPath = new StringBuilder();
        apiPath.append("/_db/").append(persistor.SYSTEM_DATABASE);
        apiPath.append(API_PATH);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }
    
    
    // drops an existing database
    private void dropDb(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Ensure required parameters/attributes
        if (!ensureParameter(dbName, MSG_PROPERTY_DATABASE, msg)) return;
        if (dbName.equals(persistor.SYSTEM_DATABASE)) {
            helper.sendError(msg, "the database " + persistor.SYSTEM_DATABASE + " cannot be dropped");
        }

        // prepare URI
        StringBuilder apiPath = new StringBuilder();
        apiPath.append("/_db/").append(persistor.SYSTEM_DATABASE);
        apiPath.append(API_PATH);
        apiPath.append("/").append(dbName);

        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }
}
