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
import static santo.vertx.arangodb.rest.AbstractRestAPI.API_BASE_PATH;

/**
 *
 * @author sANTo
 */
public class GenericAPI extends AbstractRestAPI {
    
    public static final String MSG_ACTION_GET = "GET";
    public static final String MSG_ACTION_POST = "POST";
    public static final String MSG_ACTION_PUT = "PUT";
    public static final String MSG_ACTION_PATCH = "PATCH";
    public static final String MSG_ACTION_DELETE = "DELETE";
    public static final String MSG_ACTION_HEAD = "HEAD";

    public static final String MSG_PROPERTY_PATH = "path";
    public static final String MSG_PROPERTY_BODY = "body";

    public GenericAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_GET:
                doGet(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_POST:
                doPost(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_PUT:
                doPut(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_PATCH:
                doPatch(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE:
                doDelete(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_HEAD:
                doHead(msg, timeout, headers, dbName);
                break;                
                
            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    // Performs an HTTP GET request on the specified path
    private void doGet(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);
        
        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // Performs an HTTP POST request on the specified path
    private void doPost(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;

        // request body
        JsonObject body = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_BODY, msg);
        if (body == null) return;
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);

        httpPost(persistor, apiPath.toString(), headers, body, timeout, msg);
    }

    // Performs an HTTP PUT request on the specified path
    private void doPut(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;

        // Check optional params
        
        // request body
        JsonObject body = helper.getOptionalObject(msg.body(), MSG_PROPERTY_BODY);
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);

        httpPut(persistor, apiPath.toString(), headers, body, timeout, msg);
    }

    // Performs an HTTP PATCH request on the specified path
    private void doPatch(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;

        // Check optional params
        
        // request body
        JsonObject body = helper.getOptionalObject(msg.body(), MSG_PROPERTY_BODY);
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);

        httpPatch(persistor, apiPath.toString(), headers, body, timeout, msg);
    }

    // Performs an HTTP DELETE request on the specified path
    private void doDelete(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);

        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // Performs an HTTP HEAD request on the specified path
    private void doHead(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // Check required params
        
        // the path for the request
        String path = helper.getMandatoryString(msg.body(), MSG_PROPERTY_PATH, msg);
        if (path == null) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_BASE_PATH);
        if (!path.startsWith("/")) apiPath.append("/");
        apiPath.append(path);
        
        httpHead(persistor, apiPath.toString(), headers, timeout, msg);
    }

}
