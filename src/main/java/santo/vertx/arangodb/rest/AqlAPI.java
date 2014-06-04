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
public class AqlAPI extends AbstractRestAPI {
    
    public static final String API_PATH_CURSOR = API_BASE_PATH + "/cursor";
    public static final String API_PATH_QUERY = API_BASE_PATH + "/query";
    public static final String API_PATH_EXPLAIN = API_BASE_PATH + "/explain";
    public static final String API_PATH_FUNCTION = API_BASE_PATH + "/aqlfunction";

    public static final String MSG_ACTION_EXECUTE = "execute";
    public static final String MSG_ACTION_CURSOR = "cursor";
    public static final String MSG_ACTION_EXECUTE_CURSOR = "execute-cursor";
    public static final String MSG_ACTION_VALIDATE = "validate";
    public static final String MSG_ACTION_VALIDATE_QUERY = "validate-query";
    public static final String MSG_ACTION_NEXT = "next";
    public static final String MSG_ACTION_CURSOR_NEXT = "cursor-next";
    public static final String MSG_ACTION_EXECUTE_NEXT = "execute-next";
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_DELETE_CURSOR = "delete-cursor";
    public static final String MSG_ACTION_EXPLAIN = "explain";
    public static final String MSG_ACTION_EXPLAIN_QUERY = "explain-query";
    public static final String MSG_ACTION_CREATE_FUNCTION = "create-function";
    public static final String MSG_ACTION_GET_FUNCTION = "get-function";
    public static final String MSG_ACTION_DELETE_FUNCTION = "delete-function";

    public AqlAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_EXECUTE:
            case MSG_ACTION_CURSOR:
            case MSG_ACTION_EXECUTE_CURSOR:
                execute(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_VALIDATE:
            case MSG_ACTION_VALIDATE_QUERY:
                validate(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_NEXT:
            case MSG_ACTION_CURSOR_NEXT:
            case MSG_ACTION_EXECUTE_NEXT:
                next(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE:
            case MSG_ACTION_DELETE_CURSOR:
                delete(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_EXPLAIN:
            case MSG_ACTION_EXPLAIN_QUERY:
                explain(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_CREATE_FUNCTION:
                createFunction(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_GET_FUNCTION:
                getFunction(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE_FUNCTION:
                deleteFunction(msg, timeout, headers, dbName);
                break;                
                
            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    // executes a query and creates a cursor when necessary
    private void execute(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_QUERY, msg)) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_CURSOR);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // parses the specified query and validates it
    private void validate(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_QUERY, msg)) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_QUERY);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // reads next batch from cursor
    private void next(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        
        // the cursor identifier
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_CURSOR);
        apiPath.append("/").append(id);

        httpPut(persistor, apiPath.toString(), headers, null, timeout, msg);
    }

    // deletes a cursor
    private void delete(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        
        // the cursor identifier
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;
                        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_CURSOR);
        apiPath.append("/").append(id);

        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // explains how an AQL query would be executed
    private void explain(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_EXPLAIN);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // creates or replaces an AQL user function
    private void createFunction(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_NAME, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_CODE, msg)) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_FUNCTION);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // retrieves all registered AQL user functions, optionally for given namespace
    private void getFunction(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check optional params
        String namespace = helper.getOptionalString(msg.body(), MSG_PROPERTY_NAMESPACE);        
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_FUNCTION);
        if (namespace != null) apiPath.append("/?").append(namespace);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // deletes the specified AQL user function or all user functions in namespace if name should be treated as group (i.e. group=true)
    private void deleteFunction(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        
        // the function name
        String name = helper.getMandatoryString(msg.body(), MSG_PROPERTY_NAME, msg);
        if (name == null) return;

        // check optional params
        boolean group = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_GROUP);        

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH_FUNCTION);
        apiPath.append("/").append(name);
        if (group) apiPath.append("/?").append(MSG_PROPERTY_GROUP).append("=").append(group);

        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

}
