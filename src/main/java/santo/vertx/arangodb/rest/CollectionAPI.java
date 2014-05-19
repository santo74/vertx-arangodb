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
public class CollectionAPI  extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/collection";

    public static final String INTERNAL_ACTION_PROPERTIES = "properties";
    
    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_TRUNCATE = "truncate";
    public static final String MSG_ACTION_READ = "read";
    public static final String MSG_ACTION_GET = "get";
    public static final String MSG_ACTION_GET_PROPERTIES = "get-properties";
    public static final String MSG_ACTION_COUNT = "count";
    public static final String MSG_ACTION_FIGURES = "figures";
    public static final String MSG_ACTION_REVISION = "revision";
    public static final String MSG_ACTION_CHECKSUM = "checksum";
    public static final String MSG_ACTION_LIST = "list";
    public static final String MSG_ACTION_LOAD = "load";
    public static final String MSG_ACTION_UNLOAD = "unload";
    public static final String MSG_ACTION_CHANGE_PROPERTIES = "change-properties";
    public static final String MSG_ACTION_RENAME = "rename";
    public static final String MSG_ACTION_ROTATE = "rotate";

    public CollectionAPI(Logger logger, ArangoPersistor persistor) {
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
        String dbName = helper.getOptionalString(request, MSG_PROPERTY_DATABASE, persistor.SETTING_DBNAME);

        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_CREATE:
                createCollection(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE:
                deleteCollection(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_TRUNCATE:
            case MSG_ACTION_LOAD:
            case MSG_ACTION_UNLOAD:
            case MSG_ACTION_RENAME:
            case MSG_ACTION_ROTATE:
                modifyCollection(msg, timeout, headers, dbName, action);
                break;                
            case MSG_ACTION_CHANGE_PROPERTIES:
                modifyCollection(msg, timeout, headers, dbName, INTERNAL_ACTION_PROPERTIES);
                break;                
            case MSG_ACTION_READ:
            case MSG_ACTION_GET:
            case MSG_ACTION_LIST:
                getCollection(msg, timeout, headers, dbName, null);
                break;
            case MSG_ACTION_GET_PROPERTIES:
                getCollection(msg, timeout, headers, dbName, INTERNAL_ACTION_PROPERTIES);
                break;
            case MSG_ACTION_COUNT:
            case MSG_ACTION_FIGURES:
            case MSG_ACTION_REVISION:
            case MSG_ACTION_CHECKSUM:
                getCollection(msg, timeout, headers, dbName, action);
                break;

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }
    
    // creates a new collection
    private void createCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params / attributes
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        if (!ensureAttribute(document, DOC_ATTRIBUTE_NAME, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified collection
    private void deleteCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String collection = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION, msg);
        if (collection == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(collection);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // modified the specified collection
    private void modifyCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String subAction) {
        // REQUIRED: The name of the collection to modify
        String collection = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION, msg);
        if (collection == null) return;

        // OPTIONAL: a document containing the update data for the operation
        JsonObject document = helper.getOptionalObject(msg.body(), MSG_PROPERTY_DOCUMENT);
        if (subAction.equals(MSG_ACTION_CHANGE_PROPERTIES) || subAction.equals(MSG_ACTION_RENAME)) if (document == null) return;

        //
        // check required attributes
        //
        
        if (subAction.equals(MSG_ACTION_RENAME)) if (!ensureAttribute(document, DOC_ATTRIBUTE_NAME, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(collection);
        apiPath.append("/").append(subAction);
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets (info about) the specified collection(s)
    private void getCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String filter) {
        // OPTIONAL (if not specified, then all collections will be retrieved)
        // The name of the collection for which to retrieve the info
        String collection = helper.getOptionalString(msg.body(), MSG_PROPERTY_COLLECTION);

        // OPTIONAL (only used for subaction checksum)
        // Whether or not to include document revision ids in the checksum calculation
        String withRevisions = helper.getOptionalString(msg.body(), MSG_PROPERTY_WITH_REVISIONS);

        // OPTIONAL (only used for subaction checksum)
        // Whether or not to include document body data in the checksum calculation
        String withData = helper.getOptionalString(msg.body(), MSG_PROPERTY_WITH_DATA);

        // OPTIONAL (only used when no subaction is specified, i.e. all collections are retrieved)
        // Whether or not system collections should be excluded from the result
        String excludeSystem = helper.getOptionalString(msg.body(), MSG_PROPERTY_EXCLUDE_SYSTEM);
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);        
        apiPath.append(API_PATH);
        
        // Append optional (query) parameters
        if (collection != null) {
            apiPath.append("/").append(collection);
            if (filter != null) {
                apiPath.append("/").append(filter);
                if (filter.equals(MSG_ACTION_CHECKSUM)) {
                    if (withRevisions != null) {
                        apiPath.append("/?").append(MSG_PROPERTY_WITH_REVISIONS + "=").append(withRevisions);
                    }
                    if (withData != null) {
                        if (withRevisions != null) apiPath.append("&");
                        else apiPath.append("/?");
                        apiPath.append(MSG_PROPERTY_WITH_DATA + "=").append(withData);
                    }
                }
            }
        }
        else {
            if (excludeSystem != null) apiPath.append("/?").append(MSG_PROPERTY_EXCLUDE_SYSTEM).append("=").append(excludeSystem);
        }            
        
        httpGet(persistor, apiPath.toString(), headers, timeout, msg);        
    }

}
