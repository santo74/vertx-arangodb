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

import java.util.Arrays;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import santo.vertx.arangodb.ArangoPersistor;
import static santo.vertx.arangodb.rest.AbstractRestAPI.API_BASE_PATH;

/**
 *
 * @author sANTo
 */
public class IndexAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/index";

    public static final String MSG_ACTION_READ = "read";
    public static final String MSG_ACTION_GET = "get";
    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_LIST = "list";

    public static final String TYPE_CAP = "cap";
    public static final String TYPE_HASH = "hash";
    public static final String TYPE_SKIPLIST = "skiplist";
    public static final String TYPE_GEO = "geo";
    public static final String TYPE_FULLTEXT = "fulltext";

    public IndexAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_READ:
            case MSG_ACTION_GET:
            case MSG_ACTION_LIST:
                getIndex(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_CREATE:
                createIndex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE:
                deleteIndex(msg, timeout, headers, dbName);
                break;                

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }
    
    // gets the specified index or all indexes to the specified collection
    private void getIndex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // OPTIONAL (if not specified, then a specific index id should be provided)
        // The name of the collection for which to retrieve all indexes
        String collection = helper.getOptionalString(msg.body(), MSG_PROPERTY_COLLECTION);

        // OPTIONAL (if not specified, then a collection should be provided)
        // The id of the index that should be retrieved
        String id = helper.getOptionalString(msg.body(), MSG_PROPERTY_ID);

        // Either collection or id should be specified
        if (!ensureParameter(Arrays.asList(id, collection), Arrays.asList(MSG_PROPERTY_ID, MSG_PROPERTY_COLLECTION), msg)) return;
                
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);        
        apiPath.append(API_PATH);
        if (id != null) {
            // retrieve specified index
            apiPath.append("/").append(id);
        }
        else {
            // retrieve all indexes for specified collection
            apiPath.append("/?").append(MSG_PROPERTY_COLLECTION).append("=").append(collection);
        }
        
        httpGet(persistor, apiPath.toString(), headers, timeout, msg);        
    }

    // creates a new index
    private void createIndex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params / attributes
        
        // REQUIRED: The name of the collection for which to create the index
        String collection = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION, msg);
        if (collection == null) return;

        // REQUIRED: A JSON representation of the index (see ArangoDB docs for details)
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        if (!ensureAttribute(document, DOC_ATTRIBUTE_TYPE, msg)) return;
        if (!document.getString(DOC_ATTRIBUTE_TYPE).equals(TYPE_CAP)) {
            if (!ensureAttribute(document, DOC_ATTRIBUTE_FIELDS, msg)) return;
        }

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/?").append(MSG_PROPERTY_COLLECTION).append("=").append(collection);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified index
    private void deleteIndex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

}
