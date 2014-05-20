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
public class SimpleQueryAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/simple";
    
    public static final String MSG_ACTION_GET_ALL = "all";
    public static final String MSG_ACTION_GET_BY_EXAMPLE = "by-example";
    public static final String MSG_ACTION_GET_FIRST_EXAMPLE = "first-example";
    public static final String MSG_ACTION_GET_BY_EXAMPLE_HASH = "by-example-hash";
    public static final String MSG_ACTION_GET_BY_EXAMPLE_SKIPLIST = "by-example-skiplist";
    public static final String MSG_ACTION_GET_BY_EXAMPLE_BITARRAY = "by-example-bitarray";
    public static final String MSG_ACTION_GET_BY_CONDITION_SKIPLIST = "by-condition-skiplist";
    public static final String MSG_ACTION_GET_BY_CONDITION_BITARRAY = "by-condition-bitarray";
    public static final String MSG_ACTION_GET_ANY = "any";
    public static final String MSG_ACTION_GET_RANGE = "range";
    public static final String MSG_ACTION_GET_NEAR = "near";
    public static final String MSG_ACTION_GET_WITHIN = "within";
    public static final String MSG_ACTION_GET_FULLTEXT = "fulltext";
    public static final String MSG_ACTION_REMOVE_BY_EXAMPLE = "remove-by-example";
    public static final String MSG_ACTION_REPLACE_BY_EXAMPLE = "replace-by-example";
    public static final String MSG_ACTION_UPDATE_BY_EXAMPLE = "update-by-example";
    public static final String MSG_ACTION_GET_FIRST = "first";
    public static final String MSG_ACTION_GET_LAST = "last";

    public SimpleQueryAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        // REQUIRED: query to use for the action
        JsonObject query = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_QUERY, msg);
        if (query == null) return;

        switch (action) {
            case MSG_ACTION_GET_ALL:
                get(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_EXAMPLE:
                getOrRemoveByExample(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_FIRST_EXAMPLE:
                getOrRemoveByExample(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_EXAMPLE_HASH:
                getByExampleIndex(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_EXAMPLE_SKIPLIST:
                getByExampleIndex(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_EXAMPLE_BITARRAY:
                getByExampleIndex(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_CONDITION_SKIPLIST:
                getByConditionIndex(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_BY_CONDITION_BITARRAY:
                getByConditionIndex(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_ANY:
                get(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_RANGE:
                getRange(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_NEAR:
                getNear(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_WITHIN:
                getWithin(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_FULLTEXT:
                getFulltext(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_REMOVE_BY_EXAMPLE:
                getOrRemoveByExample(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_REPLACE_BY_EXAMPLE:
                modifyByExample(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_UPDATE_BY_EXAMPLE:
                modifyByExample(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_FIRST:
                get(msg, timeout, headers, dbName, action, query);
                break;                
            case MSG_ACTION_GET_LAST:
                get(msg, timeout, headers, dbName, action, query);
                break;                

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    private void get(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getOrRemoveByExample(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_EXAMPLE, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getByExampleIndex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_INDEX, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_EXAMPLE, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getByConditionIndex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_INDEX, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_CONDITION, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getRange(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_ATTRIBUTE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_LEFT, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_RIGHT, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getNear(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_LATITUDE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_LONGITUDE, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getWithin(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_LATITUDE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_LONGITUDE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_RADIUS, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void getFulltext(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_ATTRIBUTE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_QUERY, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    private void modifyByExample(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // Ensure required parameters/attributes
        if (!ensureAttribute(query, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_EXAMPLE, msg)) return;
        if (!ensureAttribute(query, DOC_ATTRIBUTE_NEW_VALUE, msg)) return;

        performSimpleQuery(msg, timeout, headers, dbName, action, query);
    }

    
    private void performSimpleQuery(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName, String action, JsonObject query) {
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(action);
                
        httpPut(persistor, apiPath.toString(), headers, query, timeout, msg);
    }

}
