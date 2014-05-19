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
public class DocumentAPI  extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/document";

    public static final String MSG_ACTION_READ = "read";
    public static final String MSG_ACTION_GET = "get";
    public static final String MSG_ACTION_BY_ID = "by-id";
    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_REPLACE = "replace";
    public static final String MSG_ACTION_PATCH = "patch";
    public static final String MSG_ACTION_UPDATE = "update";
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_HEAD = "head";
    public static final String MSG_ACTION_HEADER = "header";
    public static final String MSG_ACTION_LIST = "list";

    public DocumentAPI(Logger logger, ArangoPersistor persistor) {
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
            case MSG_ACTION_READ:
            case MSG_ACTION_GET:
            case MSG_ACTION_BY_ID:
                getDocument(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_CREATE:
                createDocument(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_REPLACE:
                replaceDocument(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_PATCH:
            case MSG_ACTION_UPDATE:
                updateDocument(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE:
                deleteDocument(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_HEAD:
            case MSG_ACTION_HEADER:
                getDocumentHeader(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_LIST:
                getList(msg, timeout, headers, dbName);
                break;                

            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    // retrieves the specified document
    private void getDocument(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // creates a new document
    private void createDocument(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String collection = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION, msg);
        if (collection == null) return;

        // get optional params
        
        // OPTIONAL: whether the collection should be created if it doesn't exist (not supported for clusters !)
        boolean createCollection = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_CREATE_COLLECTION, false);

        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/?").append(MSG_PROPERTY_COLLECTION).append("=").append(collection);
        if (createCollection) apiPath.append("&").append(MSG_PROPERTY_CREATE_COLLECTION).append("=").append(createCollection);
        if (waitForSync) apiPath.append("&").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // replaces the specified document
    private void replaceDocument(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // OPTIONAL: policy to control update behaviour in case of a mismatch
        String policy = helper.getOptionalString(msg.body(), MSG_PROPERTY_POLICY, null);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        if (policy != null) {
            if (waitForSync || revision != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_POLICY).append("=").append(policy);
        }
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // updates (patches) the specified document
    private void updateDocument(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // OPTIONAL: policy to control update behaviour in case of a mismatch
        String policy = helper.getOptionalString(msg.body(), MSG_PROPERTY_POLICY, null);

        // OPTIONAL: if false, then all attributes that have value null in patch document will be removed from existing document
        boolean keepNull = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_KEEPNULL, true); // keep all attributes by default

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        if (policy != null) {
            if (waitForSync || revision != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_POLICY).append("=").append(policy);
        }
        if (!keepNull) {
            if (waitForSync || revision != null || policy != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_KEEPNULL).append("=").append(keepNull);
        }
        
        httpPatch(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified document
    private void deleteDocument(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // OPTIONAL: policy to control update behaviour in case of a mismatch
        String policy = helper.getOptionalString(msg.body(), MSG_PROPERTY_POLICY, null);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        if (policy != null) {
            if (waitForSync || revision != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_POLICY).append("=").append(policy);
        }
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // reads a document header
    private void getDocumentHeader(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String id = helper.getMandatoryString(msg.body(), MSG_PROPERTY_ID, msg);
        if (id == null) return;

        // get optional params
        
        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(id);
        if (revision != null) apiPath.append("/?").append(MSG_PROPERTY_REVISION).append("=").append(revision);
        
        httpHead(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // retrieves all documents from the specified collection
    private void getList(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String collection = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION, msg);
        if (collection == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/?").append(MSG_PROPERTY_COLLECTION).append("=").append(collection);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

}