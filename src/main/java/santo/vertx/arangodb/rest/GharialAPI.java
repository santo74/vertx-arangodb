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
import static santo.vertx.arangodb.rest.AbstractRestAPI.MSG_PROPERTY_DOCUMENT;

/**
 *
 * @author sANTo
 */
public class GharialAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/gharial";

    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_CREATE_GRAPH = "create-graph";
    
    public static final String MSG_ACTION_READ = "read";
    public static final String MSG_ACTION_READ_GRAPH = "read-graph";
    public static final String MSG_ACTION_GET = "get";
    public static final String MSG_ACTION_GET_GRAPH = "get-graph";
    
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_DELETE_GRAPH = "delete-graph";
    public static final String MSG_ACTION_DROP = "drop";
    public static final String MSG_ACTION_DROP_GRAPH = "drop-graph";

    public static final String MSG_ACTION_LIST_VERTEX_COLLECTIONS = "list-vertex-collections"; // NEW
    public static final String MSG_ACTION_ADD_VERTEX_COLLECTION = "add-vertex-collection"; // NEW
    public static final String MSG_ACTION_REMOVE_VERTEX_COLLECTION = "remove-vertex-collection"; // NEW

    public static final String MSG_ACTION_LIST_EDGE_COLLECTIONS = "list-edge-collections"; // NEW
    public static final String MSG_ACTION_ADD_EDGE_COLLECTION = "add-edge-collection"; // NEW
    public static final String MSG_ACTION_REPLACE_EDGE_COLLECTION = "replace-edge-collection"; // NEW
    public static final String MSG_ACTION_REMOVE_EDGE_COLLECTION = "remove-edge-collection"; // NEW
    
    public static final String MSG_ACTION_CREATE_VERTEX = "create-vertex";

    public static final String MSG_ACTION_READ_VERTEX = "read-vertex";
    public static final String MSG_ACTION_GET_VERTEX = "get-vertex";    
    
    public static final String MSG_ACTION_UPDATE_VERTEX = "update-vertex";
    public static final String MSG_ACTION_MODIFY_VERTEX = "modify-vertex";
    
    public static final String MSG_ACTION_REPLACE_VERTEX = "replace-vertex";
    
    public static final String MSG_ACTION_DELETE_VERTEX = "delete-vertex";
    public static final String MSG_ACTION_REMOVE_VERTEX = "remove-vertex";

    public static final String MSG_ACTION_CREATE_EDGE = "create-edge";
    
    public static final String MSG_ACTION_READ_EDGE = "read-edge";
    public static final String MSG_ACTION_GET_EDGE = "get-edge";

    public static final String MSG_ACTION_UPDATE_EDGE = "update-edge";
    public static final String MSG_ACTION_MODIFY_EDGE = "modify-edge";

    public static final String MSG_ACTION_REPLACE_EDGE = "replace-edge";
    
    public static final String MSG_ACTION_DELETE_EDGE = "delete-edge";
    public static final String MSG_ACTION_REMOVE_EDGE = "remove-edge";        

    public GharialAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            // Graphs
            case MSG_ACTION_CREATE:
            case MSG_ACTION_CREATE_GRAPH:
                createGraph(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_READ:
            case MSG_ACTION_READ_GRAPH:
            case MSG_ACTION_GET:
            case MSG_ACTION_GET_GRAPH:
                getGraph(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_DELETE:
            case MSG_ACTION_DELETE_GRAPH:
            case MSG_ACTION_DROP:
            case MSG_ACTION_DROP_GRAPH:
                deleteGraph(msg, timeout, headers, dbName);
                break;            
            case MSG_ACTION_LIST_VERTEX_COLLECTIONS:
                listVertexCollections(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_ADD_VERTEX_COLLECTION:
                addVertexCollection(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_REMOVE_VERTEX_COLLECTION:
                removeVertexCollection(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_LIST_EDGE_COLLECTIONS:
                listEdgeCollections(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_ADD_EDGE_COLLECTION:
                addEdgeCollection(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_REPLACE_EDGE_COLLECTION:
                replaceEdgeCollection(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_REMOVE_EDGE_COLLECTION:
                removeEdgeCollection(msg, timeout, headers, dbName);
                break;
                        
            // Vertices
            case MSG_ACTION_CREATE_VERTEX:
                createVertex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_READ_VERTEX:
            case MSG_ACTION_GET_VERTEX:
                getVertex(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_REPLACE_VERTEX:
                replaceVertex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_UPDATE_VERTEX:
            case MSG_ACTION_MODIFY_VERTEX:
                updateVertex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE_VERTEX:
            case MSG_ACTION_REMOVE_VERTEX:
                deleteVertex(msg, timeout, headers, dbName);
                break;                

            // Edges
            case MSG_ACTION_CREATE_EDGE:
                createEdge(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_READ_EDGE:
            case MSG_ACTION_GET_EDGE:
                getEdge(msg, timeout, headers, dbName);
                break;
            case MSG_ACTION_REPLACE_EDGE:
                replaceEdge(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_UPDATE_EDGE:
            case MSG_ACTION_MODIFY_EDGE:
                updateEdge(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE_EDGE:
            case MSG_ACTION_REMOVE_EDGE:
                deleteEdge(msg, timeout, headers, dbName);
                break;                
                
            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }

    // creates a new graph
    private void createGraph(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_NAME, msg)) return;
        // Optional or required (unclear from docs) ?
        if (!ensureAttribute(document, DOC_ATTRIBUTE_EDGE_DEFINITIONS, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets properties of a specific graph or all graphs
    private void getGraph(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String name = helper.getOptionalString(msg.body(), MSG_PROPERTY_GRAPH_NAME);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        if (name != null) apiPath.append("/").append(name);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // deletes the specified graph
    private void deleteGraph(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String name = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (name == null) return;

        // get optional params
        boolean dropCollections = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_DROP_COLLECTIONS, false);
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(name);
        if (dropCollections) apiPath.append("/?").append(MSG_PROPERTY_DROP_COLLECTIONS).append("=").append(dropCollections);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // lists all vertex collections within the graph
    private void listVertexCollections(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // adds a vertex collection to the set of collections of the graph. (will be created if it doesn't exist)
    private void addVertexCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_COLLECTION, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }
    
    // removes a vertex collection from the graph and optionally deletes the collection
    private void removeVertexCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        // get optional params
        boolean dropCollection = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_DROP_COLLECTION, false);
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("vertex").append("/").append(colName);
        if (dropCollection) apiPath.append("/?").append(MSG_PROPERTY_DROP_COLLECTION).append("=").append(dropCollection);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // lists all edge collections within the graph
    private void listEdgeCollections(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // adds an edge collection to the set of collections of the graph. (will be created if it doesn't exist)
    private void addEdgeCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_FROM, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_TO, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // Change one specific edge definition. Will modify all occurences of this definition in all graphs of the db
    private void replaceEdgeCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_COLLECTION, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_FROM, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_TO, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge").append("/").append(edgeName);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // removes an edge definition from the graph and optionally deletes the collection
    private void removeEdgeCollection(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        // get optional params
        boolean dropCollection = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_DROP_COLLECTION, false);
        
        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("edge").append("/").append(edgeName);
        if (dropCollection) apiPath.append("/?").append(MSG_PROPERTY_DROP_COLLECTION).append("=").append(dropCollection);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // creates a new vertex in the specified graph
    private void createVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex").append("/").append(colName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets properties of a specific vertex
    private void getVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String vertexKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_KEY, msg);
        if (vertexKey == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(colName).append("/").append(vertexKey);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // updates the data of the specified vertex
    private void updateVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String vertexKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_KEY, msg);
        if (vertexKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: if false, then all attributes that have value null in patch document will be removed from existing document
        boolean keepNull = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_KEEPNULL, true); // keep all attributes by default

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(colName).append("/").append(vertexKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (!keepNull) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_KEEPNULL).append("=").append(keepNull);
        }
        
        httpPatch(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // replaces the data of the specified vertex
    private void replaceVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String vertexKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_KEY, msg);
        if (vertexKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(colName).append("/").append(vertexKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified vertex (will also remove all edge documents that are linked to this vertex)
    private void deleteVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String vertexKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_KEY, msg);
        if (vertexKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(colName).append("/").append(vertexKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // creates a new edge in the specified collection
    private void createEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_FROM, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_TO, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge").append("/").append(colName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets an edge from the specified collection
    private void getEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;

        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String edgeKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_KEY, msg);
        if (edgeKey == null) return;

        // get optional params

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(colName).append("/").append(edgeKey);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }
    
    // updates the data of the specified edge
    private void updateEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String edgeKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_KEY, msg);
        if (edgeKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: if false, then all attributes that have value null in patch document will be removed from existing document
        boolean keepNull = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_KEEPNULL, true); // keep all attributes by default

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(colName).append("/").append(edgeKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (!keepNull) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_KEEPNULL).append("=").append(keepNull);
        }
        
        httpPatch(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // replaces the data of the specified edge
    private void replaceEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String edgeKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_KEY, msg);
        if (edgeKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(colName).append("/").append(edgeKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified edge
    private void deleteEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String colName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_COLLECTION_NAME, msg);
        if (colName == null) return;

        String edgeKey = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_KEY, msg);
        if (edgeKey == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(colName).append("/").append(edgeKey);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

}
