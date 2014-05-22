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
public class GraphAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/graph";

    public static final String MSG_ACTION_CREATE = "create";
    public static final String MSG_ACTION_CREATE_GRAPH = "create-graph";
    public static final String MSG_ACTION_READ = "read";
    public static final String MSG_ACTION_READ_GRAPH = "read-graph";
    public static final String MSG_ACTION_GET = "get";
    public static final String MSG_ACTION_GET_GRAPH = "get-graph";
    public static final String MSG_ACTION_DELETE = "delete";
    public static final String MSG_ACTION_DELETE_GRAPH = "delete-graph";

    public static final String MSG_ACTION_CREATE_VERTEX = "create-vertex";
    public static final String MSG_ACTION_READ_VERTEX = "read-vertex";
    public static final String MSG_ACTION_GET_VERTEX = "get-vertex";
    public static final String MSG_ACTION_GET_VERTICES = "get-vertices";
    public static final String MSG_ACTION_REPLACE_VERTEX = "replace-vertex";
    public static final String MSG_ACTION_UPDATE_VERTEX = "update-vertex";
    public static final String MSG_ACTION_DELETE_VERTEX = "delete-vertex";

    public static final String MSG_ACTION_CREATE_EDGE = "create-edge";
    public static final String MSG_ACTION_READ_EDGE = "read-edge";
    public static final String MSG_ACTION_GET_EDGE = "get-edge";
    public static final String MSG_ACTION_GET_EDGES = "get-edges";
    public static final String MSG_ACTION_REPLACE_EDGE = "replace-edge";
    public static final String MSG_ACTION_UPDATE_EDGE = "update-edge";
    public static final String MSG_ACTION_DELETE_EDGE = "delete-edge";

    public GraphAPI(Logger logger, ArangoPersistor persistor) {
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
                deleteGraph(msg, timeout, headers, dbName);
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
                updateVertex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE_VERTEX:
                deleteVertex(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_GET_VERTICES:
                getVertices(msg, timeout, headers, dbName);
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
                updateEdge(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_DELETE_EDGE:
                deleteEdge(msg, timeout, headers, dbName);
                break;                
            case MSG_ACTION_GET_EDGES:
                getEdges(msg, timeout, headers, dbName);
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

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_KEY, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_VERTICES, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_EDGES, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

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

    // deletes the specified edge
    private void deleteGraph(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String name = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (name == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(name);
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // creates a new vertex in the specified graph
    private void createVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String name = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (name == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // Check required attributes
        //if (!ensureAttribute(document, DOC_ATTRIBUTE_KEY, msg)) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(name);
        apiPath.append("/vertex");
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets properties of a specific vertex
    private void getVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_NAME, msg);
        if (vertexName == null) return;

        // get optional params

        // OPTIONAL: target revision for the operation (e.g. get a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(vertexName);
        if (revision != null) apiPath.append("/?").append(MSG_PROPERTY_REVISION).append("=").append(revision);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // replaces the properties of the specified vertex
    private void replaceVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_NAME, msg);
        if (vertexName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(vertexName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // partially updates the properties of the specified vertex
    private void updateVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_NAME, msg);
        if (vertexName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION);

        // OPTIONAL: if false, then all attributes that have value null in patch document will be removed from existing document
        boolean keepNull = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_KEEPNULL, true); // keep all attributes by default

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(vertexName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        if (!keepNull) {
            if (waitForSync || revision != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_KEEPNULL).append("=").append(keepNull);
        }
        
        httpPatch(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified vertex
    private void deleteVertex(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_NAME, msg);
        if (vertexName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertex");
        apiPath.append("/").append(vertexName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // gets an optionally filtered list of related vertices of the specified start vertex
    private void getVertices(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexId = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_ID, msg);
        if (vertexId == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/vertices");
        apiPath.append("/").append(vertexId);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // creates a new edge in the specified graph
    private void createEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String name = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (name == null) return;

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
        apiPath.append("/").append(name);
        apiPath.append("/edge");
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // gets properties of a specific edge
    private void getEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        // get optional params

        // OPTIONAL: target revision for the operation (e.g. get a specific revision of a document)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(edgeName);
        if (revision != null) apiPath.append("/?").append(MSG_PROPERTY_REVISION).append("=").append(revision);

        httpGet(persistor, apiPath.toString(), headers, timeout, msg);
    }
    
    // replaces the properties of the specified edge
    private void replaceEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(edgeName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        
        httpPut(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // partially updates the properties of the specified edge
    private void updateEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // OPTIONAL: if false, then all attributes that have value null in patch document will be removed from existing document
        boolean keepNull = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_KEEPNULL, true); // keep all attributes by default

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(edgeName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        if (!keepNull) {
            if (waitForSync || revision != null) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_KEEPNULL).append("=").append(keepNull);
        }
        
        httpPatch(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

    // deletes the specified edge
    private void deleteEdge(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String edgeName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_EDGE_NAME, msg);
        if (edgeName == null) return;

        // get optional params
        
        // OPTIONAL: Wait until document has been synced to disk
        boolean waitForSync = helper.getOptionalBoolean(msg.body(), MSG_PROPERTY_WAIT_FOR_SYNC, false);

        // OPTIONAL: target revision for the operation (e.g. update a specific revision of a vertex)
        String revision = helper.getOptionalString(msg.body(), MSG_PROPERTY_REVISION, null);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edge");
        apiPath.append("/").append(edgeName);
        if (waitForSync) apiPath.append("/?").append(MSG_PROPERTY_WAIT_FOR_SYNC).append("=").append(waitForSync);
        if (revision != null) {
            if (waitForSync) apiPath.append("&");
            else apiPath.append("/?");
            apiPath.append(MSG_PROPERTY_REVISION).append("=").append(revision);
        }
        
        httpDelete(persistor, apiPath.toString(), headers, timeout, msg);
    }

    // gets an optionally filtered list of edges for which the specified start vertex is an inbound or outbound relation
    private void getEdges(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;

        String graphName = helper.getMandatoryString(msg.body(), MSG_PROPERTY_GRAPH_NAME, msg);
        if (graphName == null) return;
        
        String vertexId = helper.getMandatoryString(msg.body(), MSG_PROPERTY_VERTEX_ID, msg);
        if (vertexId == null) return;

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null && dbName.length() > 0) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);
        apiPath.append("/").append(graphName);
        apiPath.append("/edges");
        apiPath.append("/").append(vertexId);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

}
