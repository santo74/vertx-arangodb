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
import java.util.List;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpHeaders;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.Helper;

/**
 *
 * @author sANTo
 */
public abstract class AbstractRestAPI {
    
    protected static final String API_BASE_PATH = "/_api";
    protected int DEFAULT_REQUEST_TIMEOUT = 10000;

    // MESSAGE PROPERTIES
    public static final String MSG_PROPERTY_ACTION = "action";
    public static final String MSG_PROPERTY_HEADERS = "headers";
    public static final String MSG_PROPERTY_DATABASE = "database";
    public static final String MSG_PROPERTY_DOCUMENT = "document";
    public static final String MSG_PROPERTY_TIMEOUT = "timeout";
    public static final String MSG_PROPERTY_ID = "id";
    public static final String MSG_PROPERTY_COLLECTION = "collection";
    public static final String MSG_PROPERTY_CREATE_COLLECTION = "createCollection";
    public static final String MSG_PROPERTY_WAIT_FOR_SYNC = "waitForSync";
    public static final String MSG_PROPERTY_REVISION = "rev";
    public static final String MSG_PROPERTY_POLICY = "policy";
    public static final String MSG_PROPERTY_KEEPNULL = "keepNull";
    public static final String MSG_PROPERTY_WITH_REVISIONS = "withRevisions";
    public static final String MSG_PROPERTY_WITH_DATA = "withData";
    public static final String MSG_PROPERTY_EXCLUDE_SYSTEM = "excludeSystem";
    public static final String MSG_PROPERTY_FROM = "from";
    public static final String MSG_PROPERTY_TO = "to";
    public static final String MSG_PROPERTY_VERTEX = "vertex";
    public static final String MSG_PROPERTY_DIRECTION = "direction";
    public static final String MSG_PROPERTY_QUERY = "query";
    public static final String MSG_PROPERTY_NAME = "name";
    public static final String MSG_PROPERTY_GRAPH_NAME = "graph-name";
    public static final String MSG_PROPERTY_VERTEX_NAME = "vertex-name";
    public static final String MSG_PROPERTY_EDGE_NAME = "edge-name";
    public static final String MSG_PROPERTY_VERTEX_ID = "vertex-id";

    // DOCUMENT ATTRIBUTES
    public static final String DOC_ATTRIBUTE_NAME = "name";
    public static final String DOC_ATTRIBUTE_COLLECTION = "collection";
    public static final String DOC_ATTRIBUTE_COLLECTIONS = DOC_ATTRIBUTE_COLLECTION + "s";
    public static final String DOC_ATTRIBUTE_EXAMPLE = "example";
    public static final String DOC_ATTRIBUTE_INDEX = "index";
    public static final String DOC_ATTRIBUTE_CONDITION = "condition";
    public static final String DOC_ATTRIBUTE_ATTRIBUTE = "attribute";
    public static final String DOC_ATTRIBUTE_LEFT = "left";
    public static final String DOC_ATTRIBUTE_RIGHT = "right";
    public static final String DOC_ATTRIBUTE_LATITUDE = "latitude";
    public static final String DOC_ATTRIBUTE_LONGITUDE = "longitude";
    public static final String DOC_ATTRIBUTE_RADIUS = "radius";
    public static final String DOC_ATTRIBUTE_QUERY = "query";
    public static final String DOC_ATTRIBUTE_NEW_VALUE = "newValue";
    public static final String DOC_ATTRIBUTE_TYPE = "type";
    public static final String DOC_ATTRIBUTE_SIZE = "size";
    public static final String DOC_ATTRIBUTE_FIELDS = "fields";
    public static final String DOC_ATTRIBUTE_ACTION = "action";
    public static final String DOC_ATTRIBUTE_KEY = "_key";
    public static final String DOC_ATTRIBUTE_VERTICES = "vertices";
    public static final String DOC_ATTRIBUTE_EDGES = "edges";
    public static final String DOC_ATTRIBUTE_FROM = "_from";
    public static final String DOC_ATTRIBUTE_TO = "_to";
    public static final String DOC_ATTRIBUTE_START_VERTEX = "startVertex";
    public static final String DOC_ATTRIBUTE_EDGE_COLLECTION = "edgeCollection";
    public static final String DOC_ATTRIBUTE_DIRECTION = "direction";
    public static final String DOC_ATTRIBUTE_EXPANDER = "expander";

    protected ArangoPersistor persistor = null;
    protected Logger logger;
    protected final String logPrefix = "";
    
    protected Helper helper = Helper.getHelper();
    
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
        
        performAction(msg, action, headers, timeout, dbName);
    }
    
    protected HttpClientRequest addRequestHeaders(HttpClientRequest clientRequest, ArangoPersistor persistor, JsonObject headers) {
        // set headers
        if (persistor.getCredentials() != null) clientRequest.putHeader(HttpHeaders.AUTHORIZATION, "Basic " + persistor.getCredentials());
        if (headers != null) {
            for (String header : headers.getFieldNames()) {
                clientRequest.putHeader(header, headers.getString(header));
            }
        }
        
        return clientRequest;
    }
    
    /**
     * Checks whether the specified parameter is available, sends an error message if it's not and finally returns a boolean indicating the availability
     * 
     * @param parameter the parameter that should be available
     * @param paramName the name of the parameter
     * @param msg the Vertx Message object to which the error message can be send
     * @return true if parameter is available, false if not
     */
    protected boolean ensureParameter(Object parameter, String paramName, Message<JsonObject> msg) {
        return ensureParameter(Arrays.asList(parameter), Arrays.asList(paramName), msg);
    }
    
    /**
     * Checks whether at least one of the specified parameters is available, sends an error message if it's not and finally returns a boolean indicating the availability
     * 
     * @param parameter a {@link List} containing parameters of which at least one should be available
     * @param paramName a {@link List} containing the parameter names corresponding to the parameters specified in the parameter list
     * @param msg the Vertx Message object to which the error message can be send
     * @return true if parameter is available, false if not
     */
    protected boolean ensureParameter(List parameter, List paramName, Message<JsonObject> msg) {
        boolean available = false;
        
        // in case of multiple params we want to ensure at least one of the parameters in that list is available
        if (parameter.size() > 1) {
            for (Object oParam : ((List) parameter)) {
                if (oParam != null) available = true;
            }
            
            if (!available) {
                StringBuilder paramBuilder = new StringBuilder();
                for (Object oParamName : paramName) {
                    if (paramBuilder.length() > 0) paramBuilder.append(",");
                    paramBuilder.append(oParamName);
                }
                helper.sendError(msg, "parameter missing: one of the following parameters should be specified: " + paramBuilder.toString());
            }
        }
        else {
            // we specified the parameter directly rather than in a list. let's check its availability
            if (parameter.get(0) != null) {
                available = true;
            }
            else {
                helper.sendError(msg, "parameter missing: " + paramName.get(0));
            }
        }
        
        return available;
    }

    /**
     * Checks whether at least one of the specified attributes is available, sends an error message if it's not and finally returns a boolean indicating the availability
     * 
     * @param attribute a {@link List} containing attributes of which at least one should be available
     * @param attributeName a {@link List} containing the attribute names corresponding to the attributes specified in the attribute list
     * @param msg the Vertx Message object to which the error message can be send
     * @return true if attribute is available, false if not
     */
    protected boolean ensureAttribute(List attribute, List attributeName, Message<JsonObject> msg) {
        boolean available = false;
        
        // in case of multiple attributes we want to ensure at least one of the attributes in that list is available (i.e. not null)
        if (attribute.size() > 1) {
            for (Object oAttribute : attribute) {
                if (oAttribute != null) available = true;
            }
            
            if (!available) {
                StringBuilder attributeBuilder = new StringBuilder();
                for (Object oAttributeName : attributeName) {
                    if (attributeBuilder.length() > 0) attributeBuilder.append(",");
                    attributeBuilder.append(oAttributeName);
                }
                helper.sendError(msg, "attribute missing: one of the following attributes should be specified: " + attributeBuilder.toString());
            }
        }
        else {
            // we specified the attribute directly rather than in a list. let's check its availability
            if (attribute.get(0) != null) {
                available = true;
            }
            else {
                helper.sendError(msg, "attribute missing: " + attributeName.get(0));
            }
        }
        
        return available;
    }

    /**
     * Checks whether the specified document attribute is available, sends an error message if it's not and finally returns a boolean indicating the availability
     * 
     * @param document The document that should contain the attribute
     * @param attributeName the attribute that needs to be checked
     * @param msg the Vertx Message object to which the error message can be send
     * @return true if attribute is available, false if not
     */
    protected boolean ensureAttribute(JsonObject document, String attributeName, Message<JsonObject> msg) {
        if (!document.containsField(attributeName)) {
            helper.sendError(msg, "document attribute missing: " + attributeName);
            return false;
        }
        return true;
    }

    /**
     * Checks whether the specified attribute equals one of the provided values
     * 
     * @param attributeName the attribute that needs to be checked
     * @param attributeValue the actual value of the attribute
     * @param attributeValues the list of possible values for the attribute
     * @param msg the Vertx Message object to which the error message can be send
     * @return true if attribute has a valid value, false if not
     */
    protected boolean ensureAttributeValue(String attributeName, Object attributeValue, List attributeValues, Message<JsonObject> msg) {
        boolean valid = false;
        
        if (attributeValues.size() > 1) {
            for (Object oValue : attributeValues) {
                if (attributeValue.equals(oValue)) valid = true;
            }
            
            if (!valid) {
                StringBuilder valueBuilder = new StringBuilder();
                for (Object oValue : attributeValues) {
                    if (valueBuilder.length() > 0) valueBuilder.append(",");
                    valueBuilder.append(oValue);
                }
                helper.sendError(msg, "Invalid attribute value! Attribute \"" + attributeName + "\" should have one of the following values: " + valueBuilder.toString());
            }
        }
        else {
            // we specified the parameter directly rather than in a list. let's check its availability
            if (attributeValue.equals(attributeValues.get(0))) {
                valid = true;
            }
            else {
                helper.sendError(msg, "Invalid attribute value! Attribute \"" + attributeName + "\" should have the following value: " + attributeValues.get(0));
            }
        }

        return valid;
    }

    /**
     * Performs a HTTP GET request on the specified address
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpGet(ArangoPersistor persistor, String apiPath, JsonObject headers, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().get(apiPath, new RestResponseHandler(msg, logger, helper));

        // set Headers and end the request
        addRequestHeaders(clientRequest, persistor, headers).setTimeout(timeout).end();        
    }
    
    /**
     * Performs a HTTP POST request on the specified address, using the body parameter for the request body
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param body the JSON document to send in the body of the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpPost(ArangoPersistor persistor, String apiPath, JsonObject headers, JsonObject body, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().post(apiPath, new RestResponseHandler(msg, logger, helper));
        
        // set headers
        clientRequest = addRequestHeaders(clientRequest, persistor, headers);
        // set content-length before we write the body !
        clientRequest.putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(body.toString().length()));
        
        // write the body
        clientRequest.write(body.toString());
        
        // end the request
        clientRequest.setTimeout(timeout).end();
    }
    
    /**
     * Performs a HTTP PUT request on the specified address, using the body parameter for the request body
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param body the JSON document to send in the body of the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpPut(ArangoPersistor persistor, String apiPath, JsonObject headers, JsonObject body, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().put(apiPath, new RestResponseHandler(msg, logger, helper));
        
        // set Headers and end the request
        addRequestHeaders(clientRequest, persistor, headers);
        
        if (body != null) {
        // set content-length before we write the body !
            clientRequest.putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(body.toString().length()));
            // write the body
            clientRequest.write(body.toString());
        }
        
        // end the request
        clientRequest.setTimeout(timeout).end();        
    }

    /**
     * Performs a HTTP DELETE request on the specified address
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpDelete(ArangoPersistor persistor, String apiPath, JsonObject headers, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().delete(apiPath, new RestResponseHandler(msg, logger, helper));
        
        // set Headers and end the request
        addRequestHeaders(clientRequest, persistor, null).setTimeout(timeout).end();        
    }
    
    /**
     * Performs a HTTP PATCH request on the specified address, using the body parameter for the request body
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param body the JSON document to send in the body of the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpPatch(ArangoPersistor persistor, String apiPath, JsonObject headers, JsonObject body, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().patch(apiPath, new RestResponseHandler(msg, logger, helper));
        
        // set headers
        clientRequest = addRequestHeaders(clientRequest, persistor, headers);
        // set content-length before we write the body !
        clientRequest.putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(body.toString().length()));
        
        // write the body
        clientRequest.write(body.toString());
        
        // end the request
        clientRequest.setTimeout(timeout).end();        
    }
    
    /**
     * Performs a HTTP HEAD request on the specified address
     * 
     * @param persistor instance of the RestPersistor
     * @param apiPath URL path to use for the request
     * @param headers optional headers to set in the request
     * @param timeout timeout for the HTTP connection
     * @param msg the Vertx Message object to which the error message can be send
     */
    protected void httpHead(ArangoPersistor persistor, String apiPath, JsonObject headers, int timeout, Message<JsonObject> msg) {
        // launch the request
        HttpClientRequest clientRequest = persistor.getClient().head(apiPath, new RestResponseHandler(msg, logger, helper));
        
        // set Headers and end the request
        addRequestHeaders(clientRequest, persistor, headers).setTimeout(timeout).end();        
    }

    protected abstract void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName);
}
