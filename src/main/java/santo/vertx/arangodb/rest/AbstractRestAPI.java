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
    protected final String MSG_PROPERTY_ACTION = "action";
    protected final String MSG_PROPERTY_HEADERS = "headers";
    protected final String MSG_PROPERTY_DATABASE = "database";
    protected final String MSG_PROPERTY_DOCUMENT = "document";
    protected final String MSG_PROPERTY_TIMEOUT = "timeout";

    // DOCUMENT ATTRIBUTES
    protected final String DOC_ATTRIBUTE_NAME = "name";

    protected ArangoPersistor persistor = null;
    protected Logger logger;
    protected final String logPrefix = "";
    
    protected Helper helper = Helper.getHelper();
    
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
                    helper.sendError(msg, "parameter missing: one of the following parameters should be specified: " + paramBuilder.toString());
                }
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

    public abstract void processRequest(Message<JsonObject> msg);
}
