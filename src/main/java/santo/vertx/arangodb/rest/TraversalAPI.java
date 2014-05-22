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
public class TraversalAPI extends AbstractRestAPI {
    
    public static final String API_PATH = API_BASE_PATH + "/traversal";

    public static final String MSG_ACTION_TRAVERSE = "traverse";

    public TraversalAPI(Logger logger, ArangoPersistor persistor) {
        this.logger = logger;
        this.persistor = persistor;
    }
    
    @Override
    protected void performAction(Message<JsonObject> msg, String action, JsonObject headers, int timeout, String dbName) {
        logger.trace(logPrefix + "Action: " + action);
        
        switch (action) {
            case MSG_ACTION_TRAVERSE:
                traverse(msg, timeout, headers, dbName);
                break;                
                
            default:
                logger.info(logPrefix + "invalid action, ignoring (" + action + ")");
                helper.sendError(msg, "invalid action, ignoring (" + action + ")");
        }
    }
    
    // traverses a graph
    private void traverse(Message<JsonObject> msg, int timeout, JsonObject headers, String dbName) {
        // check required params
        JsonObject document = helper.getMandatoryObject(msg.body(), MSG_PROPERTY_DOCUMENT, msg);
        if (document == null) return;
        
        // Check required attributes
        if (!ensureAttribute(document, DOC_ATTRIBUTE_START_VERTEX, msg)) return;
        if (!ensureAttribute(document, DOC_ATTRIBUTE_EDGE_COLLECTION, msg)) return;
        
        // Either direction or expander should be specified
        String direction = helper.getOptionalString(document, DOC_ATTRIBUTE_DIRECTION);
        String expander = helper.getOptionalString(document, DOC_ATTRIBUTE_EXPANDER);
        if (!ensureAttribute(Arrays.asList(direction, expander), Arrays.asList(DOC_ATTRIBUTE_DIRECTION, DOC_ATTRIBUTE_EXPANDER), msg)) return;
        
        // If direction is specified, then ensure its value is valid
        if (direction != null) ensureAttributeValue(DOC_ATTRIBUTE_DIRECTION, direction, Arrays.asList("outbound", "inbound", "any"), msg);

        // prepare PATH
        StringBuilder apiPath = new StringBuilder();
        if (dbName != null) apiPath.append("/_db/").append(dbName);
        apiPath.append(API_PATH);

        httpPost(persistor, apiPath.toString(), headers, document, timeout, msg);
    }

}
