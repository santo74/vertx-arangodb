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

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import santo.vertx.arangodb.Helper;

/**
 *
 * @author sANTo
 */
public class RestResponseHandler implements Handler<HttpClientResponse> {

    private String id = "";
    final StringBuffer responseData = new StringBuffer();
    
    DateFormat timeFormatter = DateFormat.getTimeInstance(DateFormat.DEFAULT, Locale.GERMAN);
    
    private Logger logger;
    private final String logPrefix = "";
    private Helper helper;
    private Message<JsonObject> msg = null;

    public RestResponseHandler(Logger logger) {
        this(null, UUID.randomUUID().toString(), logger, Helper.getHelper());
    }

    public RestResponseHandler(Message<JsonObject> msg, Logger logger, Helper helper) {
        this(msg, UUID.randomUUID().toString(), logger, helper);
    }

    public RestResponseHandler(Message<JsonObject> msg, String id, Logger logger, Helper helper) {
        this.msg = msg;
        this.id = id;
        this.logger = logger;
        this.helper = helper;
    }

    @Override
    public void handle(final HttpClientResponse response) {
        final int statusCode = response.statusCode();                    
        logger.trace("[RESPONSE-" + getId() + "] statuscode: " + statusCode + ", time: " + timeFormatter.format(new Date()));
        response.bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer body) {
                // The entire response body has been received
                JsonObject restResponse = new JsonObject();
                responseData.append(body);
                logger.trace("> response: (" + getId() + ")" + responseData);
                if (responseData != null && responseData.length() > 0) {
                    if (responseData.toString().startsWith("[")) restResponse = new JsonArray(responseData.toString()).get(0);
                    else restResponse = new JsonObject(responseData.toString());
                }
                
                // send response
                if (statusCode >= 200 && statusCode < 300) sendResponse(true, statusCode, restResponse);
                else sendResponse(false, statusCode, restResponse);
                
                logger.trace("[RESPONSE-" + getId() + "] body parsed" + ", time: " + timeFormatter.format(new Date()));
            }
        });
    }
    
    private void sendResponse(boolean success, int statuscode, Object result) {
        if (getMsg() != null) {
            if (success) helper.sendSuccess(getMsg(), statuscode, "success", result);
            else helper.sendError(getMsg(), statuscode, "error", result);
        }
    }

    protected String getId() {
        return this.id;
    }
    
    private Message<JsonObject> getMsg() {
        return this.msg;
    }
}
