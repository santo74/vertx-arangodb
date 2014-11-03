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

package santo.vertx.arangodb.integration;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.VertxAssert;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.DocumentAPI;
import santo.vertx.arangodb.rest.GraphAPI;
import santo.vertx.arangodb.rest.TraversalAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.GraphAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraphIntegrationTest extends BaseIntegrationTest {
    
    public static String idVertex01;
    public static String idVertex02;
    
    @Test
    public void test00PrepareGraphDocuments() {
        System.out.println("*** test00PrepareGraphDocuments ***");
        JsonObject documentObject = new JsonObject().putString("name", "vertex01");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    idVertex01 = arangoResult.getString("_id");
                    
                    // Create another document
                    JsonObject documentObject = new JsonObject().putString("name", "vertex02");
                    JsonObject requestObject = new JsonObject();
                    requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
                    requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
                    requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
                    vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> reply) {
                            try {
                                JsonObject response = reply.body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getObject("result");
                                VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                                if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                                idVertex02 = arangoResult.getString("_id");
                            }
                            catch (Exception e) {
                                VertxAssert.fail("test00PrepareGraphDocuments");
                            }
                            VertxAssert.testComplete();
                        }
                    });
                }
                catch (Exception e) {
                    VertxAssert.fail("test00PrepareGraphDocuments");
                }
            }
        });
    }

    
    @Test
    public void test01CreateGraph() {
        System.out.println("*** test01CreateGraph ***");
        JsonObject documentObject = new JsonObject().putString(GraphAPI.DOC_ATTRIBUTE_KEY, "testgraph");
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_VERTICES, vertexColName);
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_EDGES, edgeColName);
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_CREATE);
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Graph operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertNotNull("No graph object received", arangoResult.getObject("graph"));
                    VertxAssert.assertNotNull("No id received", arangoResult.getObject("graph").getString("_id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test01CreateGraph");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test02GetGraph() {
        System.out.println("*** test02GetGraph ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_READ);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("graph details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test02GetGraph");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test03CreateVertex() {
        System.out.println("*** test03CreateVertex ***");        
        JsonObject documentObject = new JsonObject();
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_KEY, "testvertex");
        documentObject.putString("testfield", "testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_CREATE_VERTEX);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Graph operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertNotNull("No vertex object received", arangoResult.getObject("vertex"));
                    VertxAssert.assertNotNull("No id received", arangoResult.getObject("vertex").getString("_id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test03CreateVertex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test04GetVertex() {
        System.out.println("*** test04GetVertex ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_READ_VERTEX);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_NAME, "testvertex");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test04GetVertex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test05UpdateVertex() {
        System.out.println("*** test05UpdateVertex ***");
        JsonObject documentObject = new JsonObject();
        documentObject.putString("testfield", "modified testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_UPDATE_VERTEX);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_NAME, "testvertex");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test05UpdateVertex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test06ReplaceVertex() {
        System.out.println("*** test06ReplaceVertex ***");
        JsonObject documentObject = new JsonObject();
        documentObject.putString("replaced testfield", "replaced testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_REPLACE_VERTEX);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_NAME, "testvertex");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test06ReplaceVertex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test07DeleteVertex() {
        System.out.println("*** test07DeleteVertex ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_DELETE_VERTEX);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_NAME, "testvertex");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test07DeleteVertex");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test08CreateEdge() {
        System.out.println("*** test08CreateEdge ***");        
        JsonObject documentObject = new JsonObject();
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_KEY, "testedgegraph");
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_FROM, idVertex01);
        documentObject.putString(GraphAPI.DOC_ATTRIBUTE_TO, idVertex02);
        documentObject.putString("testfield", "testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_CREATE_EDGE);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Graph operation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    VertxAssert.assertNotNull("No edge object received", arangoResult.getObject("edge"));
                    VertxAssert.assertNotNull("No id received", arangoResult.getObject("edge").getString("_id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test08CreateEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test09GetEdge() {
        System.out.println("*** test09GetEdge ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_READ_EDGE);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_EDGE_NAME, "testedgegraph");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test09GetEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test10UpdateEdge() {
        System.out.println("*** test10UpdateEdge ***");
        JsonObject documentObject = new JsonObject();
        documentObject.putString("testfield", "modified testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_UPDATE_EDGE);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_EDGE_NAME, "testedgegraph");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test10UpdateEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test11ReplaceEdge() {
        System.out.println("*** test11ReplaceEdge ***");
        JsonObject documentObject = new JsonObject();
        documentObject.putString("replaced testfield", "replaced testvalue");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_REPLACE_EDGE);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_EDGE_NAME, "testedgegraph");
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test11ReplaceEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test12GetEdges() {
        System.out.println("*** test12GetEdges ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_GET_EDGES);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_ID, idVertex01);
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, new JsonObject());
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("result details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test12GetEdges");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test13GetVertices() {
        System.out.println("*** test13GetVertices ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_GET_VERTICES);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_VERTEX_ID, idVertex01);
        requestObject.putObject(GraphAPI.MSG_PROPERTY_DOCUMENT, new JsonObject());
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("result details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test13GetVertices");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test14Traverse() {
        System.out.println("*** test14Traverse ***");
        JsonObject documentObject = new JsonObject().putString(TraversalAPI.DOC_ATTRIBUTE_START_VERTEX, idVertex01);
        documentObject.putString(TraversalAPI.DOC_ATTRIBUTE_EDGE_COLLECTION, edgeColName);
        documentObject.putString(TraversalAPI.DOC_ATTRIBUTE_DIRECTION, "any");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRAVERSAL);
        requestObject.putString(TraversalAPI.MSG_PROPERTY_ACTION, TraversalAPI.MSG_ACTION_TRAVERSE);
        requestObject.putObject(TraversalAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Traversal resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    //if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                }
                catch (Exception e) {
                    VertxAssert.fail("test14Traverse");
                }
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void test15DeleteEdge() {
        System.out.println("*** test15DeleteEdge ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_DELETE_EDGE);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.putString(GraphAPI.MSG_PROPERTY_EDGE_NAME, "testedgegraph");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);                    
                }
                catch (Exception e) {
                    VertxAssert.fail("test15DeleteEdge");
                }
                VertxAssert.testComplete();
            }
        });
    }
    
    @Test
    public void test16DeleteGraph() {
        System.out.println("*** test16DeleteGraph ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_ACTION, GraphAPI.MSG_ACTION_DELETE_GRAPH);
        requestObject.putString(GraphAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The graph operation resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    VertxAssert.fail("test16DeleteGraph");
                }
                VertxAssert.testComplete();
            }
        });
    }
}
