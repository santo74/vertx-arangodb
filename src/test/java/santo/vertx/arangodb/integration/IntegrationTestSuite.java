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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.vertx.testtools.TestVerticle;

/**
 * Integration tests for the {@link santo.vertx.arangodb.ArangoPersistor} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DatabaseIntegrationTest.class
    ,CollectionIntegrationTest.class
    ,IndexIntegrationTest.class
    ,DocumentIntegrationTest.class
    ,EdgeIntegrationTest.class
    ,SimpleQueryIntegrationTest.class
    ,TransactionIntegrationTest.class
    ,CleanupIntegrationTest.class
})
public class IntegrationTestSuite extends TestVerticle {
}
