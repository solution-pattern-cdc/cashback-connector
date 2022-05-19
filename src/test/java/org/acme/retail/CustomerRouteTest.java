package org.acme.retail;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;

public class CustomerRouteTest extends CamelTestSupport {

    @Override
    public boolean isUseAdviceWith() {
        return true;
    }

    void adviceWithRoute() throws Exception {
        AdviceWith.adviceWith(context, "kafka2PostgresqlCustomer", a -> {
            a.replaceFromWith("direct:start");
            a.weaveByToUri("jdbc:cashback").replace().to("mock:jdbc:cashback");
        });
        context.start();
    }

    @Test
    public void testCreateCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedBodiesReceived("INSERT INTO customer (customer_id, name, status) VALUES (1000, 'Kasey Keith', 'silver');");
        template.sendBody("direct:start", getCreateCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Test
    public void testUpdateCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedBodiesReceived("UPDATE customer SET name = 'Kasey Keith', status = 'silver' WHERE customer_id = 1000;");
        template.sendBody("direct:start", getUpdateCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Test
    public void testReadCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedBodiesReceived("INSERT INTO customer (customer_id, name, status) VALUES (1000, 'Kasey Keith', 'silver');");
        template.sendBody("direct:start", getCreateCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Test
    public void testDeleteCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedMessageCount(0);
        template.sendBody("direct:start", getDeleteCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Test
    public void testTruncateCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedMessageCount(0);
        template.sendBody("direct:start", getTruncateCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Test
    public void testMessageCustomerChangeEvent() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedMessageCount(0);
        template.sendBody("direct:start", getMessageCustomerChangeEvent());

        assertMockEndpointsSatisfied();
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new CustomerRoute();
    }

    String getCreateCustomerChangeEvent() {
        return "{" +
                "        \"before\": null, " +
                "        \"after\": { " +
                "            \"customer_id\": 1000," +
                "            \"name\": \"Kasey Keith\"," +
                "            \"status\": \"silver\"" +
                "        }," +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"c\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }

    String getUpdateCustomerChangeEvent() {
        return "{" +
                "        \"before\": null, " +
                "        \"after\": { " +
                "            \"customer_id\": 1000," +
                "            \"name\": \"Kasey Keith\"," +
                "            \"status\": \"silver\"" +
                "        }," +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"u\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }

    String getReadCustomerChangeEvent() {
        return "{" +
                "        \"before\": null, " +
                "        \"after\": { " +
                "            \"customer_id\": 1000," +
                "            \"name\": \"Kasey Keith\"," +
                "            \"status\": \"silver\"" +
                "        }," +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"r\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }

    String getDeleteCustomerChangeEvent() {
        return "{" +
                "        \"before\": { " +
                "            \"customer_id\": 1000"  +
                "        }," +
                "        \"after\": null, " +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"d\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }

    String getMessageCustomerChangeEvent() {
        return "{" +
                "        \"message\": { " +
                "            \"prefix\": \"foo\"," +
                "            \"content\": \"Ymfy\"" +
                "        }," +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"m\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }

    String getTruncateCustomerChangeEvent() {
        return "{" +
                "        \"source\": { " +
                "            \"version\": \"1.7.0.Final\"," +
                "            \"connector\": \"postgresql\"," +
                "            \"name\": \"retail.updates\"," +
                "            \"ts_ms\": 1652094323144," +
                "            \"snapshot\": true," +
                "            \"db\": \"retail\"," +
                "            \"schema\": \"public\"," +
                "            \"table\": \"customer\"," +
                "            \"txId\": 555," +
                "            \"lsn\": 24037904," +
                "            \"xmin\": null" +
                "        }," +
                "        \"op\": \"t\", " +
                "        \"ts_ms\": 1652094323149," +
                "        \"transaction\": null" +
                "}";
    }
}
