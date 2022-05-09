package org.acme.retail;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;

public class AggregatedSaleRouteTest extends CamelTestSupport {

    @Override
    public boolean isUseAdviceWith() {
        return true;
    }

    void adviceWithRoute() throws Exception {
        AdviceWith.adviceWith(context, "kafka2PostgresqlAggregatedSale", a -> {
            a.replaceFromWith("direct:start");
            a.weaveByToUri("jdbc:cashback").replace().to("mock:jdbc:cashback");
        });
        context.start();
    }

    @Test
    public void testAggregatedSale() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedBodiesReceived("INSERT INTO expense (sale_id, customer_id, amount, date) VALUES (1000, 1001, 678.99, TIMESTAMP '2022-05-09 14:10:09');");
        template.sendBody("direct:start", getAggregatedSaleEvent());

        assertMockEndpointsSatisfied();
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new AggregatedSalesRoute();
    }

    String getAggregatedSaleEvent() {
        return "{" +
                "        \"sale_id\": 1000," +
                "        \"customer_id\": 1001," +
                "        \"total\": 678.99," +
                "        \"date\": 1652105409000000" +
                "}";
    }
}
