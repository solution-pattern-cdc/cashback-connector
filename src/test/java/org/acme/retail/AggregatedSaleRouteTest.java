package org.acme.retail;

import org.apache.camel.Exchange;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

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
        AdviceWith.adviceWith(context, "direct2Kafka", a -> {
            a.weaveByToUri("kafka:*").replace().to("mock:kafka:producer");
        });
        context.start();
    }

    @Test
    public void testAggregatedSale() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").expectedBodiesReceived("INSERT INTO expense (sale_id, customer_id, amount, date) VALUES (1000, 1001, 678.99, TIMESTAMP '2022-05-09 14:10:09');");
        getMockEndpoint("mock:kafka:producer").expectedBodiesReceived("{\"sale_id\":1000,\"op\":\"c\"}");
        template.sendBody("direct:start", getAggregatedSaleEvent());

        assertMockEndpointsSatisfied();

        Exchange ex = getMockEndpoint("mock:kafka:producer").assertExchangeReceived(0);
        MatcherAssert.assertThat(ex.getIn().getHeaders().size(), Matchers.equalTo(1));
        MatcherAssert.assertThat(ex.getIn().getHeader(KafkaConstants.KEY), Matchers.notNullValue());
        MatcherAssert.assertThat(ex.getIn().getHeader(KafkaConstants.KEY).toString(), Matchers.equalTo("1000"));
    }

    @Test
    public void testAggregatedSaleWithException() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:jdbc:cashback").whenExchangeReceived(1, exchange -> {
            throw new PSQLException("Error", PSQLState.UNKNOWN_STATE);
        });

        getMockEndpoint("mock:jdbc:cashback").expectedMessageCount(2);
        getMockEndpoint("mock:kafka:producer").expectedBodiesReceived("{\"sale_id\":1000,\"op\":\"u\"}");
        template.sendBody("direct:start", getAggregatedSaleEvent());

        assertMockEndpointsSatisfied();
        Exchange ex = getMockEndpoint("mock:jdbc:cashback").assertExchangeReceived(1);
        MatcherAssert.assertThat(ex.getIn().getBody(String.class), Matchers.equalTo("UPDATE expense SET amount = 678.99 WHERE sale_id = 1000;"));
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
