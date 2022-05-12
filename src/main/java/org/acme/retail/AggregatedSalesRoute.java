package org.acme.retail;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;

import io.vertx.core.json.JsonObject;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.postgresql.util.PSQLException;

@ApplicationScoped
public class AggregatedSalesRoute extends RouteBuilder {

    @SuppressWarnings({"unchecked"})
    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.sale_aggregated.topic.name}}?groupId={{kafka.sale_aggregated.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlAggregatedSale")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Sale event received: ${body}")
                .process(exchange -> {
                    Message in = exchange.getIn();
                    in.setHeader("sale_id", in.getBody(Map.class).get("sale_id"));
                    in.setHeader("total", in.getBody(Map.class).get("total"));
                })
                .doTry()
                    .process(exchange -> {
                        Message in = exchange.getIn();
                        JsonObject json = new JsonObject(in.getBody(Map.class));
                        in.setBody(new StringBuilder()
                                .append("INSERT INTO expense (sale_id, customer_id, amount, date) VALUES (")
                                .append(json.getLong("sale_id")).append(", ")
                                .append(json.getLong("customer_id")).append(", ")
                                .append(json.getString("total")).append(", ")
                                .append("TIMESTAMP '").append(transformEpochToDateTime(json.getLong("date") / 1000))
                                .append("'").append(");"));
                        in.setHeader("op", "c");
                    })
                    .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                    .to("jdbc:cashback")
                    .to("direct:kafka")
                .doCatch(PSQLException.class)
                    .process(exchange -> {
                        Message in = exchange.getIn();
                        in.setBody(new StringBuilder()
                                .append("UPDATE expense SET amount = ")
                                .append(in.getHeader("total"))
                                .append(" WHERE sale_id = ")
                                .append(in.getHeader("sale_id")).append(";"));
                        in.setHeader("op", "u");
                    })
                    .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                    .to("jdbc:cashback")
                    .to("direct:kafka")
                .end();

        from("direct:kafka")
                .routeId("direct2Kafka")
                .process(exchange -> {
                    Message in = exchange.getIn();
                    Long saleId = in.getHeader("sale_id", Long.class);
                    String op = in.getHeader("op", String.class);
                    JsonObject json = new JsonObject().put("sale_id", saleId).put("op", op);
                    in.setBody(json.toString());
                    in.setHeader(KafkaConstants.KEY, saleId.toString());
                    in.removeHeader("sale_id");
                    in.removeHeader("op");
                    in.removeHeader("total");
                })
                .to("kafka:{{kafka.expense_event.topic.name}}");
    }

    private String transformEpochToDateTime(long epoch) {
        LocalDateTime ldt = Instant.ofEpochMilli(epoch).atZone(ZoneId.of("UTC")).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return ldt.format(formatter);
    }
}
