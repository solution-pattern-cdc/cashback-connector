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

@ApplicationScoped
public class AggregatedSalesRoute extends RouteBuilder {

    @SuppressWarnings({"unchecked"})
    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.sale.topic.name}}?groupId={{kafka.sale.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlAggregatedSale")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Sale event received: ${body}")
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
                })
                .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                .to("jdbc:cashback");
    }

    private String transformEpochToDateTime(long epoch) {
        LocalDateTime ldt = Instant.ofEpochMilli(epoch).atZone(ZoneId.of("UTC")).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return ldt.format(formatter);
    }
}
