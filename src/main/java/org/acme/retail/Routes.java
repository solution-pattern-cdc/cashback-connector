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
public class Routes extends RouteBuilder {

    @SuppressWarnings({"unchecked"})
    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.customer.topic.name}}?groupId={{kafka.customer.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlCustomer")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Customer event received: ${body}")
                .choice()
                .when().jsonpath("$..[?(@.op == 'c')]").log("Create")
                .setBody(simple("INSERT INTO customer (customer_id, name, status) VALUES (${body[after][customer_id]}, '${body[after][name]}', '${body[after][status]}');"))
                .when().jsonpath("$..[?(@.op == 'u')]").log("Update")
                .setBody(simple("UPDATE customer SET name = '${body[after][name]}', status = '${body[after][status]}' WHERE customer_id = ${body[after][customer_id]};"))
                .end()
                .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                .to("jdbc:cashback");

        from("kafka:{{kafka.sale.topic.name}}?groupId={{kafka.sale.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlSale")
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
