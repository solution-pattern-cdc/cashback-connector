package org.acme.retail;

import org.apache.camel.builder.RouteBuilder;

public class Routes extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.customer.topic.name}}?groupId={{kafka.customer.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2postgresql")
                .unmarshal().json()
                .log("Received: ${body}")
                .choice()
                .when().jsonpath("$..[?(@.op == 'c')]").log("Create")
                .setBody(simple("INSERT INTO customer (customer_id, name, status) VALUES (${body[after][customer_id]}, '${body[after][name]}', '${body[after][status]}');"))
                .when().jsonpath("$..[?(@.op == 'u')]").log("Update")
                .setBody(simple("UPDATE customer SET name = '${body[after][name]}', status = '${body[after][status]}' WHERE customer_id = ${body[after][customer_id]};"))
                .end()
                .log("db statement: ${body}")
                .to("jdbc:default");
    }
}
