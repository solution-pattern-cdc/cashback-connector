package org.acme.retail;

import javax.enterprise.context.ApplicationScoped;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class CustomerRoute extends RouteBuilder {

    @SuppressWarnings({"unchecked"})
    @Override
    public void configure() throws Exception {
        from("kafka:{{kafka.customer.topic.name}}?groupId={{kafka.customer.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2PostgresqlCustomer")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Customer event received: ${body}")
                .filter().jsonpath("$..[?(@.op =~ /d|r|t|m/)]")
                    .log(LoggingLevel.DEBUG, "Filtering out change event which is not 'create' or 'update'")
                    .stop()
                .end()
                .choice()
                .when().jsonpath("$..[?(@.op == 'c')]").log("Create")
                .setBody(simple("INSERT INTO customer (customer_id, name, status) VALUES (${body[after][customer_id]}, '${body[after][name]}', '${body[after][status]}');"))
                .when().jsonpath("$..[?(@.op == 'u')]").log("Update")
                .setBody(simple("UPDATE customer SET name = '${body[after][name]}', status = '${body[after][status]}' WHERE customer_id = ${body[after][customer_id]};"))
                .end()
                .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
                .to("jdbc:cashback");
    }
}
