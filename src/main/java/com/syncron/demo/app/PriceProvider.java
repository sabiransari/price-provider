package com.syncron.demo.app;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.entities.Subsegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PriceProvider implements RequestHandler<Order, Order> {
    private static final Logger logger = LoggerFactory.getLogger(PriceProvider.class);
    private static final List<Integer> DISCOUNTS = List.of(5, 10, 15, 20, 25);
    private static final Random RANDOM = new Random();

    @Override
    public Order handleRequest(Order order, Context context) {
        MetricsLogger metricsLogger = new MetricsLogger();

        String productName = order.getProductName();
        String tenantId = order.getTenantId().toString();

        DynamoDbClient ddb = DynamoDbClient.builder().region(Region.EU_WEST_1).build();
        HashMap<String, AttributeValue> p = new HashMap<>();
        p.put("name", AttributeValue.builder().s(productName).build());

        GetItemRequest request = GetItemRequest.builder()
                .key(p)
                .tableName("obs-demo-product")
                .build();

        Map<String, AttributeValue> product;
        try {
            product = ddb.getItem(request).item();
        } catch (Exception e) {
            logger.error("Error while fetching product", e);
            return null;
        }

        AttributeValue price = product.get("price");
        double originalPrice = Double.parseDouble(price.s());

        double discountedPrice;
        Subsegment subsegment = AWSXRay.beginSubsegment("DiscountCalculation");
        try(subsegment) {
            logger.info("generating random discount");
            int index = RANDOM.nextInt(4);
            int discountPercentage = DISCOUNTS.get(index);
            discountedPrice = originalPrice - (originalPrice * discountPercentage / 100);
            Thread.sleep(1000);
            metricsLogger.setNamespace("ObservabilityDiscountTest");
            metricsLogger.putDimensions(DimensionSet.of("Product", productName, "Tenant", tenantId));
            metricsLogger.putMetric("Discount", discountPercentage, Unit.PERCENT);
            metricsLogger.flush();
        } catch (InterruptedException e) {
            subsegment.addException(e);
            throw new RuntimeException(e);
        } finally {
            subsegment.end();
        }
        order.setOriginalAmount(originalPrice);
        order.setDiscountedAmount(discountedPrice);

        return order;
    }
}
