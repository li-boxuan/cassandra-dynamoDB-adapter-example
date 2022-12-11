package io.stargate.sgv2.example;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a sample application that allows developers to quickly test DynamoDB features. Remember
 * to put your AWS access id and key as your environmental valuables.
 */
public class DynamoSampleApp {
  private static String tableName = "crud_sample_table";
  private static final Logger LOGGER = LogManager.getLogger(DynamoSampleApp.class);

  private static void queryItem(DynamoDB dynamoDB) {
    LOGGER.info("query item");
    Table table = dynamoDB.getTable(tableName);
    QuerySpec querySpec =
        new QuerySpec()
            .withKeyConditionExpression("Id = :id AND sid >= :sid")
            .withValueMap(new ValueMap().withNumber(":id", 120).withString(":sid", "sid000"));
    Iterator<Item> iterator = table.query(querySpec).iterator();
    while (iterator.hasNext()) {
      LOGGER.info("Item result: {}", iterator.next());
    }
  }

  private static void createTable(DynamoDB dynamoDB) {
    LOGGER.info("Create table {}", tableName);
    CreateTableRequest req =
        new CreateTableRequest()
            .withTableName(tableName)
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(1L)
                    .withWriteCapacityUnits(1L))
            .withKeySchema(
                new KeySchemaElement("Id", KeyType.HASH),
                new KeySchemaElement("sid", KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition("Id", ScalarAttributeType.N),
                new AttributeDefinition("sid", ScalarAttributeType.S));
    dynamoDB.createTable(req);
  }

  private static void deleteTable(AmazonDynamoDB client) {
    LOGGER.info("Delete table {}", tableName);
    client.deleteTable(tableName);
  }

  private static void putSimpleItem(DynamoDB dynamoDB) {
    LOGGER.info("Put an item to table {}", tableName);
    Table table = dynamoDB.getTable(tableName);
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    LocalDateTime now = LocalDateTime.now();
    try {

      Item item =
          new Item()
              .withPrimaryKey("Id", 120, "sid", "sid001")
              .withString("Title", "Book 120 Title")
              .withString("ISBN", "120-1111111111")
              .withString("UpdateTime", dtf.format(now))
              .withStringSet("Authors", new HashSet<>(Arrays.asList("Author12", "Author22")))
              .withNumber("Price", 20)
              .withString("Dimensions", "8.5x11.0x.75")
              .withNumber("PageCount", 500)
              .withBoolean("InPublication", false)
              .withString("ProductCategory", "Book");
      table.putItem(item);

      item =
          new Item()
              .withPrimaryKey("Id", 121, "sid", "sid002")
              .withString("Title", "Book 121 Title")
              .withString("ISBN", "121-1111111111")
              .withStringSet("Authors", new HashSet<>(Arrays.asList("Author21", "Author 22")))
              .withString("UpdateTime", dtf.format(now))
              .withNumber("Price", 20)
              .withString("Dimensions", "8.5x11.0x.75")
              .withNumber("PageCount", 500)
              .withBoolean("InPublication", true)
              .withString("ProductCategory", "Book");
      table.putItem(item);
    } catch (Exception e) {
      System.err.println("Create items failed.");
      System.err.println(e.getMessage());
    }
  }

  public static AmazonDynamoDB getClientWithCassandra(final String token) {
    Properties props = System.getProperties();
    props.setProperty("aws.accessKeyId", token);
    props.setProperty("aws.secretKey", "any-string");
    AwsClientBuilder.EndpointConfiguration endpointConfiguration =
        new AwsClientBuilder.EndpointConfiguration("http://localhost:8082/v2", "any-string");
    return AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .build();
  }

  public static AmazonDynamoDB getClientWithDynamoDB() {
    return AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
  }

  public static void main(String[] args) throws InterruptedException {
    LOGGER.info("start, initialize DynamoDB client");
    AmazonDynamoDB client;
    if (args.length > 0) {
      // use Stargate - Cassandra proxy
      // first argument must be the address of the proxy
      LOGGER.info("Connecting to Stargate...");
      client = AmazonDynamoDBClientBuilder.standard()
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(args[0], "dummy"))
          .build();
    } else {
      // use AWS - DynamoDB
      LOGGER.info("Connecting to Amazon DynamoDB...");
      client = AmazonDynamoDBClientBuilder.standard().build();
    }

    DynamoDB dynamoDB = new DynamoDB(client);
    ListTablesResult tables = client.listTables();
    if (tables.getTableNames() != null && tables.getTableNames().contains(tableName)) {
      LOGGER.info("Table already exists, skip table creation");
    } else {
      createTable(dynamoDB);
      LOGGER.info("Sleep for 10 seconds to wait for on-demand table to be created");
      Thread.sleep(10000);
    }
    LOGGER.info("tables are {}", client.listTables());
    putSimpleItem(dynamoDB);
    queryItem(dynamoDB);
  }
}
