package com.azure.cosmos.examples.crudquickstart.async;

import java.time.Duration;
import java.util.List;
import java.util.prefs.PreferenceChangeEvent;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.paging.ContinuablePagedFlux;
import com.azure.core.util.paging.ContinuablePagedIterable;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Food;
import com.azure.cosmos.implementation.FeedResponseDiagnostics;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.util.CosmosPagedFlux;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SampleFoodAsync {

  private CosmosAsyncClient asyncClient;
  private CosmosClient syncClient;

  private String databaseName = "NutritionDb";
  private String containerName = "food";

  private CosmosAsyncDatabase asyncDatabase;
  private CosmosAsyncContainer asyncContainer;

  private CosmosDatabase syncDatabase;
  private CosmosContainer syncContainer;

  private int pageSize = 10;

  private CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();

  private String query = "SELECT c.id, c.description, c.version, c.foodGroup FROM c WHERE c.foodGroup = 'Beverages'";

  private static Logger logger = LoggerFactory.getLogger(SampleCRUDQuickstartAsync.class);

  public static void main(String[] args) {
    SampleFoodAsync sample = new SampleFoodAsync();

    try {

      sample.initialize();
      // sample.queryWithSync();
      // sample.queryWithAsync();
      sample.queryWithAsyncAndContinuationToken();

      System.out.println("Completed");
      System.exit(0);

    } catch (Exception e) {
      logger.error("Error occurred: ", e);
    }

  }

  private void initialize() throws Exception {

    queryRequestOptions
    .setQueryMetricsEnabled(true);

    asyncClient = new CosmosClientBuilder()
        .endpoint(AccountSettings.HOST)
        .key(AccountSettings.MASTER_KEY)
        .buildAsyncClient();

    asyncDatabase = asyncClient.getDatabase(databaseName);
    asyncContainer = asyncDatabase.getContainer(containerName);

    syncClient = new CosmosClientBuilder()
        .endpoint(AccountSettings.HOST)
        .key(AccountSettings.MASTER_KEY)
        .buildClient();

    syncDatabase = syncClient.getDatabase(databaseName);
    syncContainer = syncDatabase.getContainer(containerName);

  }

  private void queryWithSync() throws Exception {
    logger.info("Query with sync and continuation token");

    int currentPageNumber = 1;
    int documentNumber = 0;
    String continuationToken = null;

    // First iteration (continuationToken = null): Receive a batch of query response
    // pages
    // Subsequent iterations (continuationToken != null): Receive subsequent batch
    // of query response pages, with continuationToken indicating where the previous
    // iteration left off
    do {

      logger.info("Continuation Token: " + continuationToken + "\n");

      Iterable<FeedResponse<Food>> feedResponseIterator = syncContainer
          .queryItems(query, queryRequestOptions, Food.class)
          .iterableByPage(continuationToken, pageSize);

      for (FeedResponse<Food> page : feedResponseIterator) {
        
        logger.info(String.format("Current page number: %d", currentPageNumber));
        
        for (Food foodItem : page.getResults()) {

          documentNumber++;
          System.out.println(foodItem.getId() + " " + foodItem.getDescription() + " " + foodItem.getVersion() + " "
              + foodItem.getFoodGroup());

          // if (documentNumber >= 100) { break; }

        }

        // Page count so far
        logger.info(String.format("Total documents received so far: %d", documentNumber));

        // Along with page results, get a continuation token
        // which enables the client to "pick up where it left off"
        // in accessing query response pages.
        continuationToken = page.getContinuationToken();
        System.out.println("Continuation Token: " + continuationToken + "\n");

        currentPageNumber++;
      }

    } while (continuationToken != null);

  }

  private void queryWithAsync() throws Exception {

    System.out.println("query with async w/o continuation token");

    CosmosPagedFlux<Food> pagedFluxResponse = asyncContainer.queryItems(query, queryRequestOptions, Food.class);

    pagedFluxResponse.byPage(pageSize).flatMap(fluxResponse -> {
      logger.info("Got a page of query result with " +
          fluxResponse.getResults().size() + " items(s)"
          + " and request charge of " + fluxResponse.getRequestCharge());

      logger.info("Item Ids " + fluxResponse
          .getResults()
          .stream()
          .map(Food::getId)
          .collect(Collectors.toList()));

      return Flux.empty();
    }).blockLast();

  }

  private void queryWithAsyncAndContinuationToken() throws Exception {

    System.out.println("query with async and continuation token");

    int documentNumber = 0;
    int loopCounter = 0;
    String continuationToken = null;

    FeedResponse<Food> feedResponse = null;
    CosmosPagedFlux<Food> pagedFluxResponse = asyncContainer.queryItems(query, queryRequestOptions, Food.class);

    // Mono<FeedResponse<Food>> mono = pagedFluxResponse.byPage(pageSize).next();
    // feedResponse = mono.block();
    
    // logger.warn("continuation token: " + feedResponse.getContinuationToken()); 
    // logger.warn("number of results: " + feedResponse.getResults().size()); 
    do {

      System.out.println("loop counter: " + loopCounter);
      loopCounter++;

      if (continuationToken == null) {
        feedResponse = pagedFluxResponse.byPage(pageSize).next().block();
      } else {
        feedResponse = pagedFluxResponse.byPage(continuationToken).next().block();
      }

      continuationToken = feedResponse.getContinuationToken();
      logger.warn("Continuation token: " + continuationToken);

      logger.warn("headers: " + feedResponse.getResponseHeaders().toString());

      List<Food> list = feedResponse.getResults();
      logger.warn("number of items returned: " + list.size());

      documentNumber += feedResponse.getResults().size();
      logger.warn(String.format("Total documents received so far: %d", documentNumber));

    } while (continuationToken != null);

    System.out.println("fell through");
  }
}
