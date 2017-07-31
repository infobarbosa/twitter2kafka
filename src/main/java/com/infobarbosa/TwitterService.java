package com.infobarbosa;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.util.List;
import java.util.ArrayList;

public class TwitterService {
  private static TwitterService twitterService;
  private static final String TWITTER_CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");
  private static final String TWITTER_CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");
  private static final String TWITTER_ACCESS_TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
  private static final String TWITTER_ACCESS_TOKEN_SECRET = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");
  private static final String TWITTER_HASHTAG = System.getenv("TWITTER_HASHTAG");

  private static KafkaService kafkaService;

  /**Construtor padrao. Valida a presenca de variaveis de ambiente essenciais.*/
  private TwitterService() throws EnvironmentSetupException{

    if( TWITTER_CONSUMER_KEY == null ){
        throw new EnvironmentSetupException("Variável de ambiente não setada: TWITTER_CONSUMER_KEY");
    }
    
    if( TWITTER_CONSUMER_SECRET == null ){
        throw new EnvironmentSetupException("Variável de ambiente não setada: TWITTER_CONSUMER_SECRET");
    }
    
    if( TWITTER_ACCESS_TOKEN == null ){
        throw new EnvironmentSetupException("Variável de ambiente não setada: TWITTER_ACCESS_TOKEN");
    }
    
    if( TWITTER_ACCESS_TOKEN_SECRET == null ){
        throw new EnvironmentSetupException("Variável de ambiente não setada: TWITTER_ACCESS_TOKEN_SECRET");
    }

    if( TWITTER_HASHTAG == null ){
        throw new EnvironmentSetupException("Variável de ambiente não setada: TWITTER_HASHTAG");
    }

    kafkaService = KafkaService.getInstance();
  }

  /**
  * Obtém uma instância de TwitterService  
  */
  public static TwitterService getInstance() throws EnvironmentSetupException{
    if( twitterService == null  ){
       twitterService = new TwitterService();
    }

    return twitterService;
  }

  /**
  *  Onde o trabalho realmente eh feito
  */
  public void run() throws InterruptedException {
    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    // add some track terms
    List terms = new ArrayList();
    terms.add( TWITTER_HASHTAG );
    endpoint.trackTerms( terms );

    Authentication auth = new OAuth1(TWITTER_CONSUMER_KEY
                                    ,TWITTER_CONSUMER_SECRET
                                    ,TWITTER_ACCESS_TOKEN
                                    ,TWITTER_ACCESS_TOKEN_SECRET);

    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("TwitterClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();

    Gson gson = new Gson();

    // Do whatever needs to be done with messages
    while( true ) {
      if (client.isDone()) {
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }

      String msg = queue.poll(5, TimeUnit.SECONDS);
      if (msg == null) {
        System.out.println("Did not receive a message in 5 seconds");
      } else {
        JsonObject o = gson.fromJson(msg, JsonElement.class).getAsJsonObject();

        String tweetId = o.get("id").getAsString();
        System.out.println("Mensagem de id: " + tweetId);

        kafkaService.enqueue( tweetId, msg );

        System.out.println( msg );
      }
    }

    client.stop();
    kafkaService.close();

    // Print some stats
    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
  }
}
