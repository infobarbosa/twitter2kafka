package com.infobarbosa;

/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

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

  private TwitterService(){}

  public static TwitterService getInstance(){
    if( twitterService == null  ){
       twitterService = new TwitterService();
    }

    return twitterService;
  }

  public void run( String searchTerm ) throws InterruptedException {
    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    // add some track terms
    List terms = new ArrayList();
    terms.add( searchTerm );
    endpoint.trackTerms( terms );

    Authentication auth = new OAuth1( System.getenv("TWITTER_CONSUMER_KEY")
                                     ,System.getenv("TWITTER_CONSUMER_SECRET")
                                     ,System.getenv("TWITTER_ACCESS_TOKEN")
                                     ,System.getenv("TWITTER_ACCESS_TOKEN_SECRET"));

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

    KafkaService kafkaService = KafkaService.getInstance();

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
        kafkaService.enqueue( msg );
        System.out.println(msg);
      }
    }

    client.stop();
    kafkaService.close();

    // Print some stats
    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
  }
}
