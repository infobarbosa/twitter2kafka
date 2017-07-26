package com.infobarbosa;

import com.infobarbosa.TwitterService;

public class App {

  public static void main(String[] args) {

    try {
      TwitterService twitterService = TwitterService.getInstance();
      twitterService.run( args[0] );
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
