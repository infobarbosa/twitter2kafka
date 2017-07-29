package com.infobarbosa;

import com.infobarbosa.TwitterService;
import com.infobarbosa.EnvironmentSetupException;

public class App {

  public static void main(String[] args) {

    try {

      TwitterService twitterService = TwitterService.getInstance();
      twitterService.run();
    } catch( EnvironmentSetupException e){
      System.out.println(e);
    } catch( InterruptedException e ){
      System.out.println(e);
    }
  }
}
