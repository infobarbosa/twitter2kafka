package com.infobarbosa;

import com.infobarbosa.TwitterService;

public class App {

  public static void main(String[] args) {

  	if( args.length == 0 ){
  		throw new RuntimeException("Hashtag nao informada.");
  	}else{
  		System.out.println("Buscando a hashtag " + args[0]);
  	}

    try {
      TwitterService twitterService = TwitterService.getInstance();
      twitterService.run( args[0] );
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
