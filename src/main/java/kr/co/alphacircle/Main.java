package kr.co.alphacircle;

import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;


public class Main {
  private static final String URL = "ws://localhost:8080/gs-guide-websocket";
  private static final int FAN_COUNT = 10;

  public static void main(String[] args) throws InterruptedException {
    for (int i = 0; i < FAN_COUNT; i++) {
      int fanId = i;
      Thread.ofVirtual().start(() -> {
        try {
          WebSocketClient client = new StandardWebSocketClient();
          WebSocketStompClient stompClient = new WebSocketStompClient(client);

          // 핵심: 복잡한 Jackson 컨버터 대신 단순 String 컨버터 사용
          stompClient.setMessageConverter(new StringMessageConverter());

          StompSessionHandler sessionHandler = new MyStompSessionHandler(fanId);
          stompClient.connectAsync(URL, sessionHandler);

          Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
          System.err.println("Fan " + fanId + " 에러: " + e.getMessage());
        }
      });
      if (i % 100 == 0) Thread.sleep(50);
    }
    System.out.println(FAN_COUNT + "명의 팬이 접속을 시도 중입니다...");
    Thread.currentThread().join();
  }
}