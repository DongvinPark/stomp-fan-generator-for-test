package kr.co.alphacircle;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class MyStompSessionHandler extends StompSessionHandlerAdapter {
  private final int fanId;
  // 수동 파싱을 위한 ObjectMapper (static으로 공유해서 가볍게 사용)
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public MyStompSessionHandler(int fanId) { this.fanId = fanId; }

  @Override
  public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
    System.out.println("!!! [Fan-" + fanId + "] 연결 성공! 구독 시작합니다.");
    session.subscribe("/topic/greetings", this);
  }

  @Override
  public Type getPayloadType(StompHeaders headers) {
    // 이제 무조건 String으로 받습니다.
    return String.class;
  }

  @Override
  public void handleFrame(StompHeaders headers, Object payload) {
    try {
      // payload를 String으로 캐스팅한 뒤 직접 Map으로 파싱
      String rawJson = (String) payload;
      Map<String, Object> content = objectMapper.readValue(rawJson, Map.class);

      String msg = (String) content.get("content");
      String sentTimeStr = (String) content.get("clientSendTime");

      if (sentTimeStr != null) {
        Instant sentTime = Instant.parse(sentTimeStr);
        Instant receivedTime = Instant.now();
        long delay = Duration.between(sentTime, receivedTime).toMillis();

        System.out.println("[Fan-" + fanId + "] 메시지: " + msg + " | 지연시간: " + delay + "ms");
      }
    } catch (Exception e) {
      System.err.println("[Fan-" + fanId + "] 파싱 에러: " + e.getMessage());
    }
  }

  @Override
  public void handleException(StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
    System.err.println("!!! STOMP 에러 발생: " + exception.getMessage());
  }
}// end of Handler class