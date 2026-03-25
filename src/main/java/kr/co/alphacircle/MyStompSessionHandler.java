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
  private final int totalFanCount;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public MyStompSessionHandler(int fanId, int totalFanCount) {
    this.fanId = fanId;
    this.totalFanCount = totalFanCount;
  }

  @Override
  public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
    session.subscribe("/topic/greetings", this);
  }

  @Override
  public Type getPayloadType(StompHeaders headers) {
    return String.class;
  }

  @Override
  public void handleFrame(StompHeaders headers, Object payload) {
    try {
      String rawJson = (String) payload;
      Map<String, Object> content = objectMapper.readValue(rawJson, Map.class);

      String clientSendTimeStr = (String) content.get("clientSendTime");
      String serverSendTimeStr = (String) content.get("serverSendTime");
      Instant now = Instant.now();

      if (clientSendTimeStr != null && serverSendTimeStr != null) {
        Instant artistSendTime = Instant.parse(clientSendTimeStr);
        Instant serverSendTime = Instant.parse(serverSendTimeStr);

        // 1. 나노초 단위로 차이를 구한 뒤 1,000,000으로 나눠 소수점 밀리초 생성
        double totalDelay = Duration.between(artistSendTime, now).toNanos() / 1_000_000.0;
        double serverToFanDelay = Duration.between(serverSendTime, now).toNanos() / 1_000_000.0;
        double artistToServerDelay = Duration.between(artistSendTime, serverSendTime).toNanos() / 1_000_000.0;

        // 2. 결과 기록 (Double 타입으로 소수점 유지)
        Main.delayResults.add(serverToFanDelay);
        int current = Main.collectedCount.incrementAndGet();

        // 3. 로그 출력 (%.3f 포맷을 사용해 111.111ms 형태로 출력)
        if (fanId % 100 == 0) {
          System.out.printf(
              "[Fan-%d] 이티스트 -> 팬: %.3fms | 아티스트 -> 서버: %.3fms | 서버 -> 팬: %.3fms %n",
              fanId, totalDelay, artistToServerDelay, serverToFanDelay
          );
        }
      }
    } catch (Exception e) {
      System.err.println("[Fan-" + fanId + "] Error: " + e.getMessage());
    }
  }

}// end of Handler class