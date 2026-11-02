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

  // 연결 성공 여부 검증용
  @Override
  public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
    System.out.printf(
        "[Fan-%d] STOMP CONNECTED: sessionId=%s, headers=%s%n",
        fanId,
        session.getSessionId(),
        connectedHeaders
    );
    session.subscribe("/topic/greetings", this);
  }

  @Override
  public void handleTransportError(
      StompSession session,
      Throwable exception
  ) {
    System.out.printf(
        "[Fan-%d] STOMP transport error: %s%n",
        fanId,
        exception
    );

    exception.printStackTrace();
  }

  @Override
  public void handleException(
      StompSession session,
      StompCommand command,
      StompHeaders headers,
      byte[] payload,
      Throwable exception
  ) {
    System.out.printf(
        "[Fan-%d] STOMP handler error: command=%s, error=%s%n",
        fanId,
        command,
        exception
    );

    exception.printStackTrace();
  }

  @Override
  public Type getPayloadType(StompHeaders headers) {
    return String.class;
  }

  @Override
  public void handleFrame(StompHeaders headers, Object payload) {
      // 수신 완료 시각부터 먼저 기록한다.
      Long now = System.currentTimeMillis();

    // 1. STOMP 메시지 수신 여부부터 확인 : 출력은 생략한다.
    /*System.out.printf(
        "[Fan-%d] STOMP FRAME RECEIVED: headers=%s, payloadType=%s%n",
        fanId,
        headers,
        payload == null ? "null" : payload.getClass().getName()
    );*/

    try {
      String rawJson = (String) payload;

      // 2. 실제 수신한 메시지 내용 확인 : 출력은 생략한다.
      /*System.out.printf(
          "[Fan-%d] RAW PAYLOAD: %s%n",
          fanId,
          rawJson
      );*/

      // Object Mapper 같은 거 쓰지 않고 직접 찾아낸다.
      long serverSendTime = extractServerSendTime(rawJson);

      // 3. 기존 지연 시간 측정 로직 : 불필요한건 측정하지 않는다.
      if (/*clientSendTimeStr != null && */serverSendTime > 0L) {

          // 미사용 데이터는 계산을 생략한다.
        //Instant artistSendTime = Instant.parse(clientSendTimeStr);

        // 미사용 데이터는 계산 생략.
        /*double totalDelay =
            Duration.between(artistSendTime, now).toNanos() / 1_000_000.0;*/

        double serverToFanDelay = now - serverSendTime;

        Main.delayResults.add(serverToFanDelay);

        // 여기 출력도 생략한다. 이미 여러번 테스트 해보면서 검증된 부분이다.
        /*if (fanId % 100 == 0) {
          System.out.printf(
              "[Fan-%d] 아티스트 → 팬: %.3fms | " +
                  "아티스트 → 서버: %.3fms | 서버 → 팬: %.3fms | " +
                  "수집: %d%n",
              fanId,
              totalDelay,
              artistToServerDelay,
              serverToFanDelay,
              current
          );
        }*/
      } else {
        System.out.printf(
            "[Fan-%d] Missing timestamp: serverSendTime=%s%n",
            fanId,
            serverSendTime
        );
      }
    } catch (Exception e) {
      System.out.printf(
          "[Fan-%d] Error processing frame: %s%n",
          fanId,
          e
      );
      e.printStackTrace();
    }
  }

  private static long extractServerSendTime(String json) {
    String key = "\"serverSendTime\"";

    int keyIndex = json.indexOf(key);

    if (keyIndex < 0) {
      throw new IllegalArgumentException("serverSendTime not found");
    }

    int colonIndex = json.indexOf(':', keyIndex + key.length());

    if (colonIndex < 0) {
      throw new IllegalArgumentException("Invalid JSON");
    }

    int start = colonIndex + 1;

    while (start < json.length()
        && Character.isWhitespace(json.charAt(start))) {
      start++;
    }

    boolean quoted = json.charAt(start) == '"';

    if (quoted) {
      start++;
    }

    int end = start;

    while (end < json.length()
        && Character.isDigit(json.charAt(end))) {
      end++;
    }

    if (start == end) {
      throw new IllegalArgumentException("Invalid serverSendTime");
    }

    return Long.parseLong(json.substring(start, end));
  }

}// end of Handler class