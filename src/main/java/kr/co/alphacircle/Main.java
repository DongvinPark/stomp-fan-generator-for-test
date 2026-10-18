package kr.co.alphacircle;

import jakarta.websocket.ClientEndpointConfig;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
  private static String targetUrl = "ws://localhost:8080/gs-guide-websocket";
  private static int fanCount = 1000;

  // 1. 대규모 쓰기에 최적화된 비차단(Non-blocking) 큐
  public static final ConcurrentLinkedQueue<Double> delayResults = new ConcurrentLinkedQueue<>();

  // 수집된 개수를 정확히 파악하기 위한 원자적 카운터
  public static final AtomicInteger collectedCount = new AtomicInteger(0);

  public static void main(String[] args) throws InterruptedException {
    if(args.length == 1){
      fanCount = Integer.parseInt(args[0]);
    } else if (args.length >= 2) {
      fanCount = Integer.parseInt(args[0]);
      targetUrl = args[1];
    }

    System.out.println(">>> 테스트 시작: " + fanCount + " fans / " + targetUrl);

    for (int i = 0; i < fanCount; i++) {
      int fanId = i;
      Thread.ofVirtual().start(() -> {
        try {
          StandardWebSocketClient client = new StandardWebSocketClient();
          WebSocketStompClient stompClient = new WebSocketStompClient(client);
          stompClient.setMessageConverter(new StringMessageConverter());

          // 핸들러에 총 팬 수를 넘겨 수집 상태를 확인하게 함
          stompClient.connectAsync(targetUrl, new MyStompSessionHandler(fanId, fanCount));

          Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
          System.err.println("Fan " + fanId + " 연결 에러: " + e.getMessage());
        }
      });

      // Thundering Herd 방지용 간격
      if (i > 0 && i % 100 == 0) Thread.sleep(50);
    }

    // 결과 저장 감시 스레드
    Thread.ofPlatform().start(() -> {
      while (true) {
        try {
          Thread.sleep(2000); // 2초마다 체크
          int currentCount = collectedCount.get();
          if (currentCount > 0 && currentCount >= fanCount) {
            saveResults();
            break;
          }
        } catch (InterruptedException e) {
          break;
        }
      }
    });

    System.out.println(fanCount + "명의 팬이 접속 완료 및 대기 중...");
    Thread.currentThread().join();
  }

  private static synchronized void saveResults() {
    System.out.println(">>> [알림] 모든 데이터 수집 완료! result.log 파일 생성 중...");

    // LocalDateTime.now() 를 그냥 쓰면 윈도우 환경에서는 ':' 문자 때문에 파일 저장에 실패한다.
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));

    String windowOsDir = "C:\\dev\\stomp-test-result" + File.separator + "result_" + timestamp + "_cnt_" + fanCount +".log";
    String unixDir = "result_" + timestamp + "_cnt_" + fanCount +".log";
    try (PrintWriter out = new PrintWriter(
        new FileWriter(
            // TODO : 테스트 환경에 맞춰서 둘 중 하나를 주석해제 하면 된다.
            windowOsDir
            //unixDir
        )
    )
    ) {
      out.println("Index,Server To Fan Delay(ms)");

      double sum = 0;
      double max = 0;
      int index = 0;

      // Queue에서 하나씩 꺼내며 계산 및 기록
      for (Double delay : delayResults) {
        out.println(index++ + "," + delay);
        sum += delay;
        if (delay > max) max = delay;
      }

      double avg = (double) sum / index;

      out.println("-------------------------");
      out.println("Total Samples: " + index);
      out.println("Average Delay: " + String.format("%.2f", avg) + "ms");
      out.println("Max Delay: " + max + "ms");

      System.out.println(">>> [성공] 저장이 완료되었습니다. (평균 지연: " + String.format("%.2f", avg) + "ms)");
      System.out.println(">>> 이제 프로그램을 종료해도 좋습니다.");
    } catch (IOException e) {
      System.err.println("파일 저장 중 오류 발생: " + e.getMessage());
    }
  }
}