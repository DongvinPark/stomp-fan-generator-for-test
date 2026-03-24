# STOMP Client Generator for Performance Test

## 테스트 방법
- JDK 21의 Virtual Thread를 활용하여 가상 클라이언트들을 만들어냅니다.
- virtual thread 1 개 == fandom 채팅 서비스의 fan 고객 1 명입니다.
- 특정 동작을 수행한 뒤, 로그를 파일 형태로 남깁니다.