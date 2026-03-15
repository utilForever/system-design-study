# Chapter 7 토론 질문 답변

> **Design a Unique ID Generator in Distributed Systems** — 발표 후 토론용

---

## Q1. 시계가 역행하면 어떻게 될까?

### 문제 상황

분산 시스템에서 각 서버의 시계는 NTP(Network Time Protocol)로 동기화되는데, NTP 보정 시 시계가 뒤로 갈 수 있다.

Snowflake ID에서 timestamp는 상위 비트이므로, **시계가 역행하면 이전에 발행한 ID보다 작은 ID가 생성**된다.

```
시간 T=100ms → ID: 100-xxx-xxx-001
NTP 보정 → 시간이 T=98ms로 역행
시간 T=98ms  → ID: 98-xxx-xxx-001  ← 정렬 깨짐!
```

### 해결 방법

| 방법 | 설명 | 장단점 |
|------|------|--------|
| **ID 생성 거부 + last_timestamp 기억** | 이전에 사용한 최대 timestamp를 메모리에 저장하고, 역행 감지 시 에러 반환 | Twitter Snowflake의 실제 구현 방식. 간단하지만 가용성 저하 |
| **타임스탬프 유지 + sequence 증가** | 역행 감지 시 이전 timestamp를 유지하면서 sequence만 증가 | 가용성 유지, 단 sequence 소진 시 대기 필요 |
| **논리적 시계 (Logical Clock)** | Lamport Clock이나 Hybrid Logical Clock(HLC) 사용 | 물리적 시계 의존성 제거, 복잡도 증가 |
| **NTP slew mode** | NTP가 시계를 급격히 점프시키지 않고 점진적으로 조정하도록 설정 | OS 수준 설정, 큰 차이에는 한계 |

### Twitter Snowflake 실제 구현

```python
def next_id(self):
    current_ms = get_current_timestamp()

    if current_ms < self.last_timestamp:
        # 시계 역행 감지 — ID 생성 거부
        raise ClockMovedBackwardsError(
            f"Clock moved backwards by {self.last_timestamp - current_ms}ms"
        )

    if current_ms == self.last_timestamp:
        self.sequence = (self.sequence + 1) & 0xFFF  # 12비트 마스크
        if self.sequence == 0:
            # 같은 밀리초에 4096개 소진 → 다음 ms까지 대기
            current_ms = wait_next_millis(self.last_timestamp)
    else:
        self.sequence = 0

    self.last_timestamp = current_ms
    return compose_id(current_ms, self.datacenter_id, self.machine_id, self.sequence)
```

### 핵심 포인트

- **시계 역행은 분산 시스템에서 반드시 고려해야 할 현실적 문제**
- 대부분의 프로덕션 구현은 역행 감지 시 **에러를 던지거나 대기**하는 방식 채택
- Google의 TrueTime API(Spanner)는 GPS + 원자시계로 시계 불확실성 자체를 최소화

---

## Q2. 41비트 타임스탬프가 부족해지면?

### 수명 계산

```
2^41 - 1 = 2,199,023,255,551 밀리초
         = 2,199,023,255초
         ≈ 69.7년
```

커스텀 epoch를 2010-11-04(Twitter epoch)로 설정한 경우:
- **만료 시점**: 2010 + 69 = **약 2079년**

### 대응 전략

#### 1. 비트 재배분 (Section Length Tuning)

책에서 언급한 방법. 용도에 따라 비트 배분을 조정할 수 있다.

```
현재:  [1 sign] [41 timestamp] [5 dc] [5 machine] [12 sequence]

옵션A: [1 sign] [42 timestamp] [5 dc] [4 machine] [12 sequence]
        → 수명 2배 (약 139년), 머신 16대로 축소

옵션B: [1 sign] [42 timestamp] [4 dc] [4 machine] [14 sequence]
        → 수명 2배, DC 16개 + 머신 16대, 시퀀스 16,384/ms
```

- 동시성이 낮은 시스템 → sequence 비트를 줄이고 timestamp에 할당
- 장기 운영 시스템 → timestamp 비트를 늘림

#### 2. 에포크 리셋 (Epoch Migration)

새로운 커스텀 epoch를 현재 시점으로 재설정하여 수명을 리셋.

```
기존: epoch = 2010-11-04 → 2079년 만료
갱신: epoch = 2025-01-01 → 2094년까지 사용 가능
```

**주의사항:**
- 기존 ID와의 호환성 문제 발생
- 마이그레이션 기간 동안 신/구 ID가 공존해야 함
- ID 버전 필드나 별도 식별자 필요

#### 3. 64비트 → 128비트 확장

근본적으로 비트 수를 늘리는 방법. 하지만:
- 스토리지, 인덱스 크기 2배
- 기존 시스템과의 호환성 깨짐
- 최후의 수단

### 핵심 포인트

- 41비트 ≈ 69년은 대부분의 시스템에 충분하지만, **설계 시점에 만료 시점을 명시적으로 계산해둬야 함**
- 커스텀 epoch를 가능한 한 최근 날짜로 설정하여 수명을 최대화
- 비트 재배분이 가장 실용적인 첫 번째 대응

---

## Q3. 데이터센터/머신 ID는 어떻게 할당?

### 책의 언급

> "Datacenter IDs and machine IDs are chosen at the startup time, generally fixed once the system is up and running."

### 할당 방식

#### 1. 수동 설정 (Configuration)

```yaml
# config.yaml
id_generator:
  datacenter_id: 3
  machine_id: 17
```

- 가장 단순한 방법
- 서버 수가 적을 때 적합
- **단점**: 수동 관리 부담, 실수로 중복 할당 가능

#### 2. ZooKeeper / etcd 기반 자동 할당

```
[ZooKeeper]
/snowflake/
  /dc-01/
    /machine-0001  → worker-a (ephemeral node)
    /machine-0002  → worker-b (ephemeral node)
  /dc-02/
    /machine-0001  → worker-c (ephemeral node)
```

- 서버 시작 시 ZooKeeper에서 고유 ID를 자동 획득
- Ephemeral node 사용 → 서버 다운 시 자동 반납
- Twitter Snowflake의 실제 구현이 이 방식 사용
- **단점**: ZooKeeper 자체가 SPOF가 될 수 있음 (클러스터 구성 필요)

#### 3. 네트워크 정보 기반

```python
datacenter_id = hash(datacenter_name) % 32   # 5비트
machine_id = hash(ip_address + port) % 32     # 5비트
```

- 별도 코디네이션 서비스 불필요
- **단점**: 해시 충돌 가능성

#### 4. 클라우드 메타데이터 활용

```python
# AWS 예시
import requests
az = requests.get("http://169.254.169.254/latest/meta-data/placement/availability-zone").text
instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text

datacenter_id = hash(az) % 32
machine_id = hash(instance_id) % 32
```

### ID 변경의 위험성

책에서 경고하는 핵심 사항:

> "Any changes in datacenter IDs and machine IDs require careful review since an accidental change in those values can lead to ID conflicts."

```
서버 A: dc=1, machine=5  → ID: ts-00001-00101-seq
서버 B: dc=1, machine=5  → ID: ts-00001-00101-seq  ← 충돌!
```

같은 밀리초에 같은 dc+machine 조합이 두 대 존재하면 **동일한 ID가 생성**될 수 있다.

### 핵심 포인트

- 프로덕션에서는 **ZooKeeper/etcd 기반 자동 할당**이 가장 안전
- DC/Machine ID는 **시작 시 고정, 런타임 중 변경 금지**
- 스케일링, 장애 복구 시 ID 재할당 절차를 사전에 정의해야 함

---

## Q4. ID의 보안은 어떻게 보장할 수 있을까?

### Snowflake ID의 보안 취약점

Snowflake ID는 **보안이 아닌 성능과 정렬**을 위해 설계되었다. 구조를 알면 다음 정보를 추출할 수 있다:

```python
id = 1586451091225 * (2**22) + ...

# ID에서 추출 가능한 정보:
timestamp = (id >> 22) + TWITTER_EPOCH    # → 생성 시각
datacenter = (id >> 17) & 0x1F           # → 데이터센터 번호
machine = (id >> 12) & 0x1F             # → 머신 번호
sequence = id & 0xFFF                   # → 시퀀스 번호
```

**노출되는 정보:**
- 정확한 생성 시각 → 사용자 활동 패턴 추론 가능
- 인프라 구조 (DC 수, 머신 수) 노출
- 시퀀스 번호로 트래픽 볼륨 추정 가능
- ID가 순차적이므로 **열거 공격(enumeration attack)** 가능

### 보안 강화 방법

#### 1. 내부/외부 ID 분리

```
내부 ID (DB): Snowflake ID (정렬, 인덱싱 최적화)
외부 ID (API): 암호화된 ID 또는 UUID
```

```python
# 외부 노출 시 암호화
from cryptography.fernet import Fernet

def to_external_id(snowflake_id: int) -> str:
    """내부 Snowflake ID → 외부용 암호화 ID"""
    return fernet.encrypt(str(snowflake_id).encode()).decode()

def to_internal_id(external_id: str) -> int:
    """외부용 암호화 ID → 내부 Snowflake ID"""
    return int(fernet.decrypt(external_id.encode()).decode())
```

#### 2. ID 난독화 (Obfuscation)

```python
# Hashids 라이브러리 활용
from hashids import Hashids
hashids = Hashids(salt="my-secret-salt", min_length=10)

external = hashids.encode(snowflake_id)   # → "kRNrz3vA0J"
internal = hashids.decode(external)[0]     # → 원래 Snowflake ID
```

- 가역적(복호화 가능)이면서 salt 없이는 역추론이 어려움 (암호학적 보안은 아님)
- URL-safe 문자열로 변환

#### 3. Rate Limiting + 접근 제어

```
GET /api/users/1234  → 200 OK
GET /api/users/1235  → 200 OK
GET /api/users/1236  → 429 Too Many Requests (열거 공격 차단)
```

- 순차 접근 패턴 감지 시 차단
- 인증된 사용자만 접근 가능하도록 제한

#### 4. 비트 셔플링 (Bit Shuffling)

```python
def shuffle_id(snowflake_id: int) -> int:
    """비트 위치를 비밀 순서로 재배치"""
    bits = format(snowflake_id, '064b')
    shuffled = ''.join(bits[i] for i in SECRET_PERMUTATION)
    return int(shuffled, 2)
```

- 비트 순서를 비밀 순열로 섞어 구조 해독 방지
- 성능 오버헤드 최소

### 접근법 비교

| 방법 | 정렬 유지 | 보안 수준 | 성능 영향 | 복잡도 |
|------|----------|----------|----------|--------|
| 내부/외부 분리 | 내부만 | 높음 | 중간 | 중간 |
| Hashids 난독화 | ❌ | 중간 | 낮음 | 낮음 |
| Rate Limiting | ✅ | 낮음 | 낮음 | 낮음 |
| 비트 셔플링 | ❌ | 중간 | 낮음 | 중간 |

### 핵심 포인트

- Snowflake ID는 **내부 시스템용**으로 설계됨 — 외부 노출 시 보안 고려 필수
- 가장 실용적인 방법은 **내부/외부 ID 분리** (DB에서는 Snowflake, API에서는 난독화)
- 보안과 정렬/성능은 트레이드오프 — 시스템 요구사항에 맞게 선택
