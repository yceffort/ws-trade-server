# bench/linux.sh가 쓰는 측정용 이미지. 맥과 같은 버전의 컴파일러에 run.sh가 쓰는 도구만 더한다.
FROM rust:1.95-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends jq procps util-linux && rm -rf /var/lib/apt/lists/*
# 저장소를 /work에 물리면 소유자가 달라 git이 거부하므로 허용해 둔다(결과에 커밋 해시를 남기기 위해서다).
RUN git config --system --add safe.directory /work
