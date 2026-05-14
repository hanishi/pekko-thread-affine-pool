# syntax=docker/dockerfile:1.7

# ── build stage ───────────────────────────────────────────────────
# Use a vanilla Temurin JDK and install sbt from the official tgz —
# Docker Hub's sbtscala tag namespace churns and pinning to a tag
# that disappears breaks the build. The tgz URL has been stable
# across sbt 1.x releases.
FROM eclipse-temurin:21-jdk AS build
ARG SBT_VERSION=1.11.4
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
    | tar xz -C /usr/local
ENV PATH=/usr/local/sbt/bin:$PATH

WORKDIR /app

# Resolve deps first so they cache between source changes.
COPY project ./project
COPY build.sbt ./
RUN sbt update

COPY src ./src
RUN sbt stage

# ── runtime stage ─────────────────────────────────────────────────
# Microsoft's official Playwright-for-Java image — Ubuntu 22.04
# (jammy) with Chromium + every OS lib Chromium needs already
# baked in. Tag pinned to the Playwright Java version in build.sbt.
#
# Tradeoff vs. GraalVM Community: GraalJS runs in AST-interpreter
# mode here (no Truffle JIT), so the JS pool is slower than on a
# GraalVM runtime. The `-Dpolyglot.engine.WarnInterpreterOnly=false`
# flag silences the warning Polyglot emits on first eval.
FROM mcr.microsoft.com/playwright/java:v1.54.0-jammy
WORKDIR /app

COPY --from=build /app/target/universal/stage ./

ENV JAVA_OPTS="-Dpolyglot.engine.WarnInterpreterOnly=false"

EXPOSE 8090
ENTRYPOINT ["./bin/pekko-thread-affine-pool"]
