#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
 && apt-get update -qq \
 && apt-get install -qq --yes --no-install-recommends tzdata \
 && dpkg-reconfigure -f noninteractive tzdata \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update -qq \
 && apt-get install -qq --yes --no-install-recommends locales \
 && locale-gen en_US.UTF-8
ENV LANG="en_US.UTF-8" LANGUAGE="en_US.UTF-8" LC_ALL="en_US.UTF-8"

RUN apt-get update -qq && apt-get install -qq --yes --no-install-recommends \
    ca-certificates \
    curl \
    git \
    gnupg \
    lsof \
    maven \
    openjdk-17-jdk \
    python3-dev \
    python3-pip \
    python-is-python3 \
    libkrb5-dev \
    software-properties-common \
    telnet \
    vim \
    wget \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN update-alternatives --set java  ${JAVA_HOME}/bin/java  && \
    update-alternatives --set javac ${JAVA_HOME}/bin/javac

RUN DEBIAN_FRONTEND=noninteractive apt-get update -qq && apt-get install -qq --yes --no-install-recommends \
      ca-certificates gnupg curl \
 && mkdir -p /etc/apt/keyrings \
 && curl -fsSL https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc \
    | gpg --dearmor -o /etc/apt/keyrings/cran.gpg \
 && echo "deb [signed-by=/etc/apt/keyrings/cran.gpg] https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/" \
    > /etc/apt/sources.list.d/cran-r.list \
 && apt-get update -qq \
 && apt-get install -qq --yes --no-install-recommends r-base \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update -qq && apt-get install -qq --yes --no-install-recommends \
    build-essential \
    gcc \
    pkg-config \
    libkrb5-dev \
    python3-dev \
    python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install -U pip \
 && python3 -m pip install \
      cloudpickle \
      codecov \
      flake8 \
      flaky \
      pytest \
      pytest-runner \
      requests>=2.10.0 \
      responses>=0.5.1 \
      requests-kerberos

WORKDIR /workspace

