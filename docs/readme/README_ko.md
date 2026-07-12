<p align="center">
    <a href="https://openim.io">
        <img src="../../assets/logo-gif/openim-logo.gif" width="60%" height="30%"/>
    </a>
</p>

<div align="center">

[![Stars](https://img.shields.io/github/stars/openimsdk/open-im-server?style=for-the-badge&logo=github&colorB=ff69b4)](https://github.com/openimsdk/open-im-server/stargazers)
[![Forks](https://img.shields.io/github/forks/openimsdk/open-im-server?style=for-the-badge&logo=github&colorB=blue)](https://github.com/openimsdk/open-im-server/network/members)
[![Codecov](https://img.shields.io/codecov/c/github/openimsdk/open-im-server?style=for-the-badge&logo=codecov&colorB=orange)](https://app.codecov.io/gh/openimsdk/open-im-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/openimsdk/open-im-server?style=for-the-badge)](https://goreportcard.com/report/github.com/openimsdk/open-im-server)
[![Go Reference](https://img.shields.io/badge/Go%20Reference-blue.svg?style=for-the-badge&logo=go&logoColor=white)](https://pkg.go.dev/BaoIM-Server)
[![License](https://img.shields.io/badge/license-Apache--2.0-green?style=for-the-badge)](https://github.com/openimsdk/open-im-server/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Slack-500%2B-blueviolet?style=for-the-badge&logo=slack&logoColor=white)](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q)
[![Best Practices](https://img.shields.io/badge/Best%20Practices-purple?style=for-the-badge)](https://www.bestpractices.dev/projects/8045)
[![Good First Issues](https://img.shields.io/github/issues/openimsdk/open-im-server/good%20first%20issue?style=for-the-badge&logo=github)](https://github.com/openimsdk/open-im-server/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22good+first+issue%22)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg?style=for-the-badge&logo=go&logoColor=white)](https://golang.org/)


<p align="center">
  <a href="../../README.md">Englist</a> · 
  <a href="../../README_zh_CN.md">中文</a> · 
  <a href="./README_uk.md">Українська</a> · 
  <a href="./README_cs.md">Česky</a> · 
  <a href="./README_hu.md">Magyar</a> · 
  <a href="./README_es.md">Español</a> · 
  <a href="./README_fa.md">فارسی</a> · 
  <a href="./README_fr.md">Français</a> · 
  <a href="./README_de.md">Deutsch</a> · 
  <a href="./README_pl.md">Polski</a> · 
  <a href="./README_id.md">Indonesian</a> · 
  <a href="./README_fi.md">Suomi</a> · 
  <a href="./README_ml.md">മലയാളം</a> · 
  <a href="./README_ja.md">日本語</a> · 
  <a href="./README_nl.md">Nederlands</a> · 
  <a href="./README_it.md">Italiano</a> · 
  <a href="./README_ru.md">Русский</a> · 
  <a href="./README_pt_BR.md">Português (Brasil)</a> · 
  <a href="./README_eo.md">Esperanto</a> · 
  <a href="./README_ko.md">한국어</a> · 
  <a href="./README_ar.md">العربي</a> · 
  <a href="./README_vi.md">Tiếng Việt</a> · 
  <a href="./README_da.md">Dansk</a> · 
  <a href="./README_el.md">Ελληνικά</a> · 
  <a href="./README_tr.md">Türkçe</a>
</p>


</div>

</p>


## Ⓜ️ OpenIM에 대하여

OpenIM은 채팅, 오디오-비디오 통화, 알림 및 AI 챗봇을 애플리케이션에 통합하기 위해 특별히 설계된 서비스 플랫폼입니다. 이 플랫폼은 강력한 API와 웹훅을 제공하여 개발자가 이러한 상호작용 기능을 애플리케이션에 쉽게 통합할 수 있게 합니다. OpenIM은 독립 실행형 채팅 애플리케이션이 아니라, 다른 애플리케이션들이 풍부한 커뮤니케이션 기능을 달성할 수 있도록 지원하는 플랫폼으로서의 역할을 합니다. 다음 다이어그램은 AppServer, AppClient, OpenIMServer, 및 OpenIMSDK 간의 상호작용을 자세히 설명하기 위해 제시되었습니다.



![App-OpenIM Relationship](../images/oepnim-design.png)

## 🚀 OpenIMSDK에 대하여

 **OpenIMSDK**는**OpenIMServer**를 위해 특별히 제작된 IM SDK로, 클라이언트 애플리케이션 내에 내장하기 위해 설계되었습니다. 그 주요 기능 및 모듈은 다음과 같습니다:


+ 🌟 주요 기능:

  - 📦 로컬 스토리지
  - 🔔 리스너 콜백
  - 🛡️ API 래핑
  - 🌐 연결 관리

  ## 📚 주요 모듈:

  1. 🚀 초기화 및 로그인
  2. 👤 사용자 관리
  3. 👫 친구 관리
  4. 🤖 그룹 기능
  5. 💬 대화 처리

이는 Golang을 사용하여 구축되었으며, 모든 플랫폼에서 일관된 접근 경험을 보장하는 크로스 플랫폼 배포를 지원합니다.

👉 **[GO SDK 탐색하기](https://github.com/openimsdk/openim-sdk-core)**

## 🌐 OpenIMServer에 대하여

+ **OpenIMServer** 는 다음과 같은 특성을 가지고 있습니다:
  - 🌐 마이크로서비스 아키텍처: 게이트웨이 및 다수의 rpc 서비스를 포함하는 클러스터 모드를 지원합니다.
  - 🚀  다양한 배포 방법: 소스 코드, 쿠버네티스 또는 도커를 통한 배포를 지원합니다.
  - 대규모 사용자 기반 지원: 수십만 명의 사용자를 포함하는 초대형 그룹, 수천만 명의 사용자 및 수십억 건의 메시지를 지원합니다.

### 강화된 비즈니스 기능:

+ **REST API**：OpenIMServer는 비즈니스 시스템을 위한 REST API를 제공하여, 백엔드 인터페이스를 통해 그룹 생성 및 푸시 메시지 전송과 같은 더 많은 기능을 비즈니스에 제공하기 위해 설계되었습니다.
+ **Webhooks**：OpenIMServer는 더 많은 비즈니스 형태를 확장할 수 있는 콜백 기능을 제공합니다. 콜백이란 메시지 전송 전후와 같은 특정 이벤트 전후에 OpenIMServer가 비즈니스 서버로 요청을 보내는 것을 의미합니다.

👉 **[더 알아보기](https://docs.openim.io/guides/introduction/product)**

## :building_construction: 전체 아키텍처

Open-IM-Server의 기능의 핵심으로 들어가 우리의 아키텍처 다이어그램을 자세히 살펴보세요.

![Overall Architecture](../images/architecture-layers.png)

## :rocket: 빠른 시작

우리는 많은 플랫폼을 지원합니다. 웹 측에서 빠른 체험을 위한 주소는 다음과 같습니다:

👉 **[OpenIM online demo](https://www.openim.io/zh/commercial)**

🤲 사용자 경험을 용이하게 하기 위해, 다양한 배포 솔루션을 제공합니다. 아래 목록에서 배포 방법을 선택할 수 있습니다:

+ **[소스 코드 배포 가이드](https://docs.openim.io/guides/gettingStarted/imSourceCodeDeployment)**
+ **[docker 배포 가이드](https://docs.openim.io/guides/gettingStarted/dockerCompose)**
+ **[Kubernetes 배포 가이드](https://docs.openim.io/guides/gettingStarted/k8s-deployment)**
+ **[Mac 개발자 배포 가이드](https://docs.openim.io/guides/gettingstarted/mac-deployment-guide)**

## :hammer_and_wrench: OpenIM 개발 시작하기

[![Open in Dev Container](https://img.shields.io/static/v1?label=Dev%20Container&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/github/openimsdk/open-im-server)

OpenIM의 목표는 최상위 수준의 오픈 소스 커뮤니티를 구축하는 것입니다. 우리는 [커뮤니티 리포지토리에서](https://github.com/OpenIMSDK/community) 일련의 표준을 가지고 있습니다.

이 Open-IM-Server 리포지토리에 기여하고 싶다면, 우리의 [기여자 문서](https://github.com/openimsdk/open-im-server/blob/main/CONTRIBUTING.md)를 읽어주세요.

시작하기 전에, 변경 사항이 필요한지 확인해 주세요. 가장 좋은 방법은 [새로운 토론](https://github.com/openimsdk/open-im-server/discussions/new/choose)을 생성하거나 [Slack 통신을](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q) 하거나, 문제를 발견했다면 먼저 [보고](https://github.com/openimsdk/open-im-server/issues/new/choose)하는 것입니다.

- [OpenIM API 참조](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/api.md)
- [OpenIM Bash 로깅](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/bash-log.md)
- [OpenIM CI/CD 액션](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/cicd-actions.md)
- [OpenIM 코드 규칙](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/code-conventions.md)
- [OpenIM 커밋 가이드라인](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/commit.md)
- [OpenIM 개발 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/development.md)
- [OpenIM 디렉토리 구조](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/directory.md)
- [OpenIM 환경 설정](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/environment.md)
- [OpenIM 오류 코드 참조](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/error-code.md)
- [OpenIM Git 작업 흐름](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/git-workflow.md)
- [OpenIM Git 체리 픽 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/gitcherry-pick.md)
- [OpenIM GitHub 작업 흐름](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/github-workflow.md)
- [OpenIM Go 코드 표준](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/go-code.md)
- [OpenIM 이미지 가이드라인](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/images.md)
- [OpenIM 초기 구성](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/init-config.md)
- [OpenIM docker 설치 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/install-docker.md)
- [OpenIM OpenIM Linux 설치](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/install-openim-linux-system.md)
- [OpenIM Linux 개발 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/linux-development.md)
- [OpenIM 로컬 액션 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/local-actions.md)
- [OpenIM 로깅 규칙](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/logging.md)
- [OpenIM 오프라인 배포](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/offline-deployment.md)
- [OpenIM Protoc 도구](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/protoc-tools.md)
- [OpenIM 테스트 가이드](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/test.md)
- [OpenIM 유틸리티 Go](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-go.md)
- [OpenIM 메이크파일 유틸리티](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-makefile.md)
- [OpenIM 스크립트 유틸리티](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-scripts.md)
- [OpenIM 버전 관리](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/version.md)
- [백엔드 관리 및 모니터 배포](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/prometheus-grafana.md)
- [맥 개발자 배포 가이드 for OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/mac-developer-deployment-guide.md)


## :busts_in_silhouette: 커뮤니티

+ 📚 [OpenIM 커뮤니티](https://github.com/OpenIMSDK/community)
+ 💕 [OpenIM 관심 그룹](https://github.com/Openim-sigs)
+ 🚀 [우리의 Slack 커뮤니티에 가입하기](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q)
+ :eyes: [우리의 위챗(微信群)에 가입하기](https://openim-1253691595.cos.ap-nanjing.myqcloud.com/WechatIMG20.jpeg)

## :calendar: 커뮤니티 미팅

우리는 누구나 커뮤니티에 참여하고 코드를 기여할 수 있도록 하며, 선물과 보상을 제공하며, 매주 목요일 밤에 여러분을 환영합니다.

우리의 회의는 [OpenIM Slack](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q) 🎯에서 이루어지며, Open-IM-Server 파이프라인을 검색하여 참여할 수 있습니다.

우리는 격주 회의의 메모를 [GitHub 토론](https://github.com/openimsdk/open-im-server/discussions/categories/meeting)에서 기록하며, 우리의 역사적 [회의](https://github.com/orgs/OpenIMSDK/discussions/categories/meeting) 노트와 회의 재생은 [Google Docs 📑](https://docs.google.com/document/d/1nx8MDpuG74NASx081JcCpxPgDITNTpIIos0DS6Vr9GU/edit?usp=sharing)에서 이용할 수 있습니다.


## :eyes: OpenIM을 사용하는 사람들

프로젝트 사용자 목록을 위한 우리의 [사용자 사례 연구](https://github.com/OpenIMSDK/community/blob/main/ADOPTERS.md) 페이지를 확인하세요. 사용 사례를 공유하고 싶다면 주저하지 말고 [📝코멘트](https://github.com/openimsdk/open-im-server/issues/379)를 남겨주세요.

## :page_facing_up: 라이선스

OpenIM은 Apache 2.0 라이선스에 따라 라이선스가 부여됩니다. 전체 라이선스 텍스트는 [LICENSE](https://github.com/openimsdk/open-im-server/tree/main/LICENSE)에서 확인할 수 있습니다.

이 리포지토리 [OpenIM](https://github.com/openimsdk/open-im-server)에 표시된 OpenIM 로고, 그 변형 및 애니메이션 버전은 [assets/logo](../../assets/logo) 및 [assets/logo-gif](../../assets/logo-gif) 디렉토리 아래에 있으며, 저작권 법에 의해 보호됩니다.


## 🔮 우리의 기여자들에게 감사합니다!

<a href="https://github.com/openimsdk/open-im-server/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=openimsdk/open-im-server" />
</a>