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

## درباره OpenIM Ⓜ️

OpenIM یک پلتفرم خدماتی است که به طور خاص برای ادغام چت، تماس های صوتی و تصویری، اعلان ها و چت ربات های هوش مصنوعی در برنامه ها طراحی شده است. این مجموعه ای از API ها و Webhook های قدرتمند را ارائه می دهد که به توسعه دهندگان این امکان را می دهد تا به راحتی این ویژگی های تعاملی را در برنامه های خود بگنجانند. OpenIM یک برنامه چت مستقل نیست، بلکه به عنوان یک پلتفرم برای پشتیبانی از برنامه های کاربردی دیگر در دستیابی به قابلیت های ارتباطی غنی عمل می کند. نمودار زیر تعامل بین AppServer، AppClient، OpenIMServer و OpenIMSDK را برای توضیح جزئیات نشان می دهد.

![App-OpenIM Relationship](../images/oepnim-design.png)

## 🚀 درباره OpenIMSDK

**OpenIMSDK** یک IM SDK است که برای **OpenIMServer** طراحی شده است که به طور خاص برای جاسازی در برنامه های مشتری ایجاد شده است. ویژگی ها و ماژول های اصلی آن به شرح زیر است:

+ 🌟 ویژگی های اصلی:

  - 📦 ذخیره سازی محلی
  - 🔔 پاسخ تماس شنونده
  - 🛡️ بسته بندی API
  - 🌐 مدیریت اتصال

+ 📚 ماژول های اصلی:

  1. 🚀 مقداردهی اولیه و ورود
  2. 👤 مدیریت کاربر
  3. 👫 مدیریت دوست
  4. 🤖 توابع گروه
  5. 💬 مدیریت مکالمه

این برنامه با استفاده از Golang ساخته شده است و از استقرار چند پلت فرم پشتیبانی می کند و تجربه دسترسی ثابت را در تمام پلتفرم ها تضمین می کند.

👉 **[کاوش GO SDK](https://github.com/openimsdk/openim-sdk-core)**

## 🌐 درباره OpenIMServer

+ **OpenIMServer** دارای ویژگی های زیر است:
  - 🌐 معماری Microservice: از حالت کلاستر، از جمله یک دروازه و چندین سرویس rpc پشتیبانی می کند.
  - 🚀 روش‌های استقرار متنوع: از استقرار از طریق کد منبع، Kubernetes یا Docker پشتیبانی می‌کند.
  - پشتیبانی از پایگاه عظیم کاربران: گروه های فوق العاده بزرگ با صدها هزار کاربر، ده ها میلیون کاربر و میلیاردها پیام.

### عملکردهای تجاری پیشرفته:

+ **REST API**: OpenIMServer APIهای REST را برای سیستم‌های تجاری ارائه می‌کند، با هدف توانمندسازی کسب‌وکارها با قابلیت‌های بیشتر، مانند ایجاد گروه‌ها و ارسال پیام‌های فشار از طریق رابط‌های باطنی.
+ **Webhooks**: OpenIMServer قابلیت های پاسخ به تماس را برای گسترش بیشتر فرم های تجاری ارائه می دهد. پاسخ به تماس به این معنی است که OpenIMServer درخواستی را قبل یا بعد از یک رویداد خاص به سرور تجاری ارسال می کند، مانند تماس های قبل یا بعد از ارسال یک پیام.

👉 **[بیشتر بدانید](https://docs.openim.io/guides/introduction/product)**

## :building_construction: معماری کلی

با نمودار معماری ما به قلب عملکرد Open-IM-Server بپردازید.

![Overall Architecture](../images/architecture-layers.png)


## :rocket: شروع سریع

ما از بسیاری از پلتفرم ها پشتیبانی می کنیم. در اینجا آدرس هایی برای تجربه سریع در سمت وب آمده است:

👉 **[نسخه نمایشی وب آنلاین OpenIM](https://web-enterprise.rentsoft.cn/)**

🤲 برای تسهیل تجربه کاربر، ما راه حل های مختلف استقرار را ارائه می دهیم. می توانید روش استقرار خود را از لیست زیر انتخاب کنید:

+ **[راهنمای استقرار کد منبع](https://docs.openim.io/guides/gettingStarted/imSourceCodeDeployment)**
+ **[راهنمای استقرار داکر](https://docs.openim.io/guides/gettingStarted/dockerCompose)**
+ **[راهنمای استقرار Kubernetes](https://docs.openim.io/guides/gettingStarted/k8s-deployment)**
+ **[راهنمای استقرار توسعه دهنده مک](https://docs.openim.io/guides/gettingstarted/mac-deployment-guide)**

## :hammer_and_wrench: برای شروع توسعه OpenIM

[![Open in Dev Container](https://img.shields.io/static/v1?label=Dev%20Container&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/github/openimsdk/open-im-server)

OpenIM هدف ما ایجاد یک جامعه منبع باز سطح بالا است. ما مجموعه ای از استانداردها را در [مخزن انجمن](https://github.com/OpenIMSDK/community) داریم..

اگر می‌خواهید در این مخزن Open-IM-Server مشارکت کنید، لطفاً [مستندات مشارکت‌کننده](https://github.com/openimsdk/open-im-server/blob/main/CONTRIBUTING.md) ما را بخوانید.

قبل از شروع، لطفاً مطمئن شوید که تغییرات شما مورد تقاضا هستند. بهترین کار برای آن این است که یک [بحث جدید](https://github.com/openimsdk/open-im-server/discussions/new/choose) یا [ارتباط اسلک](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q) ایجاد کنید، یا اگر مشکلی پیدا کردید، ابتدا [آن را گزارش کنید](https://github.com/openimsdk/open-im-server/issues/new/choose).

- [مرجع OpenIM API](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/api.md)
- [OpenIM Bash Logging](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/bash-log.md)
- [OpenIM CI/CD Actions](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/cicd-actions.md)
- [کنوانسیون کد OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/code-conventions.md)
- [دستورالعمل های تعهد OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/commit.md)
- [راهنمای توسعه OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/development.md)
- [ساختار دایرکتوری OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/directory.md)
- [تنظیم محیط OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/environment.md)
- [مرجع کد خطا OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/error-code.md)
- [OpenIM Git Workflow](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/git-workflow.md)
- [راهنمای انتخاب گیلاس OpenIM Git](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/gitcherry-pick.md)
- [OpenIM GitHub Workflow](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/github-workflow.md)
- [استانداردهای کد OpenIM Go](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/go-code.md)
- [دستورالعمل های تصویر OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/images.md)
- [پیکربندی اولیه OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/init-config.md)
- [راهنمای نصب OpenIM Docker](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/install-docker.md)
- [نصب سیستم OpenIM Linux OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/install-openim-linux-system.md)
- [راهنمای توسعه OpenIM Linux](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/linux-development.md)
- [راهنمای اقدامات محلی OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/local-actions.md)
- [OpenIM Logging Conventions](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/logging.md)
- [استقرار آفلاین OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/offline-deployment.md)
- [OpenIM Protoc Tools](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/protoc-tools.md)
- [راهنمای تست OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/test.md)
- [OpenIM Utility Go](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-go.md)
- [OpenIM Makefile Utilities](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-makefile.md)
- [ابزارهای OpenIM Script](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/util-scripts.md)
- [نسخه OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/version.md)
- [مدیریت استقرار باطن و نظارت](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/prometheus-grafana.md)
- [راهنمای استقرار توسعه دهنده مک برای OpenIM](https://github.com/openimsdk/open-im-server/tree/main/docs/contrib/mac-developer-deployment-guide.md)


## :busts_in_silhouette: انجمن

+ 📚 [انجمن OpenIM](https://github.com/OpenIMSDK/community)
+ 💕 [گروه علاقه OpenIM](https://github.com/Openim-sigs)
+ 🚀 [به انجمن Slack ما بپیوندید](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q)
+ :eyes: [به وی چت ما بپیوندید](https://openim-1253691595.cos.ap-nanjing.myqcloud.com/WechatIMG20.jpeg)

## :calendar: جلسات جامعه

ما می‌خواهیم هر کسی در انجمن ما مشارکت کند و در کد مشارکت کند، ما هدایا و جوایزی ارائه می‌کنیم، و از شما استقبال می‌کنیم که هر پنجشنبه شب به ما بپیوندید.

کنفرانس ما در [OpenIM Slack](https://join.slack.com/t/openimsdk/shared_invite/zt-22720d66b-o_FvKxMTGXtcnnnHiMqe9Q) 🎯 است، سپس می توانید خط لوله Open-IM-Server را برای پیوستن جستجو کنید.

ما از هر [جلسه دو هفته‌ای](https://github.com/orgs/OpenIMSDK/discussions/categories/meeting) در  [بحث‌های GitHub](https://github.com/openimsdk/open-im-server/discussions/categories/meeting) یادداشت‌برداری می‌کنیم، یادداشت‌های جلسه تاریخی ما، و همچنین بازپخش جلسات در  [Google Docs :bookmark_tabs:](https://docs.google.com/document/d/1nx8MDpuG74NASx081JcCpxPgDITNTpIIos0DS6Vr9GU/edit?usp=sharing) موجود است.

## :eyes: چه کسانی از OpenIM استفاده می کنند

صفحه [مطالعات موردی کاربر](https://github.com/OpenIMSDK/community/blob/main/ADOPTERS.md) ما را برای لیستی از کاربران پروژه بررسی کنید. از گذاشتن [نظر📝](https://github.com/openimsdk/open-im-server/issues/379) و به اشتراک گذاری مورد استفاده خود دریغ نکنید.

## :page_facing_up: مجوز

OpenIM تحت مجوز Apache 2.0 مجوز دارد. برای متن کامل مجوز به [LICENSE](https://github.com/openimsdk/open-im-server/tree/main/LICENSE) مراجعه کنید.

نشان‌واره OpenIM، شامل انواع و نسخه‌های متحرک آن، که در این مخزن [OpenIM](https://github.com/openimsdk/open-im-server) تحت فهرست‌های [assets/logo](./assets/logo) و [assets/logo-gif](assets/logo-gif) نمایش داده می‌شود، توسط قوانین حق چاپ محافظت می‌شود.

## 🔮 با تشکر از همکاران ما!

<a href="https://github.com/openimsdk/open-im-server/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=openimsdk/open-im-server" />
</a>
