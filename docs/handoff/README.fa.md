# Handoff پروژه asynq و asynqmon

آخرین به‌روزرسانی: 2026-09-06
ریپوی اصلی فعلی: `/root/src`
ریپوی UI/sibling فعلی: `/root/asynqmon`

این handoff وضعیت patch پایدار `v0.27.1` و آماده‌سازی preview با تگ `v1.0.0-beta.1` را ثبت می‌کند. کارهای اجرایی P0 تا P5، اصلاح‌های نهایی ابزارها و کنترل‌های انتشار انجام شده‌اند. ماژول‌های root، `x` و `tools` با tagهای هم‌نسخه منتشر می‌شوند و tagهای قبلی نیز بدون جابه‌جایی حفظ شده‌اند. تست کامل، vet، race، soak هم‌زمان، تست و build رابط کاربری، smoke یکپارچه و اجرای واقعی Redis Cluster سه‌گرهی برای خط پایدار پاس شده‌اند؛ workflow انتشار همین کنترل‌ها را روی commit بتا دوباره اجرا می‌کند.

انتشار beta با workflow اختصاصی GitHub Actions انجام می‌شود: ابتدا تطابق tag و نسخه‌ی embedded کنترل می‌شود، سپس تست‌های race، vet و soak اجرا می‌شوند؛ در پایان CLI برای شش ترکیب سیستم‌عامل/معماری build و همراه `SHA256SUMS` به‌صورت GitHub Pre-release منتشر می‌شود.

هویت ماژول Asynqmon نیز از نسخهٔ `v0.8.0` برابر `github.com/pars-aria-labs/asynqmon` است؛ بنابراین کد برنامه، ابزار smoke و نمونه‌های README همگی از namespace سازمان استفاده می‌کنند.

جزئیات نتیجه هر TODO در [TODO.fa.md](./TODO.fa.md) و قرارداد APIهای جدید در [../batch-inspector.md](../batch-inspector.md) ثبت شده است.

## خلاصه تغییرهای asynq

### هویت و منشأ سورس

سورس و ماژول canonical اکنون `github.com/pars-aria-labs/asynq` است و نام package در کد همچنان `asynq` باقی مانده است. مبنای تاریخچه‌ی واردشده commit `2f4fd0a` است. تاریخچه، LICENSE و attribution نویسندگان اصلی حفظ شده‌اند و README وضعیت نگه‌داری مستقل پروژه را به‌روشنی توضیح می‌دهد.

فاز اول:

- isolation کامل namespace/prefix در مسیرهای Inspector؛
- pipeline شدن `ListServers`، `ListWorkers` و `ListSchedulerEntries` با حفظ skip behavior داده malformed/missing؛
- parse صحیح Redis INFO برای valueهای دارای colon مثل IPv6؛
- بررسی سبک‌تر خالی بودن queue در `RemoveQueue`؛
- پشتیبانی timezone عددی مانند `+0330`؛
- cleanup اتصال Redis در تست‌ها و regression testهای مرتبط؛
- workspace، targetهای Makefile و smoke test سازگاری `asynqmon`.

فاز دوم:

- `TaskBatch` و `ProcessTaskBatch` برای حداکثر 500 transition از state مبدأ در هر درخواست؛ عملیات archive بودجهٔ مستقل و محدود 500تایی برای پاک‌سازی retention دارد؛
- batch داخلی برای APIهای legacy با budget ثابت بر اساس cardinality اولیه؛
- اجرای بدون transport-retry برای Lua mutation، همراه با fallback امن `NOSCRIPT`؛
- حفظ processed count در partial failure؛
- آزادسازی uniqueness lock در حذف batch taskهای pending و aggregating؛
- cleanup درست groupهای aggregation در batch آخر و source خالی؛
- `CurrentStatsBatch` و `GetQueueInfoBatch` با pipeline، context cancellation و cache اختیاری فقط برای memory estimate؛
- fallback هدفمند `NOSCRIPT`، error propagation و پشتیبانی duplicate queue؛
- نگاشت queue ناموجود به `ErrQueueNotFound` در API عمومی؛
- تست‌های کامل RDB و Inspector، benchmark و مستندات عمومی.

فاز سوم:

- `WithContext` برای لغو یا deadline دادن به تمام متدهای قدیمی Inspector، بدون شکستن امضاهای موجود؛
- `TaskBatchPolicy` برای سقف ثابت کار، timeout کل عملیات و backpressure میان batchها؛
- query صفحه‌بندی‌شده بر اساس type، متن خطای آخر و فیلد زمانی انتخابی، سپس mutation فهرست صریح شناسه‌های deduplicate‌شده؛ pagination روی صف زنده snapshot لحظه‌ای نیست؛
- telemetry عمومی و collector کم‌کاردینالیتی Prometheus برای outcome، duration، processed و اجرای instrumentشدهٔ Redis client؛ این مقدار RTT فیزیکی شبکه نیست؛
- compile-time contract test، release note و راهنمای مهاجرت مسیر module؛
- soak test اختیاری با producer و stats هم‌زمان، `SCRIPT FLUSH` دوره‌ای و cleanup کاملاً namespace-scoped.

فایل‌های اصلی افزوده‌شده در این فاز:

- `inspector_batch.go`
- `inspector_batch_test.go`
- `internal/rdb/bulk.go`
- `internal/rdb/bulk_test.go`
- `internal/rdb/stats_batch.go`
- `docs/batch-inspector.md`
- `inspector_context.go`
- `inspector_batch_policy.go`
- `inspector_query.go`
- `inspector_telemetry.go`
- `x/metrics/inspector.go`
- `docs/migrating-to-pars-aria-labs.md`
- `docs/release-notes-v0.27.0.md`
- `docs/release-notes-v0.27.1.md`
- `docs/inspector-soak.md`

تغییرهای workflow در `.github/workflows/build.yml` و `.github/workflows/benchstat.yml` نیز بخشی از تحویل هستند.

## خلاصه تغییرهای asynqmon

سورس و هویت ماژول sibling اکنون `github.com/pars-aria-labs/asynqmon` است. staging اولیه به‌صورت conflict-aware روی تاریخچه‌ی واردشده ادغام شد و dependency هسته نیز مستقیماً از `github.com/pars-aria-labs/asynq` استفاده می‌کند.

- handlerهای queue/task/group از batch APIهای جدید در صورت وجود استفاده می‌کنند و با dependency قدیمی fallback دارند؛
- bulk endpointهای موجود query اختیاری `batch_size=1..500` و فیلد `remaining` دارند؛
- UI مقدار کار اولیه را ثابت می‌کند، progress نشان می‌دهد و mutation نامطمئن را خودکار retry نمی‌کند؛
- response shape قدیمی حفظ شده است؛
- Basic Auth و read-only middleware بدون bypass روی همان routeها اعمال می‌شوند؛
- تست handler واقعی با 523 task، تست middleware، تست fallback/partial failure UI و build assetهای embedded اضافه شده‌اند؛
- `README.md` sibling رفتار compatibility را توضیح می‌دهد.

آرشیو staging اولیه پس از ادغام حذف شد؛ نسخه‌ی نهایی و قابل‌آزمون در working tree `/root/asynqmon` قرار دارد. تغییرهای UI با React 16 و Material UI 4 همان checkout هماهنگ شده‌اند.

## چیدمان و dependency محلی

چیدمان فعلی با `dev/asynqmon.work` سازگار است:

```text
/root/
  src/       # asynq
  asynqmon/  # sibling UI/server
```

workspace، ماژول اصلی asynq، ماژول `x` و sibling را کنار هم قرار می‌دهد. importهای Go در asynqmon اکنون مستقیماً از مسیر canonical یعنی `github.com/pars-aria-labs/asynq` و `github.com/pars-aria-labs/asynq/x` استفاده می‌کنند. graph انتشار root، `x` و `tools` روی `v1.0.0-beta.1` هم‌نسخه است؛ Asynqmon `v0.8.0` برای خط پایدار خود dependencyهای `v0.27.1` را نگه می‌دارد. replaceهای version-specific فقط برای توسعه و آزمون هم‌زمان checkoutهای محلی هستند و در مصرف نسخهٔ منتشرشده نقشی ندارند.

tagهای canonical خط `v0.27.1` در remote منتشر شده‌اند و graph پایدار با `GOWORK=off` آزموده شده است. برای beta، سه tag هم-SHA به‌صورت atomic منتشر می‌شوند؛ سپس workflow و پیش از ساخت GitHub Pre-release، graph مستقل root، `x` و `tools` را بدون workspace محلی تست و vet می‌کند.

اگر parent path تغییر کرد، مسیر `../../asynqmon` در `dev/asynqmon.work` نیز باید متناسب با آن به‌روزرسانی شود.

## اعتبارسنجی ثبت‌شده

- `go test ./...`: پاس؛ package اصلی در اجرای نهایی ایزوله حدود `200.2s` زمان برد.
- `go vet ./...`: پاس؛ vet ماژول‌های `x` و `tools` نیز پاس است.
- `CGO_ENABLED=1 go test -race ./internal/rdb`: پاس در حدود `12.9s`.
- فرمان دقیق CI روی Redis Cluster سه‌گرهی با Go 1.25 و race پاس شد: package اصلی `210.423s`، `internal/rdb` برابر `15.168s` و `x/rate` برابر `1.147s`.
- race test هم‌زمانی cache عمومی Inspector: پاس.
- `make test-asynqmon`: پاس.
- smoke واقعی با `deps-redis:6379`: پاس برای queue/task، pause/resume، batch/remaining و archive/run/delete.
- تست Go sibling با workspace و نیز تست race و vet مستقل با `GOWORK=off`: پاس.
- integration واقعی sibling با 523 task: پاسخ `scheduled=500` و `remaining=23` و state مقصد صحیح.
- UI: دو suite و هشت تست پاس؛ production build پاس.
- `actionlint` و `git diff --check`: پاس.
- soak نهایی patch release: 1999 task enqueue و دقیقاً پردازش شد؛ 3211 batch call، 119 stats read و 16 بار `SCRIPT FLUSH` ثبت شد.

build رابط کاربری فقط warning قدیمی source-map مربوط به Redux Toolkit دارد و artifact قابل deploy تولید می‌شود.

Benchmark `CurrentStatsBatch` در برابر loop:

| Queue | Loop | Batch | بهبود |
| --- | ---: | ---: | ---: |
| 1 | 299692 ns/op | 138822 ns/op | 2.16x |
| 100 | 27972685 ns/op | 3576224 ns/op | 7.82x |

تست‌ها دیتابیس‌های Redis را flush می‌کنند. روی یک Redis مشترک، suiteهای مختلف را هم‌زمان اجرا نکنید؛ CI به همین دلیل test jobها را serial و سرویس‌ها را ایزوله کرده است.
