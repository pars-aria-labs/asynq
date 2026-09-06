# وضعیت کارهای handoff

آخرین به‌روزرسانی: 2026-09-06

کارهای اجرایی P0 تا P5 این handoff تکمیل شده‌اند. این فایل نتیجه و محدودیت‌های اعتبارسنجی را ثبت می‌کند تا فهرست قدیمیِ کارها با وضعیت فعلی اشتباه نشود.

## P0: پایدارسازی فاز دوم — انجام شد

- ترتیب `deleteAllAggregatingCmd` اصلاح شد: taskها ابتدا از zset حذف می‌شوند و فقط پس از `ZCARD=0` نام group از set گروه‌ها پاک می‌شود.
- cleanup گروه خالی در حالت count صفر برای delete، run و archive تست شد.
- عملیات legacy با cardinality اولیه budget می‌گیرد؛ producer هم‌زمان نمی‌تواند loop را نامحدود نگه دارد.
- retry خودکار transport برای Lua mutation غیرفعال است تا گم‌شدن reply باعث اجرای batch دوم و عبور از limit نشود؛ fallback امن `NOSCRIPT` حفظ شده است.
- `runBulkScript` و wrapperهای legacy در خطای batch بعدی، تعداد batchهای موفق قبلی را همراه error حفظ می‌کنند.
- `TaskBatch` نیز در خطای شمارش `remaining` پس از mutation، مقدار `processed` را از دست نمی‌دهد.
- حذف batch در حالت‌های pending و aggregating قفل taskهای unique را آزاد می‌کند و task گروهی unique نیز کلید قفل را در hash نگه می‌دارد.
- خطای queue ناموجود در لایه داخلی و API عمومی قابل تشخیص است.

## P1: تست، race و benchmark — انجام شد

- matrix همه ترکیب‌های معتبر delete/run/archive برای stateهای `pending`، `scheduled`، `retry`، `archived`، `completed` و `aggregating` در هر دو لایه RDB و Inspector پوشش داده شد.
- validation مربوط به limit، queue، group، state/action نامعتبر و context لغوشده پوشش داده شد.
- `CurrentStatsBatch` برای 103 queue، ترتیب ورودی، duplicate، cache و TTL صفر، queue ناموجود یا حذف‌شده حین pipeline، خطای pipeline، context لغوشده و cold `NOSCRIPT` تست شد.
- دسترسی هم‌زمان به cache حافظه Inspector زیر race detector تست شد.
- benchmark با 100 iteration روی `deps-redis:6379`:
  - یک queue: loop حدود `299692 ns/op` و batch حدود `138822 ns/op`؛ تقریباً 2.16 برابر سریع‌تر.
  - صد queue: loop حدود `27972685 ns/op` و batch حدود `3576224 ns/op`؛ تقریباً 7.82 برابر سریع‌تر.
- `CGO_ENABLED=1 go test -race ./internal/rdb -redis_addr=deps-redis:6379` پاس شد.

## P2: ادغام کنترل‌شده asynqmon — انجام شد

- چون sibling اولیه در محیط حاضر نبود، checkout متناظر `parsidev/asynqmon` در `/root/asynqmon` ساخته شد.
- hash فایل‌های handler با manifest آرشیو تطبیق داشت و تغییرهای Go ادغام شد.
- UI آرشیو روی یک migration متفاوت MUI/React ساخته شده بود؛ قابلیت bulk به‌صورت دستی با React 16، Material UI 4، Jest و axios همین checkout ادغام شد و فایل‌های unrelated جایگزین نشدند.
- endpointهای قدیمی با query اختیاری `batch_size=1..500` پاسخ `remaining` می‌دهند؛ dependency فاقد batch API همچنان از مسیر legacy استفاده می‌کند.
- UI budget اولیه را ثابت نگه می‌دارد، progress نشان می‌دهد و mutation دارای پاسخ نامطمئن را خودکار retry نمی‌کند.
- اگر mutation موفق شود ولی خواندن `remaining` خطا بدهد، endpoint مقدار `processed` تأییدشده را در پاسخ خطا نگه می‌دارد و UI آن را به progress اضافه می‌کند.
- Basic Auth و read-only middleware روی route واقعی به‌ترتیب 401 و 405 برمی‌گردانند.
- smoke test با 501 task گسترش یافت و پاسخ‌های `archived=500, remaining=1` و سپس `archived=1, remaining=0` را بررسی می‌کند.
- assetهای production دوباره build شدند، چون مستقیماً در binary تعبیه می‌شوند.

## P3: CI — انجام شد

- matrix ناسازگار Go 1.24 حذف و Go 1.25 تنظیم شد.
- حالت‌های Redis standalone و cluster سه‌گرهی جدا شدند و تست‌ها برای جلوگیری از تداخل `FLUSHDB` به‌صورت serial اجرا می‌شوند.
- core، `x` و `tools` با race detector تست می‌شوند.
- job سازگاری، sibling `pars-aria-labs/asynqmon` را checkout می‌کند و با `dev/asynqmon.work` تست Go، تست UI و production build را اجرا می‌کند.
- workflow قدیمی benchstat به یک workflow دستی و فعال تبدیل شد.
- هر دو workflow با `actionlint` بررسی شدند.

## اعتبارسنجی نهایی محلی

موارد زیر پاس شدند:

```text
go test ./...
go vet ./...
(cd x && go test ./... && go vet ./...)
(cd tools && go test ./... && go vet ./...)
CGO_ENABLED=1 go test -race ./internal/rdb -redis_addr=deps-redis:6379
make test-asynqmon
ASYNQ_TEST_REDIS_ADDR=deps-redis:6379 make smoke-asynqmon
GOWORK=/root/src/dev/asynqmon.work ASYNQ_TEST_REDIS_ADDR=deps-redis:6379 go test ./...   # در sibling
CI=true npm test --prefix ui -- --watchAll=false
npm run build --prefix ui
```

importهای sibling به مسیر canonical `github.com/pars-aria-labs/asynq` مهاجرت کرده‌اند و `go.mod`های root، `x`، `tools` و Asynqmon نسخهٔ واقعی `v0.27.0` را pin می‌کنند. tagهای `v0.27.0`، `x/v0.27.0` و `tools/v0.27.0` منتشر شده‌اند و build، test و vet مستقل ماژول‌های وابسته با `GOWORK=off` و بدون replace محلی پاس شده است. workspace صرفاً برای توسعهٔ هم‌زمان checkoutها حفظ می‌شود.

تست cluster در این container اجرا نشد، چون endpoint کلاستر یا Docker در دسترس نبود. راه‌اندازی و اجرای آن در workflow CI اضافه شده و اجرای واقعی workflow پس از push تنها بررسی محیطی باقی‌مانده است.

## P4: APIهای عملیاتی و observability — انجام شد

- `Inspector.WithContext` یک نمای سبک از همان اتصال می‌سازد و تمام متدهای قدیمی Inspector را بدون تغییر امضای قبلی، context-aware می‌کند.
- `ProcessTaskBatches` با `TaskBatchPolicy` سقف کار، timeout کل عملیات و فاصله‌ی قابل‌لغو میان batchها را اعمال می‌کند.
- `QueryTasks` روی هر صفحه‌ی محدود، فیلتر دقیق type، substring خطای آخر و بازه‌ی زمانی صریح را اجرا می‌کند؛ pagination روی صف زنده snapshot لحظه‌ای نیست، بنابراین برای انتخاب کامل باید state را موقتاً آرام کرد و شناسه‌ها را deduplicate کرد. `ProcessTaskIDs` فهرست صریح حداکثر 500 شناسه را با حفظ confirmed lower bound پردازش می‌کند.
- observer عمومی Inspector تعداد آیتم، duration، outcome، partial failure و اجرای instrumentشدهٔ Redis client را گزارش می‌دهد؛ این شمارنده معادل دقیق RTT فیزیکی شبکه نیست.
- collector جدید `x/metrics` این داده‌ها را بدون label نام queue، group، task ID یا متن خام خطا به Prometheus می‌دهد.
- exporter واقعی در برابر `deps-redis:6379` اجرا و endpoint آن scrape شد؛ سلامت `deps-prometheus:9090` و Query API آن نیز بررسی شد.

## P5: انتشار fork و آزمون ماندگاری — انجام شد

- مسیر canonical ماژول‌های root، `x` و `tools` به `github.com/pars-aria-labs/asynq` منتقل شد و package identifier همچنان `asynq` باقی ماند.
- importهای source، test، example، ابزار، protobuf metadata و مصرف‌کننده‌ی Asynqmon هماهنگ شدند.
- README منبع اصلی `hibiken/asynq`، fork میانی `parsidev/asynq` و base دقیق `v0.26.0-parsidev-02`/`2f4fd0a` را ثبت می‌کند و تغییرهای fork را همراه نمونه‌های آموزشی توضیح می‌دهد.
- release note، راهنمای migration و compile-time contract test برای API عمومی اضافه شد.
- soak test اختیاری producer، mutation، stats و `SCRIPT FLUSH` هم‌زمان را با namespace یکتا و cleanup محدود به همان namespace اجرا می‌کند.
- اجرای واقعی soak نهایی پاس شد: 2499 task enqueue و دقیقاً 2499 task پردازش شد؛ 3106 batch call، 120 stats read و 16 script flush ثبت شد.

قرارداد و آموزش کامل APIها در `docs/batch-inspector.md`، مهاجرت مسیر ماژول در `docs/migrating-to-pars-aria-labs.md` و اجرای soak در `docs/inspector-soak.md` ثبت شده است.
