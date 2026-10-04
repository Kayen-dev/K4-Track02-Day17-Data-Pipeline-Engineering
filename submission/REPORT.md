# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:**
**Repo:**
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Hai contract `one_row_per_ticket` và `latest_state_wins` fail: `silver_tickets` có nhiều hàng cho cùng `ticket_id`, T-91 còn cả trạng thái cũ thay vì chỉ `high / closed / bug`. | `feature_daily`, `late_events`, `lookback` fail; u05 thiếu 5 sự kiện của ngày 12/08 và Gold lệch full recompute. | T-97 đã bị xoá ở nguồn nhưng vẫn là hàng sống trong Silver, snapshot mới nhất và RAG index. |
| **Nguyên nhân gốc** | `row_number()` chỉ dedup trong batch; câu `INSERT` luôn append nên không thực thi khoá `ticket_id` giữa nhiều batch và không chặn replay batch cũ. | `LOOKBACK_DAYS = 0` chỉ tính partition của ngày ingest; sự kiện có `event_time` 12/08 nhưng `_ingested_at` 15/08 nên partition 12/08 không được tính lại. | Parser chỉ lấy `ticket_id` từ `after`; với `op = d`, `after = null` nên delete bị `WHERE ticket_id IS NOT NULL` loại bỏ. Kafka tombstone kế tiếp không phải CDC change. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: thay `INSERT` bằng `MERGE ON ticket_id`; chỉ `UPDATE` khi `_lsn` nguồn lớn hơn `_lsn` đích, còn khoá mới thì `INSERT`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = ceil(P99) = 3`; giữ phép gom nhóm theo `CAST(event_time AS DATE)` và overwrite các partition trong cửa sổ. | `pipeline/staging.py`: lấy khoá bằng `coalesce(after.ticket_id, before.ticket_id)`; Silver ghi tombstone PII-null và giữ `_lsn`, nên replay bản cũ không hồi sinh ticket. |
| **Khái niệm trên slide** | Silver có khoá; keyed upsert/MERGE idempotent; CDC ordering bằng LSN. | Event time khác processing/ingest time; late data, watermark/lookback, overwrite-partition idempotent. | Debezium CDC delete khác Kafka compaction tombstone; delete propagation/erasure và tombstone có thứ tự. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- Bất biến replay: `C0` = fresh build; `C1`, `C2`, `C3` = sau ba lần chạy lại `2026-08-12`; PASS khi `C0 = C1 = C2 = C3`.
- `make parity`: PARITY — `silver_tickets` và `gold_feature_daily` cùng checksum giữa lite và dbt.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ đúng một trạng thái hiện tại cho mỗi ticket; overwrite-partition tính lại trọn cửa sổ event-date nên nhận được late data mà vẫn idempotent.
- Tham số dbt: `unique_key` là khoá MERGE; `merge_update_condition` chỉ nhận LSN mới hơn; `batch_size='day'` chia Gold theo event-day; `lookback=3` tính lại ba batch trước để nhận late data.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ `_lsn` và dấu xoá để audit, truyền delete xuống Gold và ngăn replay thay đổi cũ làm T-97 sống lại.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu seed nhỏ chạy cục bộ nên DuckDB đủ nhẹ; dbt bổ sung incremental/microbatch khai báo, contract, test và parity mà không cần hạ tầng phân tán.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

### Thử thách 1 — Silver key

```text
PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"
..                                                                       [100%]
2 passed, 11 deselected in 1.52s
```

### Thử thách 2 — Late data

```text
PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "feature_daily or late_events or lookback"
...                                                                      [100%]
3 passed, 10 deselected in 1.70s
```

### Thử thách 3 — CDC delete

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.23s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
C0 fresh build          8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
C1 re-run 2026-08-12    8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
C2 re-run 2026-08-12    8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
C3 re-run 2026-08-12    8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — C0 = C1 = C2 = C3
```

### Thử thách 4 — dbt parity

```text
PS> .\.venv\Scripts\python.exe main.py --land-only
2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

PS> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily
Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily
Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily
Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily
Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily
Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily
Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
