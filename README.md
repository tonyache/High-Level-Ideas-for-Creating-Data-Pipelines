# Sessionization of Web Log Data in C++

## 🧠 Motivation

Modern web servers receive millions of HTTP requests from users every day.
Each request contains a timestamp, an IP address, and various identifiers.
However, understanding user behavior requires more than just counting requests —
we need to know **how long each user stayed active** and **when their sessions ended**.

This project implements a **sessionization engine** in C++.
It takes a stream of timestamped requests and groups them into **user sessions** based on inactivity timeouts.
If a user stops sending requests for a configurable number of seconds, their session is considered finished.

The resulting session data (start time, end time, duration, and request count) can be used for:
- analyzing traffic patterns,
- identifying engagement drop-offs,
- detecting abnormal user behavior,
- or preparing inputs for downstream analytics systems.

---

## ⚙️ Methodology

### 1) Inputs

- **`input/log.csv`** — a CSV log file with the header `ip,date,time,cik,accession,extension`
  (aliases like `extention` or `doc` are accepted).
- **`input/inactivity_period.txt`** — a single integer: the number of seconds of allowed inactivity.

Example:
```
2
```

### 2) Core Idea

Each IP address is treated as a distinct user. As the log is processed line by line in chronological order:

1. The current request updates that user’s *last seen time*.
2. If any user’s *last seen time + inactivity period* is **less than** the current timestamp,
   their session is **closed and written to output**.
3. After the final log line, all remaining sessions are flushed.

**Inclusive duration:**

```
duration = (last_request_time - first_request_time) + 1
```

### 3) Data Structures

- **unordered_map** for active sessions per IP (start time, last time, request count, arrival order).
- **priority_queue** keyed by expiration time (and arrival order) to close sessions efficiently.

This design yields streaming behavior with `O(log A)` expiration work (`A` = number of active sessions),
and modest memory usage.

### 4) Output

`output/sessionization.txt` with one line per finished session:

```
ip,start_time,end_time,duration,request_count
```

Timestamps are UTC in `YYYY-MM-DD HH:MM:SS` format.

### 5) Running

Using the helper script:

```bash
./run.sh
```

Manual compile and run:

```bash
mkdir -p build output
clang++ -O2 -std=c++17 -o build/edgar_sessionization src/edgar_sessionization.cpp
./build/edgar_sessionization input/log.csv input/inactivity_period.txt output/sessionization.txt
```

### 6) Notes

- Portable C++17, no external deps.
- Robust header handling: case-insensitive, trims whitespace, accepts aliases (`extension/extention/doc`).
- Deterministic ordering when multiple sessions end at the same second (by first-seen order).

### 7) Possible Extensions

- Join sessions using cookies or user IDs (beyond IP).
- Real-time ingestion with incremental output.
- Emit to Parquet/Arrow or a database.
- Add visualization notebooks to plot active sessions over time.
