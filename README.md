# geoai-data-pipeline

NYC 311 ingestion pipeline with coordinate validation, schema drift detection, and a benchmark comparing concurrent vs sequential file loading.

Part of my [35-project AI Engineering Roadmap](https://ai-engineer-roadmap-ee841.web.app/projects).

---

## What it does

- Downloads NYC 311 service requests via the Socrata API (paginated, three dataset sizes)
- Loads CSVs using `asyncio` + `run_in_executor` for concurrent multi-file loading
- Drops rows with missing or non-numeric coordinates
- Normalises column names (strips whitespace, lowercases, replaces spaces with underscores)
- Flags schema drift when expected columns are absent
- Reports encoding error counts

---

## Project structure

```
geoai-data-pipeline/
├── pipeline.py          # async loader + LoadResult dataclass
├── benchmark.py         # sync baseline + concurrent vs sequential comparison
├── download_data.py     # Socrata API download (small / medium / large)
├── tests/
│   └── test_pipeline.py # 5 unit tests
└── data/                # downloaded CSVs (gitignored)
```

---

## Setup

```bash
pip install pandas pytest requests
```

Download data (no API key required, NYC Open Data is public):

```bash
python download_data.py
```

---

## Running the benchmark

```bash
python benchmark.py
```

Sample output:

```
-- sync baseline (single file) ------------------------
tier             rows     size     time
------------------------------------------
small         100,000     65MB    2.07s
medium        500,000    328MB    8.42s

splitting nyc311_small.csv into 5 parts...

-- concurrent vs sequential (5 files x ~20,000 rows) --
method                         total rows     time
--------------------------------------------------
async (run_in_executor)            97,631    4.98s
sync (sequential)                  97,631    5.92s

  async: 1.2x faster than sequential
```

---

## What I learned

For small local files (around 20K rows each), running `pd.read_csv` concurrently via `asyncio` + `run_in_executor` is modestly faster than sequential reads, about 1.2x in this benchmark.

The gain is real but small. Here is why:

**How the async side works**

`run_in_executor` submits each file load to Python's default thread pool. `asyncio.gather` fires all five off at once, so the thread pool runs them concurrently rather than waiting for each to finish before starting the next.

**Why the gain is limited**

Python has something called the Global Interpreter Lock (GIL). The GIL is a mutex inside CPython that prevents more than one thread from executing Python bytecode at the same time. Even with a thread pool, threads take turns and do not actually run in true parallel on multiple cores.

For `pd.read_csv`, there are two phases:

1. **Input/Output (I/O): reading bytes from disk.** The GIL is released during the actual disk read system call, so threads can genuinely overlap here. This is where the 1.2x gain comes from.

2. **Parsing: converting those bytes into a DataFrame.** The pandas C parser holds the GIL during this work, so even though multiple threads are active, only one can parse at a time. This is the bottleneck that caps the gain.

The result is that you get some overlap on the Input/Output (I/O) side, but the CPU-bound parsing still mostly serializes. For small files where parsing is fast, the thread pool overhead is close to the benefit, which is why the result is 1.2x and not 5x.

**Where async actually shines**

The GIL is released while a thread is waiting on a network response. If each file were fetched from an API or cloud storage instead of read from local disk, each thread would spend most of its time waiting on the network, and the GIL would be free throughout. Five concurrent network fetches that each take 500ms would finish in roughly 500ms total instead of 2500ms. That is where `asyncio.gather` shows a real multiplier.

Local disk Input/Output (I/O) is fast enough that the OS cache delivers data almost immediately, so there is not much latency for async to hide.

---

## Tests

```bash
pytest tests/
```

Five unit tests cover: valid row loading, missing coordinate filtering, column name normalisation, schema drift detection, and non-numeric coordinate handling.

A second test file (`tests/test_verify.py`) checks implementation assumptions directly. It verifies that `load()` uses `run_in_executor`, that both benchmark sides call the same underlying function, and that concurrent loading is not dramatically slower than sequential. If it were dramatically slower, that would indicate the event loop is being blocked rather than work being properly offloaded.

---

## Dataset

NYC 311 Service Requests from [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9), public domain.
