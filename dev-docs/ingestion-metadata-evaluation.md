# Ingestion Pipeline Metadata Evaluation

Verifying a reported issue: the original uploaded filename is lost during ingestion, and Qdrant ends up storing the temp-download filename instead. Traced through `app/workers/tasks.py`, `app/pipeline/extractor.py`, `app/pipeline/chunker.py`, `app/pipeline/indexer.py`, `app/services/celery_storage_service.py`, `app/routes/ingestion_routes.py`, `app/models/job_model.py`.

**Verdict: confirmed.** The root cause and the general direction of the proposed fix are correct, but the fix as originally written is incomplete — two of the four proposed metadata fields (`job_id`, `user_id`) would silently fail to reach Qdrant even after the change, for a reason not covered in the original report. Details below.

---

## 1. (Critical) The original filename is lost — confirmed

`CeleryStorageService.download_to_temp` downloads the object to a randomly-named temp file:

```python
# app/services/celery_storage_service.py
def _download():
    suffix = Path(object_key).suffix or ".pdf"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    ...
    return tmp.name
```

`_run_pipeline` passes that temp path straight into the extractor:

```python
# app/workers/tasks.py — _run_pipeline
tmp_path = await storage.download_to_temp(minio_object_key)
...
extractor = PDFExtractor(min_chars=80)
...
for page in extractor.extract(tmp_path):
    page_buffer.append(page)
```

`PDFExtractor.extract` derives the filename from that same temp path:

```python
# app/pipeline/extractor.py
file_path = Path(pdf_path)
...
filename = file_path.name
...
metadata = {
    "source": str(file_path),
    "filename": filename,
    ...
}
```

So every `PageContent.metadata["filename"]` is something like `tmp5fiq82s6.pdf`, and `metadata["source"]` is the full temp path (e.g. `/tmp/tmp5fiq82s6.pdf`) — neither has any relationship to the file the user actually uploaded.

`_run_pipeline` *does* have the real filename available the whole time — it's a function parameter (`filename: str`), sourced from `Job.filename` via `process_pdf_task(job_id, filename, minio_object_key)` — it's just never used to correct the extractor's metadata before chunking.

**Downstream impact confirmed too.** `HierarchicalChunker._build_chunks` copies `filename`/`source` straight from the first page's (corrupted) metadata into every chunk:

```python
# app/pipeline/chunker.py — _build_chunks
source_meta = pages[0].metadata
...
metadata={
    "source": source_meta.get("source", ""),
    "filename": source_meta.get("filename", ""),
    ...
}
```

And `VectorIndexer.index_chunks` writes that chunk metadata straight into the Qdrant payload:

```python
# app/pipeline/indexer.py — index_chunks
payload={
    "text": chunks[i].text,
    "job_id": job_id,
    "original_chunk_id": chunks[i].chunk_id,
    **chunks[i].metadata,   # filename/source land here, corrupted
},
```

So yes — Qdrant stores `tmp5fiq82s6.pdf` instead of the uploaded filename, exactly as reported. This breaks:
- **Filename-based filtering** in `RetrievalService` / `VectorIndexer.delete_document` — both take a real filename and would never match anything, since nothing in Qdrant has one.
- **Citations** shown to the end user — `SourceCitation.filename` (in `AI-Learning-Assistant-Service`) surfaces this value directly; users would see a meaningless temp filename instead of the document title.

---

## 2. The proposed fix is right in principle, but incomplete for `job_id` / `user_id`

The reported fix — override the extractor's metadata with the real filename right after extraction, before chunking — is correct and sufficient **for `filename` and `source` only**:

```python
for page in extractor.extract(tmp_path):
    page.metadata["filename"] = filename
    page.metadata["source"] = filename
    page_buffer.append(page)
```

This works because `HierarchicalChunker._build_chunks` (shown above) explicitly reads `source_meta.get("source", ...)` and `source_meta.get("filename", ...)` — both keys are on its whitelist, so overriding them upstream flows all the way to Qdrant.

**It does *not* work for the other two fields the report proposed adding** (`page.metadata["job_id"]`, `page.metadata["user_id"]`), because `_build_chunks` doesn't forward arbitrary `source_meta` keys — it rebuilds a brand-new metadata dict with a hardcoded field list that only includes `source`, `filename`, `pages`, `page_start`, `chunk_index`, `total_chunks`, `token_estimate`:

```python
# app/pipeline/chunker.py — _build_chunks
metadata={
    "source": source_meta.get("source", ""),
    "filename": source_meta.get("filename", ""),
    "pages": source_pages,
    "page_start": source_pages[0] if source_pages else 0,
    "chunk_index": idx,
    "total_chunks": len(raw_chunks),
    "token_estimate": _estimate_tokens(text),
},
```

Anything set on `page.metadata` that isn't in this list — including `job_id` and `user_id` as proposed — is silently dropped between the extractor and the chunker. Setting them in the extraction loop alone is a no-op.

Two more things worth noting about those two specific fields:

- **`job_id` is actually already correct today**, independent of this bug: `VectorIndexer.index_chunks` sets `"job_id": job_id` explicitly from its own function parameter (not from chunk metadata), so every point already has the right `job_id` in Qdrant. Adding it to `page.metadata` as proposed would be redundant even if the chunker did forward it.
- **`user_id` isn't in the pipeline at all yet** — it's not a parameter of `_run_pipeline` or `process_pdf_task`, and it isn't passed in the `.delay(...)` call that queues the job, even though the uploader's `user_id` is already captured on the `Job` row at upload time:

  ```python
  # app/routes/ingestion_routes.py — upload_pdfs
  job = Job(
      job_id=uuid.UUID(job_id),
      user_id=user_id,
      filename=filename,
      ...
  )
  ...
  process_pdf_task.delay(
      job_id=job_id,
      filename=filename,
      minio_object_key=object_key,
      # user_id not passed
  )
  ```

  So wiring `user_id` through requires changes at every hop, not just the extraction loop. Note this is provenance/audit metadata about *who uploaded the document* (an admin/content-owner concern) — it's unrelated to, and doesn't reintroduce, the per-end-user retrieval filtering that was separately ruled out for `AI-Learning-Assistant-Service` (that service's knowledge base is shared and admin-curated; end users don't own or scope documents). Recording the uploader on each point is just useful for auditing/cleanup, not for gating retrieval.

### Corrected fix

**a) Fix `filename`/`source` at the extraction loop (as proposed — this part was correct):**

```python
# app/workers/tasks.py — _run_pipeline
for page in extractor.extract(tmp_path):
    page.metadata["filename"] = filename
    page.metadata["source"] = filename
    page_buffer.append(page)
    pages_extracted += 1
    ...
```

**b) Thread `user_id` through the task call chain** so it's actually available in `_run_pipeline`:

```python
# app/routes/ingestion_routes.py — upload_pdfs
process_pdf_task.delay(
    job_id=job_id,
    filename=filename,
    minio_object_key=object_key,
    user_id=user_id,
)
```

```python
# app/workers/tasks.py
@celery.task(bind=True, max_retries=3, default_retry_delay=60, name="tasks.process_pdf")
def process_pdf_task(
    self: Task,
    job_id: str,
    filename: str,
    minio_object_key: str,
    user_id: str,
) -> dict:
    ...
    future = asyncio.run_coroutine_threadsafe(
        _run_pipeline(job_id, filename, minio_object_key, user_id, publisher),
        loop,
    )

async def _run_pipeline(
    job_id: str,
    filename: str,
    minio_object_key: str,
    user_id: str,
    publisher: ProgressPublisher,
) -> dict:
    ...
    for page in extractor.extract(tmp_path):
        page.metadata["filename"] = filename
        page.metadata["source"] = filename
        page.metadata["user_id"] = user_id
        page_buffer.append(page)
```

**c) Extend the chunker's whitelist so `user_id` actually survives into chunk metadata** (this is the step the original report missed — without it, step (b) still silently drops `user_id` before it reaches Qdrant):

```python
# app/pipeline/chunker.py — _build_chunks
metadata={
    "source": source_meta.get("source", ""),
    "filename": source_meta.get("filename", ""),
    "user_id": source_meta.get("user_id", ""),   # NEW
    "pages": source_pages,
    "page_start": source_pages[0] if source_pages else 0,
    "chunk_index": idx,
    "total_chunks": len(raw_chunks),
    "token_estimate": _estimate_tokens(text),
},
```

**d) Harden the indexer so authoritative fields can't be silently clobbered by chunk metadata**, per the original report's request. Today `**chunks[i].metadata` is spread *after* the explicit fields, which means if a chunk's metadata ever contained a key like `job_id` or `text`, it would silently win over the trusted value — currently a latent risk rather than an active bug, but cheap to close off:

```python
# app/pipeline/indexer.py — index_chunks
points = [
    PointStruct(
        id=_deterministic_point_id(job_id, chunks[i].text),
        vector=embeddings[i],
        payload={
            **chunks[i].metadata,
            "text": chunks[i].text,
            "job_id": job_id,
            "original_chunk_id": chunks[i].chunk_id,
        },
    )
    for i in range(len(chunks))
]
```

(Spreading `chunks[i].metadata` first, then setting `text`/`job_id`/`original_chunk_id` afterward, means those three are always authoritative regardless of what ends up in chunk metadata.)

---

## Existing data note

This bug means every document ingested before the fix already has the wrong `filename`/`source` stored in Qdrant. The fix only corrects new ingestions — existing points will need either a payload backfill (Qdrant supports `set_payload` by filter, keyed off `job_id`, which is already correct, joined against `Job.filename` in Postgres) or a full re-ingestion of affected documents.
