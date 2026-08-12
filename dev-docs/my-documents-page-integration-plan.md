# "My Documents" Page — Backend Integration Plan

`PDF-Ingestion-Web-Application`'s `app/dashboard/page.tsx` ("My Documents") currently renders from hardcoded mock state (`useState` with three sample jobs) — see `PDF-Ingestion-Web-Application/CLAUDE.md`. This plan wires it up to real data from this backend.

## Confirmed scope

- **No per-user filtering.** Everyone using this dashboard is a platform admin; the page shows every document any user has uploaded, not just the logged-in user's own uploads.
- **Only terminal jobs are shown.** No `queued`/`processing` rows — the page lists jobs that have finished one way or another: `done` (successfully indexed) and `failed`. In-flight jobs stay off this page (that's what the upload page's job table is for).
- **Delete is real; Download is still a stub.** The three-dot (`MoreHorizontal`) menu has two items, **Download** and **Delete**. Delete is wired to the real `DELETE /ingest/{job_id}` endpoint. Download stays a frontend-only placeholder with no click handler — see "Out of scope" below.

## Current backend state

Routes today (`app/routes/ingestion_routes.py`, prefix `/ingest`):

| Endpoint | Purpose |
|---|---|
| `POST /ingest/upload` | create jobs from uploaded PDFs |
| `GET /ingest/status/{job_id}` | single job's full state |
| `DELETE /ingest/{job_id}` | delete job (MinIO object + Qdrant points + DB row) |
| `WS /ingest/ws/{job_id}` | realtime progress for one job, via Redis pub/sub |

There is no list endpoint. `app/services/job_service.py` already has:

```python
class JobService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_jobs(self) -> list[Job]:
        result = await self.session.execute(select(Job))
        return list(result.scalars().all())


def get_job_service(session: AsyncSession = Depends(get_async_session)) -> JobService:
    return JobService(session)
```

— but it's dead code: never imported by any router, and it already does exactly what's needed here (no `user_id` filter, returns every job). It just needs an ordering tweak and a route.

## Backend changes

**1. `app/services/job_service.py`** — order results so newest uploads appear first:

```python
async def get_all_jobs(self) -> list[Job]:
    result = await self.session.execute(
        select(Job).order_by(Job.started_at.desc())
    )
    return list(result.scalars().all())
```

No signature change needed — no `user_id` parameter, by design (per the "no per-user filtering" decision above).

**2. `app/routes/ingestion_routes.py`** — add a list route, mirroring the response shape of the other endpoints (`Job.to_dict()`):

```python
from app.services.job_service import JobService, get_job_service

@ingestion_router.get("/jobs")
async def list_jobs(
    job_service: JobService = Depends(get_job_service),
):
    jobs = await job_service.get_all_jobs()
    return {"jobs": [job.to_dict() for job in jobs]}
```

Route path is `/ingest/jobs`, distinct from the existing `/ingest/status/{job_id}` and `DELETE /ingest/{job_id}` so there's no path collision with the `{job_id}` param routes.

No new schema needed — `Job.to_dict()` already returns every field the frontend's `Job` type (`lib/api/types.ts`) expects (`job_id`, `filename`, `status`, `progress`, `current_stage`, `chunks_created`, `pages_processed`, `started_at`, `completed_at`, etc.).

## Frontend changes (`PDF-Ingestion-Web-Application`)

**1. `lib/api/ingestion.ts`** — add:

```ts
export async function listJobs(): Promise<Job[]> {
  const response = await apiClient.get<{ jobs: Job[] }>("/ingest/jobs");
  return response.data.jobs;
}
```

**2. `lib/query/ingestion.ts`** — add a query hook and key:

```ts
export const ingestionQueryKeys = {
  job: (jobId: string) => ["ingestion-job", jobId] as const,
  jobs: () => ["ingestion-jobs"] as const,
};

export function useJobsQuery() {
  return useQuery({
    queryKey: ingestionQueryKeys.jobs(),
    queryFn: listJobs,
    refetchInterval: 15000,
  });
}
```

No per-row WebSocket here — the page only ever shows terminal jobs, so there's no live progress to stream. A light fixed poll (15s) just catches jobs that finished (or failed) since the last fetch; `refetchOnWindowFocus` is already disabled globally in `app/providers.tsx`, so this is the only freshness mechanism for the page.

**3. `app/dashboard/page.tsx`** — replace the mock `useState(jobs)` with `useJobsQuery()`. Filter the fetched jobs to `status === 'done' || status === 'failed'` (both are "terminal" — the mock's existing `documents = jobs.filter(...)` line becomes a two-way filter instead of `'done'`-only) and drive the table from that. Render the real `status` badge per row (done → success styling, failed → destructive styling) instead of the hardcoded "Indexed" badge, since failed rows are now visible. Keep the three stat cards (Documents / Total Pages / Total Chunks) computed from `done` jobs only — a failed job has no pages/chunks worth counting toward the knowledge base. Replace the "Added" column's hardcoded "Just now" with `started_at` (or `completed_at` when present), formatted.

**4. Actions column** — collapse to the three-dot dropdown already scaffolded in the mock (`DropdownMenu` / `MoreHorizontal`), wired as:

```tsx
const deleteMutation = useDeleteJobMutation();
const queryClient = useQueryClient();

const handleDelete = async (jobId: string, filename: string) => {
  try {
    await deleteMutation.mutateAsync(jobId);
    queryClient.setQueryData<Job[]>(ingestionQueryKeys.jobs(), (prev) =>
      prev?.filter((job) => job.job_id !== jobId),
    );
    toast.success(`Deleted "${filename}".`);
  } catch (error) {
    toast.error(toErrorMessage(error));
  }
};
```

```tsx
<DropdownMenuItem>
  <Download className="mr-2 h-4 w-4" />
  Download
</DropdownMenuItem>
<DropdownMenuItem variant="destructive" onClick={() => handleDelete(doc.job_id, doc.filename)}>
  <Trash2 className="mr-2 h-4 w-4" />
  Delete
</DropdownMenuItem>
```

`useDeleteJobMutation` already exists in `lib/query/ingestion.ts` and is already used the same way in `components/dashboard/dashboard-shell.tsx` — this page reuses it rather than adding a new mutation. Download keeps no `onClick` — pure placeholder this phase.

**5. "New Document" button** — point at `/dashboard/upload` via `next/link` (currently a dead button).

## Out of scope (deferred, not part of this plan)

- Wiring **Download** to anything — there is currently no presigned-URL / download-by-job-id endpoint on the backend at all (`StorageService` never calls `generate_presigned_url`). Needs its own backend endpoint before it can do anything real.
- Showing in-flight (`queued`/`processing`) jobs on this page — intentionally excluded from this scope; that's covered by the upload page's own job table.
- Pagination/search/filtering server-side — `GET /ingest/jobs` returns the full table; fine at current expected volume, revisit if the jobs table grows large.

## Verification

- `GET /ingest/jobs` with an empty `jobs` table returns `{"jobs": []}`.
- Upload a PDF, confirm it does **not** appear on the My Documents page while `queued`/`processing`, then appears once it reaches `done` (or `failed`).
- Confirm jobs from multiple different `user_id`s all appear in the same response (no filtering).
- Frontend: dashboard page shows only done/failed documents (failed ones visibly marked), stat cards only count `done`, three-dot menu's Delete actually removes the job (row disappears, backend row/MinIO object/Qdrant points gone), Download has no wired behavior.
