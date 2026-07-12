from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.models.job_model import Job


class JobService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_jobs(self) -> list[Job]:

        result = await self.session.execute(select(Job))

        return list(result.scalars().all())


def get_job_service(session: AsyncSession = Depends(get_async_session)) -> JobService:
    return JobService(session)
