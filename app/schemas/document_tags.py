from pydantic import BaseModel, ConfigDict, field_validator

from app.core.config import settings


def canonical_subject(value: str) -> str | None:
    key = " ".join(value.split()).casefold()

    return next(
        (subject for subject in settings.KB_SUBJECTS if subject.casefold() == key),
        None,
    )


class DocumentTags(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grade: int | None = None
    subject: str | None = None

    @field_validator("grade")
    @classmethod
    def _grade_in_range(cls, value: int | None) -> int | None:
        if value is not None and not settings.KB_MIN_GRADE <= value <= settings.KB_MAX_GRADE:
            raise ValueError(
                f"grade must be between {settings.KB_MIN_GRADE} and {settings.KB_MAX_GRADE}"
            )
        return value

    @field_validator("subject", mode="before")
    @classmethod
    def _known_subject(cls, value: object) -> str | None:
        if value is None or (isinstance(value, str) and not value.strip()):
            return None

        if not isinstance(value, str):
            raise ValueError("subject must be a string")

        subject = canonical_subject(value)
        if subject is None:
            raise ValueError(f"subject must be one of: {', '.join(settings.KB_SUBJECTS)}")
        return subject

    def payload(self) -> dict[str, int | str]:
        return {key: value for key, value in self.model_dump().items() if value is not None}
