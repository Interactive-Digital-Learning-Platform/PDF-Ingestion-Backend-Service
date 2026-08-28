import enum


class JobStatus(str, enum.Enum):
    QUEUED     = "queued"
    PROCESSING = "processing"
    DONE       = "done"
    FAILED     = "failed"


class CallbackStatus(str, enum.Enum):
    PENDING  = "pending"
    RETRYING = "retrying"
    DELIVERED = "delivered"
    FAILED   = "failed"