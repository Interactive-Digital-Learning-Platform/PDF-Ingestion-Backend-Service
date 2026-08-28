

class CollectionNotConfiguredError(Exception):
    """Raised when job.qdrant_collection does not exist. Collections are created only via
    qdrant-migrations/, run by hand — never automatically by the API or worker."""


class PageLimitExceededError(Exception):
    """Raised when job.max_pages is set and the PDF's actual page count exceeds it."""
