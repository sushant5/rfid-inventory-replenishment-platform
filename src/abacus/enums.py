from enum import StrEnum


class TenantStatus(StrEnum):
    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"


class StoreStatus(StrEnum):
    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class ZoneKind(StrEnum):
    SALES_FLOOR = "SALES_FLOOR"
    BACKROOM = "BACKROOM"
    RECEIVING = "RECEIVING"
    OTHER = "OTHER"


class DeviceStatus(StrEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class BatchStatus(StrEnum):
    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobKind(StrEnum):
    CATALOG_IMPORT = "CATALOG_IMPORT"
    # Persisted compatibility values remain readable during the reversible schema
    # retirement window. No current process enqueues or claims these kinds.
    RFID_OBSERVATION = "RFID_OBSERVATION"
    REPLENISHMENT_RECALC = "REPLENISHMENT_RECALC"


class JobStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


class ObservationStatus(StrEnum):
    RECEIVED = "RECEIVED"
    PROCESSED = "PROCESSED"
    LATE_IGNORED = "LATE_IGNORED"
    QUARANTINED = "QUARANTINED"
