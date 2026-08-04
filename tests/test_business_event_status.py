import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from sqlalchemy.orm import Session

from abacus.models.architecture import (
    BusinessEvent,
    BusinessEventStatus,
    InventoryTransitionOutbox,
)
from abacus.services.business_events import business_event_status


@pytest.mark.parametrize(
    ("published_at", "quarantined_at", "reconciled_at", "expected"),
    [
        (None, None, None, BusinessEventStatus.PENDING_PROJECTION),
        (datetime.now(UTC), None, None, BusinessEventStatus.PROJECTED),
        (None, datetime.now(UTC), None, BusinessEventStatus.FAILED),
        (
            None,
            datetime.now(UTC),
            datetime.now(UTC),
            BusinessEventStatus.PROJECTED,
        ),
    ],
)
def test_business_event_status_reflects_projection_reconciliation(
    published_at: datetime | None,
    quarantined_at: datetime | None,
    reconciled_at: datetime | None,
    expected: BusinessEventStatus,
) -> None:
    transition_id = uuid.uuid4()
    event = cast(BusinessEvent, SimpleNamespace(transition_id=transition_id))
    transition = cast(
        InventoryTransitionOutbox,
        SimpleNamespace(
            published_at=published_at,
            quarantined_at=quarantined_at,
            reconciled_at=reconciled_at,
        ),
    )
    db = Mock(spec=Session)
    db.get.return_value = transition

    assert business_event_status(db, event) is expected


def test_business_event_status_fails_when_transition_is_missing() -> None:
    event = cast(BusinessEvent, SimpleNamespace(transition_id=uuid.uuid4()))
    db = Mock(spec=Session)
    db.get.return_value = None

    assert business_event_status(db, event) is BusinessEventStatus.FAILED
