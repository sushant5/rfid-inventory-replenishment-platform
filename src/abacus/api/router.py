from fastapi import APIRouter

from abacus.api.routes.auth import canonical_router as canonical_auth_router
from abacus.api.routes.auth import router as auth_router
from abacus.api.routes.canonical_replenishment import router as canonical_replenishment_router
from abacus.api.routes.catalog import canonical_router as canonical_catalog_router
from abacus.api.routes.catalog import router as catalog_router
from abacus.api.routes.health import router as health_router
from abacus.api.routes.onboarding import canonical_router as canonical_onboarding_router
from abacus.api.routes.onboarding import router as onboarding_router
from abacus.api.routes.replenishment import router as replenishment_router
from abacus.api.routes.rfid import canonical_router as rfid_canonical_router
from abacus.api.routes.rfid import device_router as rfid_device_router
from abacus.api.routes.rfid import platform_router as rfid_platform_router
from abacus.api.routes.users import router as users_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(canonical_onboarding_router)
api_router.include_router(canonical_catalog_router)
api_router.include_router(canonical_replenishment_router)
api_router.include_router(rfid_canonical_router)
api_router.include_router(canonical_auth_router)
api_router.include_router(auth_router)
api_router.include_router(users_router)

# Kept only to test migrations from the earlier prototype. The submitted application
# never mounts these routes, so it cannot accept work that its canonical workers do
# not consume.
legacy_test_router = APIRouter()
legacy_test_router.include_router(onboarding_router, include_in_schema=False)
legacy_test_router.include_router(catalog_router, include_in_schema=False)
legacy_test_router.include_router(rfid_device_router, include_in_schema=False)
legacy_test_router.include_router(rfid_platform_router, include_in_schema=False)
legacy_test_router.include_router(replenishment_router, include_in_schema=False)
