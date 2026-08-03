from fastapi import APIRouter

from abacus.api.routes.auth import canonical_router as canonical_auth_router
from abacus.api.routes.auth import router as auth_router
from abacus.api.routes.canonical_replenishment import router as canonical_replenishment_router
from abacus.api.routes.catalog import canonical_router as canonical_catalog_router
from abacus.api.routes.health import router as health_router
from abacus.api.routes.onboarding import canonical_router as canonical_onboarding_router
from abacus.api.routes.rfid import canonical_router as rfid_canonical_router
from abacus.api.routes.users import router as users_router

api_router = APIRouter()
api_router.include_router(canonical_onboarding_router)
api_router.include_router(canonical_catalog_router)
api_router.include_router(rfid_canonical_router)
api_router.include_router(canonical_auth_router)
api_router.include_router(auth_router)
api_router.include_router(users_router)
api_router.include_router(canonical_replenishment_router)
api_router.include_router(health_router)
