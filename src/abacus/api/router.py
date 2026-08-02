from fastapi import APIRouter

from abacus.api.routes.auth import router as auth_router
from abacus.api.routes.catalog import router as catalog_router
from abacus.api.routes.health import router as health_router
from abacus.api.routes.onboarding import router as onboarding_router
from abacus.api.routes.replenishment import router as replenishment_router
from abacus.api.routes.rfid import device_router as rfid_device_router
from abacus.api.routes.rfid import platform_router as rfid_platform_router
from abacus.api.routes.users import router as users_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(onboarding_router)
api_router.include_router(catalog_router)
api_router.include_router(rfid_device_router)
api_router.include_router(rfid_platform_router)
api_router.include_router(replenishment_router)
api_router.include_router(auth_router)
api_router.include_router(users_router)
