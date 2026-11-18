"""A workspace-scoped App using Service-owned authentication and admission."""

from a13n_service.app_definitions import AppDefinition
from a13n_service.app_plugins import Frontend
from a13n_service.tenancy.app_plugins import app_router
from a13n_service.tenancy.requests import WorkspaceId
from pydantic import BaseModel

router = app_router("saurlax.channels")


class Summary(BaseModel):
    workspace_id: str
    message: str


@router.get("/summary", name="read_a13n_channels_summary", openapi_extra={"x-a13n-mcp": True})
async def summary(workspace_id: WorkspaceId) -> Summary:
    return Summary(workspace_id=workspace_id, message="Your App backend is connected.")


def definition() -> AppDefinition:
    return AppDefinition(
        app_id="saurlax.channels",
        name="Channels",
        routers=(router,),
        frontend=Frontend("a13n_channels", "static", entry="index.js", styles=("index.css",)),
    )
