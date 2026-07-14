"""FastAPI web application for the PBFT test console."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from ament_index_python.packages import get_package_share_directory
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import uvicorn

from .catalog import load_catalog, public_scenario
from .configuration import (
    default_configuration,
    scenario_compatibility,
    validate_configuration,
)
from .manager import ScenarioManager


PACKAGE_NAME = "pbft_emergency_stop_simulator"


class ConfigurationRequest(BaseModel):
    configuration: dict[str, Any]


class RunRequest(BaseModel):
    scenario_ids: list[str] = Field(min_length=1)
    repeat: int = Field(default=1, ge=1, le=100)
    stop_on_failure: bool = False
    configuration: dict[str, Any]


def create_app(
    catalog_path: str | None = None,
    results_dir: str | None = None,
) -> FastAPI:
    catalog = load_catalog(catalog_path)
    root = (
        Path(results_dir).expanduser()
        if results_dir
        else Path.home() / ".ros" / "pbft_test_console" / "runs"
    )
    manager = ScenarioManager(catalog, root)

    share = Path(get_package_share_directory(PACKAGE_NAME))
    web_dir = share / "web"

    required_web_files = ("index.html", "styles.css", "app.js")
    missing_web_files = [
        name for name in required_web_files if not (web_dir / name).is_file()
    ]
    if missing_web_files:
        raise RuntimeError(
            "PBFT Test Console web assets are missing from "
            f"{web_dir}: {missing_web_files}. Rebuild the package."
        )

    app = FastAPI(title="PBFT Test Console", version="2.0")
    app.state.manager = manager
    app.state.web_dir = web_dir

    app.mount(
        "/static",
        StaticFiles(directory=str(web_dir), follow_symlink=True),
        name="static",
    )

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(web_dir / "index.html")

    @app.get("/api/configuration/defaults")
    async def configuration_defaults() -> dict[str, Any]:
        return {"configuration": default_configuration()}

    @app.post("/api/configuration/validate")
    async def configuration_validate(
        request: ConfigurationRequest,
    ) -> dict[str, Any]:
        return validate_configuration(request.configuration)

    @app.post("/api/scenarios/compatible")
    async def compatible_scenarios(
        request: ConfigurationRequest,
    ) -> dict[str, Any]:
        validation = validate_configuration(request.configuration)
        items = []
        for scenario in catalog["scenarios"]:
            public = public_scenario(scenario)
            public["compatibility"] = scenario_compatibility(
                scenario, validation
            )
            items.append(public)
        return {
            "version": catalog["version"],
            "validation": validation,
            "scenarios": items,
        }

    @app.get("/api/scenarios")
    async def scenarios() -> dict[str, Any]:
        validation = validate_configuration(default_configuration())
        items = []
        for scenario in catalog["scenarios"]:
            public = public_scenario(scenario)
            public["compatibility"] = scenario_compatibility(
                scenario, validation
            )
            items.append(public)
        return {
            "version": catalog["version"],
            "validation": validation,
            "scenarios": items,
        }

    @app.get("/api/suites")
    async def suites() -> list[dict[str, Any]]:
        return manager.list_suites()

    @app.get("/api/suites/{suite_id}")
    async def suite(suite_id: str) -> dict[str, Any]:
        item = manager.get_suite(suite_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Suite not found")
        return item

    @app.post("/api/suites")
    async def start_suite(request: RunRequest) -> dict[str, str]:
        try:
            suite_id = await manager.start_suite(
                scenario_ids=request.scenario_ids,
                repeat=request.repeat,
                stop_on_failure=request.stop_on_failure,
                configuration=request.configuration,
            )
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"suite_id": suite_id}

    @app.post("/api/cancel")
    async def cancel() -> dict[str, bool]:
        await manager.cancel_active()
        return {"accepted": True}

    @app.get("/api/suites/{suite_id}/report")
    async def report(suite_id: str) -> FileResponse:
        item = manager.get_suite(suite_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Suite not found")
        path = Path(item["report_path"])
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Report not ready")
        return FileResponse(path, media_type="text/html")

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = manager.subscribe()
        try:
            await websocket.send_json(
                {
                    "type": "initial",
                    "suites": manager.list_suites(),
                    "active_suite_id": manager.active_suite_id,
                }
            )
            while True:
                event = await queue.get()
                await websocket.send_json(event)
        except WebSocketDisconnect:
            pass
        finally:
            manager.unsubscribe(queue)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="PBFT web test console")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--results-dir", default=None)
    args = parser.parse_args()

    app = create_app(args.catalog, args.results_dir)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
