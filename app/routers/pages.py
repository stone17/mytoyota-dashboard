# app/routers/pages.py
import time
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="app/templates")

def get_cache_buster():
    return int(time.time())

@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"cache_buster": get_cache_buster()}
    )

@router.get("/settings", response_class=HTMLResponse)
async def read_settings(request: Request):
    return templates.TemplateResponse(
        request=request, name="settings.html", context={"cache_buster": get_cache_buster()}
    )

@router.get("/trips", response_class=HTMLResponse)
async def read_trips(request: Request):
    return templates.TemplateResponse(
        request=request, name="trips.html", context={"cache_buster": get_cache_buster()}
    )

@router.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="logs.html", context={"cache_buster": get_cache_buster()}
    )

@router.get("/notifications", response_class=HTMLResponse)
async def read_notifications_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="notifications.html", context={"cache_buster": get_cache_buster()}
    )

@router.get("/heatmap", response_class=HTMLResponse)
async def read_heatmap_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="heatmap.html", context={"cache_buster": get_cache_buster()}
    )