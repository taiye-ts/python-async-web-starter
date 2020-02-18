from aiohttp import web

from project_name.api.urls import urls


app = web.Application()
for url in urls:
    app.router.add_view(*url)
