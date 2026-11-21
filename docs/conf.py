"""Sphinx documentation for HydroCouple Execution Platform."""
import os
import sys

project = "HydroCouple Execution Platform"
author = "HydroCouple"
extensions = ["sphinx.ext.viewcode", "autoapi.extension"]
exclude_patterns = ["_build"]
html_theme = "pydata_sphinx_theme"
html_logo = "_static/hydrocouple.png"
html_static_path = ["_static"]
html_theme_options = {
    "logo": {"text": project},
    "show_toc_level": 2,
}

# Parse the source tree so optional engines and services need not run during a docs build.
autoapi_dirs = ['../src/hydrocouple_platform']
autoapi_options = ["members", "undoc-members", "show-module-summary"]
autoapi_add_toctree_entry = False
autoapi_ignore = ["*/db/migrations/*"]


def omit_inherited_model_doc(app, what, name, obj, options, lines):
    # AutoAPI otherwise repeats Pydantic's generic BaseModel manual for every
    # contract without a class docstring. Keep the contract fields and signatures.
    from pydantic import BaseModel
    from sphinx.util.docstrings import prepare_docstring

    if what == "class" and lines == prepare_docstring(BaseModel.__doc__):
        lines.clear()


def setup(app):
    app.connect("autodoc-process-docstring", omit_inherited_model_doc)


# -- HydroCouple documentation theme ---------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "_hydrocouple"))
import hydrocouple_theme  # noqa: E402

hydrocouple_theme.configure(
    globals(),
    manual_id="hydrocouple.platform",
    project_label="Execution Platform",
    repository="https://github.com/HydroCouple/hydrocouple.platform",
    hero={
        "kicker": "HydroCouple Execution Platform",
        "title": "Simulations on compute you control",
        "lede": "The control plane records each SWMM simulation or HydroCouple composition, and an "
                "agent at your own site leases the work, runs it and returns results with their provenance.",
        "actions": [
            {"label": "First local run", "url": "first-run", "style": "solid"},
            {"label": "API reference", "url": "api", "style": "ghost"},
            {"label": "MCP server manual", "url": "https://www.hydrocouple.org/openswmm.mcp/", "style": "ghost"},
        ],
    },
)
