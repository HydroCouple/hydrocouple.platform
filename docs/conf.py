"""Sphinx documentation for HydroCouple Execution Platform."""
project = "HydroCouple Execution Platform"
author = "HydroCouple"
extensions = ["myst_parser", "sphinxcontrib.mermaid"]
myst_enable_extensions = ["colon_fence"]
myst_fence_as_directive = ["mermaid"]
myst_heading_anchors = 6
exclude_patterns = ["_build"]
html_theme = "pydata_sphinx_theme"
html_logo = "_static/hydrocouple.png"
html_static_path = ["_static"]
html_css_files = ['hydrocouple-palette.css']
html_theme_options = {
    "github_url": "https://github.com/HydroCouple/hydrocouple.platform",
    "logo": {"text": project},
    "show_toc_level": 2,
}
