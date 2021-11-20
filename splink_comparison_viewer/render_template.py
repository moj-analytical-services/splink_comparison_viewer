from jinja2 import Template
import json
import os
import pkgutil
import pandas as pd


def render_html_vis(
    comparison_vector_data,
    splink_settings: dict,
    out_path: str,
    overwrite: bool = False,
):
    """Render the visualisation to a self-contained html page"""

    # When developing the package, it can be easier to point
    # ar the script live on observable using <script src=>
    # rather than bundling the whole thing into the html
    bundle_observable_notebook = True

    template_path = "jinja/template.j2"
    template = pkgutil.get_data(__name__, template_path).decode("utf-8")
    template = Template(template)

    template_data = {
        "comparison_vector_data": comparison_vector_data.to_json(orient="records"),
        "splink_settings": json.dumps(splink_settings),
    }

    files = {
        "embed": "vega-embed@6",
        "vega": "vega@5",
        "vegalite": "vega-lite@5",
        "svu_text": "splink_vis_utils.js",
    }
    for k, v in files.items():
        f = pkgutil.get_data(__name__, f"js_lib/{v}")
        f = f.decode("utf-8")
        template_data[k] = f

    files = {"custom_css": "custom.css"}
    for k, v in files.items():
        f = pkgutil.get_data(__name__, f"css/{v}")
        f = f.decode("utf-8")
        template_data[k] = f

    template_data["bundle_observable_notebook"] = bundle_observable_notebook

    rendered = template.render(**template_data)

    if os.path.isfile(out_path) and not overwrite:
        raise ValueError(
            f"The path {out_path} already exists. Please provide a different path."
        )
    else:
        with open(out_path, "w") as html_file:
            html_file.write(rendered)