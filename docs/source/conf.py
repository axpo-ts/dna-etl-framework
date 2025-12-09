# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

print(sys.path)
# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DnA ETL Framework"
copyright = "2025, Axpo"
author = "Axpo"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary"]
# autosummary_generate = True
# autodoc_mock_imports = ['dlt','dbutils']
extensions = ["autoapi.extension"]

autoapi_dirs = ["../../src"]
templates_path = ["_templates"]
exclude_patterns = [
    "./../src/data_platform/data_model/bronze/**",
    "./../src/data_platform/data_model/silver/**",
    "./../src/data_platform/data_model/gold/**",
]

autoapi_ignore = [
    "./../src/data_platform/data_model/bronze/**",
    "./../src/data_platform/data_model/silver/**",
    "./../src/data_platform/data_model/gold/**",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
