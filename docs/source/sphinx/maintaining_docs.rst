=================================
Previewing documentation locally
=================================

This page is to give the steps to run locally in order to preview the HTML which will be generated when

Prerequisites
^^^^^^^^^^^^^

This prerequisites should be installed by PDM automatically; but documenting here for clarity

-  Sphinx - ``pip install sphinx``
-  Sphinx AutoAPI - ``pip install sphinx-autoapi``
-  Furo Theme for Sphinx - ``pip install furo``

Generating the HTML locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Open a terminal window at the root of the project
2. Activate the venv - ``. .venv/bin/activate``
3. Run ``sphinx-build -M html docs/source docs/build``
4. Under ``docs/build`` you will find the generated HTML; open ``index.html`` in a web browser to verify your changes