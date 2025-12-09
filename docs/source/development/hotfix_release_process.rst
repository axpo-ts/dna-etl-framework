
====================
Hotfix Release Process
====================


This document outlines the hotfix process utilizing the `main` and `develop` branches and an automated GitHub Actions job to rebase changes from `main` into `develop`.

A hotfix is a critical change applied directly to the production codebase (the main branch) to address urgent issues—such as security vulnerabilities, data corruption, or system outages—that cannot wait for the standard release cycle.

Branches
---------------

* ``main``: Production-ready code. Only hotfixes and release-ready commits are merged here.

* ``develop``:   Integration branch for features and hotfixes. Contains code for the next release.

Hotfix Workflow
---------------

1. **Create a hotfix branch**::

::

   git fetch origin main
   git checkout main
   # Note: The '--ff-only' flag requires the local 'main' branch to be fully synchronized with 'origin/main'.
   git pull origin main --ff-only
   git checkout -b hotfix/<issue-number>_description

2. **Implement the fix** and commit your changes.

3. **Push and open a PR** targeting `main`.

4. **Review and merge** the PR into `main`.

Automated Rebase from `main` to `develop`
~~~~~~~~~~~~~~

* After a hotfix is merged into `main`, the GitHub Actions workflow defined in .github/workflows/merge-main-develop.yml triggers automatically to rebase `main` onto `develop`

This process ensures that all hotfixes applied to `main` are incorporated into `develop` for the next release cycle. 

**It's important that this process runs correctly; otherwise, manual conflict resolution will be required**:

Manual Conflict Resolution
~~~~~~~~~~~~~~
If the automated rebase encounters conflicts, you can resolve them manually

::

  git checkout develop
  git fetch origin main
  git merge origin/main 
  # Resolve conflicts as prompted
  git commit -m "Resolve merge conflicts between main and develop"
  git push

Additional Resources
~~~~~~~~~~~~~~
* `Workflow file <https://github.com/axpo-ts/dna-platform-etl/actions/workflows/merge-main-develop.yml>`_

