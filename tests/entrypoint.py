# Databricks notebook source
import sys

import pytest

if __name__ == "__main__":
    exit_code = pytest.main(sys.argv[1:])
    if exit_code != pytest.ExitCode.OK:
        raise RuntimeError(f"pytest returned non-zero exit code: {exit_code!s}. See logs for details.")
