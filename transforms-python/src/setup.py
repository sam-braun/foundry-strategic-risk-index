#!/usr/bin/env python
"""Python project setup script."""

import os
from setuptools import find_packages, setup

setup(
    name=os.environ["PKG_NAME"],
    version=os.environ["PKG_VERSION"],
    description="Computes a Strategic Risk Index for US metro areas based on QCEW employment data",
    author="strategic_industrial_dependencies",
    packages=find_packages(exclude=["contrib", "docs", "test"]),
    install_requires=[],
    entry_points={
        "transforms.pipelines": ["root = strategic_risk_index.pipeline:sri_pipeline"]
    },
)
