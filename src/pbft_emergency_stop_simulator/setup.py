from glob import glob
from os.path import join

import os
from setuptools import find_packages, setup


package_name = "pbft_emergency_stop_simulator"


web_files = [
    "web/index.html",
    "web/styles.css",
    "web/app.js",
]

setup(
    name=package_name,
    version="0.0.2",
    packages=['pbft_emergency_stop_simulator',
    'pbft_emergency_stop_simulator.replica',
    'pbft_emergency_stop_simulator.test_console',],
    data_files=[
        (
            "share/ament_index/resource_index/packages",
            ["resource/" + package_name],
        ),
        (
            "share/" + package_name,
            ["package.xml"],
        ),
        (
            join("share", package_name, "launch"),
            glob("launch/*.launch.py"),
        ),
        (
            os.path.join("share", package_name, "config"),
            glob("config/*.yaml"),
        ),
        (
            os.path.join("share", package_name, "web"),
            web_files,
        ),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="Maja",
    maintainer_email="maja@todo.todo",
    description="ROS 2 PBFT emergency-stop simulator.",
    license="Apache-2.0",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            (
                "client_node = "
                "pbft_emergency_stop_simulator.client_node:main"
            ),
            (
                "pbft_replica = "
                "pbft_emergency_stop_simulator.pbft_replica:main"
            ),
            (
                "pbft_monitor = "
                "pbft_emergency_stop_simulator.pbft_monitor:main"
            ),
            (
                "safety_supervisor = "
                "pbft_emergency_stop_simulator.safety_supervisor:main"
            ),
            (
                "scenario_evaluator = "
                "pbft_emergency_stop_simulator.scenario_evaluator:main"
            ),
            (
                "performance_monitor = "
                "pbft_emergency_stop_simulator.performance_monitor:main"
            ),
            (
                "pbft_test_console = "
                "pbft_emergency_stop_simulator.test_console.web_app:main"
            ),
            (
                "pbft_test_cli = "
                "pbft_emergency_stop_simulator.test_console.cli:main"
            )

        ],
    },
)
