from glob import glob
from os.path import join

from setuptools import find_packages, setup


package_name = "pbft_emergency_stop_simulator"


setup(
    name=package_name,
    version="0.0.2",
    packages=find_packages(exclude=["test"]),
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
        ],
    },
)
