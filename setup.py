import os

from setuptools import find_packages, setup

NAME = "flattering"


def get_version():
    about = {}
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, NAME.replace("-", "_"), "__version__.py")) as f:
        exec(f.read(), about)
    return about["__version__"]


setup(
    name=NAME,
    version=get_version(),
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "attrs>=21.2.0",
        "scalpl>=0.4.2",
    ],
    entry_points={
        "console_scripts": [
            "flattering=flattering.cli:main",
        ]
    },
)
