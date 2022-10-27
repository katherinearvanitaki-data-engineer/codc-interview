"""
Setup file to define how to build and install the given packages
"""
import sys
import os
import shutil
import setuptools

sys.path.append(os.path.dirname(os.path.realpath(__file__)))


def get_settings(_install_requires):
    """Global settings for setup.py"""

    _global_settings = {
        "name": "codc-interview",
        "version": "1.2.0-beta.0",
        "url": "https://github.com/katherinearvanitaki-data-engineer/codc-interview",
        "license": "Katherine",
        "author": "Katherine Arvanitaki",
        "author_email": "k.arvanitaki@gmail.com",
        "description": "Shared repository for codc-interview",
        "install_requires": _install_requires,
    }
    return _global_settings


def get_requirements():
    """Function to read the requirements file and use as "install_requires" argument"""
    the_lib_folder = os.path.dirname(os.path.realpath(__file__))
    requirement_path = the_lib_folder + "/requirements.txt"
    _install_requires = []
    if os.path.isfile(requirement_path):
        with open(requirement_path, encoding="UTF-8") as file:
            _install_requires = file.read().splitlines()

    return _install_requires


install_requires = get_requirements()
global_settings = get_settings(install_requires)

setuptools.setup(
    name=global_settings["name"],
    version=global_settings["version"],
    packages=setuptools.find_packages(),
    url=global_settings["url"],
    license=global_settings["license"],
    author=global_settings["author"],
    author_email=global_settings["author_email"],
    description=global_settings["description"],
    install_requires=global_settings["install_requires"],
)

