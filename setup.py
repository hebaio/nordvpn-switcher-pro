from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nordvpn-switcher-pro",
    version="2.0.1",
    author="Sebastian Hanisch",
    author_email="contact.sebastian.hanisch@gmail.com",
    description="An advanced Python library to automate NordVPN server switching on Windows and Linux.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Sebastian7700/nordvpn-switcher-pro",
    packages=find_packages(),
    install_requires=[
        "requests>=2.20.0",
        "questionary>=1.10.0",
        "fake-useragent>=1.2.0",
        "psutil>=5.9.0",
    ],
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 5 - Production/Stable",

        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Topic :: System :: Networking",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",

        # Pick your license as you wish
        "License :: OSI Approved :: MIT License",

        # Specify the Python versions you support here.
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",

        # Specify the OS this project is intended for
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.10',
    keywords='nordvpn vpn switcher automation web-scraping ip-rotation rotate-ip windows linux api',
    project_urls={
        'Bug Reports': 'https://github.com/Sebastian7700/nordvpn-switcher-pro/issues',
        'Source': 'https://github.com/Sebastian7700/nordvpn-switcher-pro/',
    },
)