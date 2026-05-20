from setuptools import setup

setup(
    name="tap-azure-devops",
    version="1.0.0",
    description="Singer.io tap for extracting data from the Azure DevOps API",
    author="Bilanc",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python>=5.12.1",
        "requests>=2.20.0",
        "backoff>=1.8.0",
        "pytz>=2021.1",
    ],
    extras_require={"dev": ["pylint==2.6.2", "ipdb", "nose", "requests-mock==1.9.3"]},
    entry_points="""
        [console_scripts]
        tap-azure-devops=tap_azure_devops:main
    """,
    packages=["tap_azure_devops"],
    package_data={"tap_azure_devops": ["schemas/*.json"]},
    include_package_data=True,
)
