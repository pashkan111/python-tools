from setuptools import find_packages, setup


setup(
    name='python-tools',
    version='0.0.2',
    author='pashkan111',
    python_requires='>=3.10',
    description='Lib for using postgres and functional tests',
    packages=find_packages(),
    include_package_data=True,
    install_requires=["asyncpg", "sqlalchemy"],
)
