from setuptools import setup, find_packages

setup(
    name='tenb-agents',
    version='0.1.0',
    package_dir={"": "src"},

    install_requires=[
        'click',
        'python-dotenv',
        'pytenable',
        'pandas',
        'hvplot',
        'jupyterlab'
    ],
    entry_points={
        'console_scripts': [
            'tenb-agents = cli:cli',
            'tenb-generate-example = cli:cli',
        ],
    },
)
