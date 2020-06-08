import setuptools

requirements = filter(lambda x: x != "", open('requirements.txt').read().split('\n'))

with open("README.md", "r") as fh:
    long_description = fh.read()

    setuptools.setup(
        name="braintrust_analysis",
        version="0.0.1",
        author="Raymond Pulver IV",
        author_email="raymondpulver@my.uri.edu",
        description="Braintrust DeFi adapter",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/raymondpulver/braintrust-analysis",
        packages=setuptools.find_packages(include=['braintrust_analysis']),
        install_requirements: requirements,
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        python_requires=">=3.6",
    )
