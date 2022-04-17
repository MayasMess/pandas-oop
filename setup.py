import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pandas-oop",
    version="0.9.4",
    author="Amayas Messara",
    author_email="amayas.messara@gmail.com",
    description="Pandas dataframes with object oriented programming style",
    install_requires=["pandas", "pangres", "sqlalchemy"],
    keywords=["pandas", "oop", "dataframe", "poop"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MayasMess/pandas-oop",
    project_urls={
        "Bug Tracker": "https://github.com/MayasMess/pandas-oop/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)