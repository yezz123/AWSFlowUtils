
<p align="center">
    <em>Improve your data workflow with enhanced simplicity and robustness in handling common data tasks ✨</em>
</p>

<p align="center">
<a href="https://github.com/yezz123/AWSFlowUtils/actions/workflows/test.yml" target="_blank">
    <img src="https://github.com/yezz123/AWSFlowUtils/actions/workflows/test.yml/badge.svg" alt="Test">
</a>
<a href="https://codecov.io/gh/yezz123/AWSFlowUtils">
    <img src="https://codecov.io/gh/yezz123/AWSFlowUtils/branch/main/graph/badge.svg"/>
</a>
</p>

AWSFlowUtils is an invaluable toolkit offering a wide range of utility functions meticulously crafted to facilitate seamless interactions with AWS S3 and AWS Redshift.

With a focus on simplicity, robustness, and efficiency, this package serves as a powerful aid for users engaged in data work-flows.

Designed to streamline common tasks encountered during data operations, AWSFlowUtils empowers users to effortlessly access and manipulate data stored in AWS S3 and AWS Redshift.

Whether it's retrieving, uploading, or deleting files, or executing data transformations, this package provides a comprehensive set of tools that simplify and accelerate data work-flows.

## Installation

You can add AWSFlowUtils in a few easy steps. First of all, install the dependency:

```shell
$ pip install awsflowutils

---> 100%

Successfully installed awsflowutils
```

## Development 🚧

### Setup environment 📦

You should create a virtual environment and activate it:

```bash
python -m venv venv/
```

```bash
source venv/bin/activate
```

And then install the development dependencies:

```bash
# Install dependencies
pip install -e .[test,lint]
```

### Run tests 🌝

You can run all the tests with:

```bash
bash scripts/test.sh
```

### Format the code 🍂

Execute the following command to apply `pre-commit` formatting:

```bash
bash scripts/format.sh
```

Execute the following command to apply `mypy` type checking:

```bash
bash scripts/lint.sh
```

## License

This project is licensed under the terms of the MIT license.
