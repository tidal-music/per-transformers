# per-transformers
Repo containing common pyspark transformers for different pipelines

## Downloading Dependencies

- Make sure you have pyenv and [pyenv](https://github.com/pyenv/pyenv) amd [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed on your local environment.
- Install python 3.8.16 with pyenv `pyenv install 3.10.13`.
- Set up a new virtual env `pyenv virtualenv 3.10.13 transformers`
- Set local pyenv version `pyenv local transformers`
- Activate the virtual pyenv using `pyenv activate transformers`
- Upgrade the pip package installer `pip install --upgrade pip`
- Install poetry for package management `pip install poetry==1.7.1`
- Install dependencies from the lock file `poetry install --no-root` 
