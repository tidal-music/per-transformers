# per-transformers
Repo containing common pyspark transformers for different pipelines

## Downloading Dependencies

- Make sure you have pyenv and [pyenv](https://github.com/pyenv/pyenv) amd [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed on your local environment.
- Install python 3.8.8 with pyenv `pyenv install 3.8.8`.
- Upgrade the pip package installer `pip install --upgrade pip`
- Set up a new virtual env `pyenv virtualenv 3.8.8 transformers`
- Activate the virtual pyenv using `pyenv local transformers`
- Install dependencies (from `requirements.txt`) and test dependencies (from `requirements-test.txt`) into the env using
` pip install -r requirements.txt -r requirements-test.txt` 