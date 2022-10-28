# codc-interview
Repository for codc-interview

=================

## Introduction
This project contains code to complete the exercise as described in exercise.md.

## Create virtual environment for local development of shared libraries
To create the virtual environment, run the following (instructions for windows):
```
virtualenv venv
venv\Scripts\activate
pip install --upgrade setuptools wheel pip
pip install -r requirements.txt
```

## Install within existing project
The following commands test the installation from Git to the local machine
```
pip install "git+https://git@github.com/katherinearvanitaki-data-engineer/codc-interview.git"
```

## Install with new project
The following commands test the installation from Git to the local machine
```
mkdir codc-interview
cd codc-interview
virtualenv venv
venv\Scripts\activate
pip install "git+https://git@github.com/katherinearvanitaki-data-engineer/codc-interview.git"
```

## Update package once installed
The following commands test the installation from Git to the local machine
```
pip install "git+https://git@github.com/katherinearvanitaki-data-engineer/codc-interview.git" --upgrade
```

## Working with a specific branch
If the project requires install from a different branch, this can be done by appending "@<branch-name>" to the URL
```
pip install "git+https://git@github.com/katherinearvanitaki-data-engineer/codc-interview.git@development" --upgrade
```

## execute main script
input parameters: 
--dataset_one, default value = "dataset_one.csv", 
--dataset_two, default value = "dataset_one.csv", 
--countries_list, default value = ""

example command: 
python main.py --dataset_one "dataset_one.csv" --dataset_two "dataset_two.csv" --countries_list "United Kingdom,Netherlands"
