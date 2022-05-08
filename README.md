# How to run this project locally

## Installation and Start Service
Please Setup Docker and Docker-Compose on your machine before moving on next steps.

1. Clone this repository.
```
git clone https://github.com/chanontv/basic-airflow.git
```

2. Initialize the database
```
docker-compose up airflow-init
```

3. Start all services
```
docker-compose up
```

The webserver is available at: http://localhost:8080