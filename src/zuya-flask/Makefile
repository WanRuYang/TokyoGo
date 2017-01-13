build_env:
	pip install -r requirements.txt

deploy:
	ansible-playbook deployment/site.yml

# Establish a tunnel for web app
connect_flask_app:
	scripts/connect_flask.sh 8080

# Establish a tunnel for Jupyter notebook
connect_jupyter:
	scripts/connect_flask.sh 8888

build_tokyogo_server_env: build_tokyogo_server_virtualenv
	/bin/bash -c "source tokyogo/env/bin/activate && cd tokyogo && pip install -r requirements.txt"

build_tokyogo_server_virtualenv:
	/bin/bash -c "cd tokyogo && virtualenv env"

run_tokyogo_server:
	/bin/bash -c "source tokyogo/env/bin/activate && cd tokyogo && python app.py"
