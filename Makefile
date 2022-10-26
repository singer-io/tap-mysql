test:
	SINGER_TAP_MYSQL_TEST_DB_HOST=localhost SINGER_TAP_MYSQL_TEST_DB_PORT=3306 SINGER_TAP_MYSQL_TEST_DB_USER=root SINGER_TAP_MYSQL_TEST_DB_PASSWORD=password nosetests

start-dev-db:
	TAP_MYSQL_PASSWORD=password	TAP_MYSQL_DBNAME=tap_mysql_test
	python ./bin/test-db start 

venv-test:
	SINGER_TAP_MYSQL_TEST_DB_HOST=localhost SINGER_TAP_MYSQL_TEST_DB_PORT=3306 SINGER_TAP_MYSQL_TEST_DB_USER=root SINGER_TAP_MYSQL_TEST_DB_PASSWORD=password ;\
	. ./venv/bin/activate ;\
	nosetests tests/nosetests

venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[dev]