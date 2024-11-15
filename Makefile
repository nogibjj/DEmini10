install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv -cov=mylib test_*.py

format:	
	black *.py 

lint:
	
    # pylint --disable=W0621,W1510,W1514,W0612 *.py mylib/*.py src/**/*.py
	ruff check --ignore E,F	*.py mylib/*.py test_*.py 
    # pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	#deploy goes here
		
all: install lint test format deploy

generate_and_push:
	# Add, commit, and push the generated files to GitHub
	@if [ -n "$$(git status --porcelain)" ]; then \
		git config --local user.email "sammyissmiling@gmail.com"; \
		git config --local user.name "GitHub Action"; \
		git add .; \
		git commit -m "Add output log"; \
		git push; \
	else \
		echo "No changes to commit. Skipping commit and push."; \
	fi