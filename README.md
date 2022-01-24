# gcp-cli-helper

## gcputil.py
tiny Python3 script for idempotent deployment with gcloud.

deploy GCP resources with template file like makefile of make command, playbook of Ansible, or recipe of Chef.

**parameters:**
- binfmt: path for google cloud SDK
- cache_dir: directory for cache status (command parameter and result) of resources

**methods:**
- `make(template_path)`
- `remove(template_path)`
