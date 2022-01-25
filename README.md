# gcp-cli-helper

## gcputil.py
tiny Python3 script for idempotent deployment with gcloud.

deploy GCP resources with template file like makefile (make command), playbook (Ansible), or recipe (Chef).

automatically determine the order of command execution by the dependencies of parameters in template
(no circular dependencies assumed).

update or replace resources depending on the type of flags to be changed:
- update to change flag values which could be specified with create/update commands
- replace to change flag values which could be specified with create command only

**parameters:**
- binfmt: path for google cloud SDK
- cache_dir: directory for cache status (command parameter and result) of resources

**methods:**
- `make(template_path)`
- `remove(template_path)`

**keywords in template:**
- Parameters:
- Defaults:
- Alias:
- Resource:
  - Alias:
  - ID:
  - Name:
  - Type:
  - Parent:
  - Create:
  - Update:
  - Label:
  - Role:
  - Flag:
- _sub_:
- _zip_:
- _yml_:

**reference:**
  [gcloud reference](https://cloud.google.com/sdk/gcloud/reference)
