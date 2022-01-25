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
- Parameters: parameter-map to refer by \_sub_ key
- Defaults: default settings for all resources
- Alias: additional default settings to refer by resources key
- Resources: resource settings
  - Alias: alias-map keys for additional default settings
  - ID: id of resource
  - Name: optional name to specify resource prior to id creation
  - Type: type-list of resource
  - Parent: parent-map of resource (change triggers replace)
  - Create: optional map of settings (change triggers replace)
  - Update: optional map of settings (change triggers update)
  - Flag: optional list of flags (change triggers update)
  - Label: optional map of labels (change triggers update)
  - Role: role settings for service-account
- \_sub_: replace "{key}" with value of other resources or parameters
- \_zip_: make zip archive from source directory and send gcs
- \_yml_: make yaml file from source value specified in template

**reference:**
  [gcloud reference](https://cloud.google.com/sdk/gcloud/reference)
