# gcp-cli-helper

**prerequisite:**
- Python3
- pyyaml
- GCP credentials
- Google Cloud SDK

## gcputil.py
tiny Python3 script aiming idempotent deployment with gcloud cli.

deploy GCP resources via template file
determining the order of command execution by dependencies of resource settings in template,
similar to makefile (make command), playbook (Ansible), or recipe (Chef).

update or replace resources according to the flags to be changed:
- update to change flag values which could be specified with create/update commands
- replace to change flag values which could be specified with create command only

add, update or remove bindings to resources by specifying special type "\_bind" such as:
- iam role: \[\_bind, projects, iam-policy-binding]
  - possible action: add-iam-policy-binding, remove-iam-policy-binding
- backend: \[\_bind, compute, backend-services, backend]
  - possible action: add-backend, remove-backend, update-backend
- ... and so on.

changed flags shall be detected after comparing previous settings in cache with current settings in template file.

no circular dependencies assumed in template file.

**parameters:**
- binfmt: path for Google Cloud SDK
- cache_dir: directory for cache status (command settings and result) of resources to be created

**methods:**
- `make(template_path)` -- create/update resources and remove storage bucket related to gcf (if any)
- `remove(template_path)`

**keywords in template:**
- Parameters: parameter-map to refer by \_sub_ key
- Defaults: map of default settings for all resources
- Alias: additional map of default settings for specific resources
- Resources: resource settings
  - Type: type-list of resource to create/update ("\_bind" type to add/remove binding)
  - ID: id of resource
  - Name: optional name to specify resource prior to id creation
  - Alias: optional list of alias-map keys for additional default settings
  - Parent: parent-map of resource (required by any command action)
  - Create: optional map of settings (change triggers replace)
  - Update: optional map of settings (change triggers update)
  - Flag: optional list of flags (change triggers update)
  - Tag: optional map of key-value labels, env-vars, roles (change triggers update)
- \_sub_: to replace "{key}" with value of parameters or other resources
- \_zip_: to make zip archive from source directory and send to gcs
- \_yml_: to make yaml file from source value specified in template

**reference:**
  [gcloud reference](https://cloud.google.com/sdk/gcloud/reference)
