
* TODO: EVERYTHING - I used my other plugin for ansible-plugin-cache-kafka as a frame work

# Files defined 
{{CWD}}/plugins/cache/kafka.py  = plugin to write selected ansible varibles to kafka

{{CWD}}/plugins/cache/kafka.ini = Configuration File for the plugin

{{CWD}}/install-playbook.yml = install the prerequisites

{{CWD}}/debug-playbook.yml   = test examples playbook


# Installation

There is one dependency and makes a copy of the ansible.cfg.example to ansible.cfg (For now)

```
ansible-playbook install-playbook.yml --ask-become-pass
```

# Configuration Ansible

```
[defaults]
cache_plugins      = /usr/share/ansible/plugins/cache:{{CWD}}/plugins/cache
stdout_callback=debug
fact_caching = kafka
```

# Configration Kafka.ini

The Kafka.ini is expected to be in the same location as Kafka.py

{{CWD}}/plugins/cache/Kafka.ini
This is JSON format because it was quick and easy for me. 

```
{
"######": "List of hostnames",
"es_hostnames": ["localhost"],
"######": "Elastic Search Port number",
"es_port": 9200,
"######": "Elastic Search Index to save data",
"es_index": "ansible_cache",
"######": "directory name, if unset will disable local cache - relative to {{CWD}} or specify full path",
"local_cache_directory": "ansible_cache",
"######": "Write data to local cache",
"write_local_cache_directory": true,
"######": "Read from local cache instead of kafka good for offline testing",
"read_local_cache_directory": true,
"######": "Filter fields because ansible has many thousands of varibles which we don't need",
"field_filter": [
                    "######": "All fields are optional",
                    "ansible_hostname",
                    "ansible_distribution",
                    "ansible_distribution_version",
                    "ansible_architecture",
                    "ansible_product_serial",
                    "ansible_product_name",
                    "ansible_kernel",
                    "ansible_memtotal_mb",
                    "ansible_processor",
                    "ansible_processor_cores",
                    "ansible_processor_count",
                    "ansible_processor_vcpus",
                    "######": "I even support BASIC DOT notitation to only grab certain sub-fields",
                    "ansible_date_time.iso8601_basic"
                ]
}
```


# ansible-plugin-cache-kafka
