start on runlevel [2345]
stop on runlevel [016]
${respawn}
${respawn_limit}
setuid ${application_user}
env FLINK_VERSION=${component_flink_version}
pre-start exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
pre-stop exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
env programDir=/opt/${environment_namespace}/${component_application}/${component_name}/
chdir /opt/${environment_namespace}/${component_application}/${component_name}/
exec flink run -m yarn-cluster ${component_flink_config_args} -ynm ${component_job_name} --class ${component_main_class} ${component_main_jar}  ${component_application_args}

