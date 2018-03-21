[Unit]
Description=PNDA Application: ${component_application}-${component_name}

[Service]
Type=simple
User=${application_user}
WorkingDirectory=/opt/${environment_namespace}/${component_application}/${component_name}/
ExecStartPre=/opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
ExecStopPost=/opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
Environment=FLINK_VERSION=${component_flink_version}
ExecStart=/usr/bin/flink run -m  yarn-cluster ${component_flink_config_args} -ynm ${component_job_name} --class ${component_main_class} ${component_main_jar} ${component_application_args}
${respawn}
${respawn_limit}
