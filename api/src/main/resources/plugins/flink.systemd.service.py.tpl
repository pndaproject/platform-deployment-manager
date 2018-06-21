[Unit]
Description=PNDA Application: ${component_application}-${component_name}

[Service]
Type=simple
User=${application_user}
WorkingDirectory=/opt/${environment_namespace}/${component_application}/${component_name}/
ExecStartPre=/opt/${environment_namespace}/${component_application}/${component_name}/flink-stop.py
ExecStop=/opt/${environment_namespace}/${component_application}/${component_name}/flink-stop.py
Environment=FLINK_VERSION=${component_flink_version}
ExecStart=${environment_flink} run -m  yarn-cluster ${component_flink_config_args} -ynm ${component_job_name} -v ${flink_python_jar} ${component_main_py} ${component_application_args}
Restart=${component_respawn_type}
RestartSec=${component_respawn_timeout_sec}
