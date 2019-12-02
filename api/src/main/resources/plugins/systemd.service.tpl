[Unit]
Description=PNDA Application: ${component_application}-${component_name}

[Service]
Type=simple
User=${application_user}
WorkingDirectory=/opt/${environment_namespace}/${component_application}/${component_name}/
Environment=SPARK_MAJOR_VERSION=${component_spark_version}
ExecStart=${environment_kube_path}${component_yaml_file}
Restart=${component_respawn_type}
RestartSec=${component_respawn_timeout_sec}

