[Unit]
Description=PNDA Application: ${component_application}-${component_name}

[Service]
Type=simple
User=${application_user}
WorkingDirectory=/opt/${environment_namespace}/${component_application}/${component_name}/
ExecStartPre=/opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
ExecStopPost=/opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
Environment=SPARK_MAJOR_VERSION=${component_spark_version}
ExecStart=${environment_spark_submit} --driver-java-options "-Dlog4j.configuration=file:////opt/${environment_namespace}/${component_application}/${component_name}/log4j.properties" --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=file:////opt/${environment_namespace}/${component_application}/${component_name}/log4j.properties' --name '${component_job_name}' --master yarn-cluster --py-files application.properties,${component_py_files} ${component_spark_submit_args} ${component_main_py}
Restart=${component_respawn_type}
RestartSec=${component_respawn_timeout_sec}
