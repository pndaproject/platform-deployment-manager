start on runlevel [2345]
stop on runlevel [016]
respawn
respawn limit unlimited
pre-start exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
pre-stop exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
chdir /opt/${environment_namespace}/${component_application}/${component_name}/
exec sudo -u ${application_user} spark-submit --driver-java-options "-Dlog4j.configuration=file:///opt/${environment_namespace}/${component_application}/${component_name}/log4j.properties" --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///opt/${environment_namespace}/${component_application}/${component_name}/log4j.properties' --name '${component_job_name}' --master yarn-cluster --py-files application.properties,${component_py_files} ${component_spark_submit_args} ${component_main_py}
